<?php

namespace Tochka\Esb\Stomp;

/**
 * Class StompAdapter
 * @package Tochka\Esb\Stomp
 */
class StompAdapter
{
    const DEFAULT_STOMP_VERSION = '1.2';
    /*** @var resource */
    protected $stomp;

    /** @var array */
    protected $hosts;
    /** @var string */
    protected $login;
    /** @var string */
    protected $password;
    /** @var array */
    protected $headers = [];

    /** @var string[] */
    protected $errors = [];

    /** @var array */
    protected $queues = [];

    /**
     * Примеры $connectionString:
     * - Use only one broker uri: tcp://localhost:61614
     * - use failover in given order: failover://(tcp://localhost:61614,ssl://localhost:61612)
     *
     * @param string $connectionString
     * @param string $login
     * @param string $password
     * @param array  $headers
     *
     * @throws \Exception
     */
    public function __construct($connectionString, $login, $password, array $headers = [])
    {
        $this->hosts = $this->parseConnectionString($connectionString);
        $this->login = $login;
        $this->password = $password;
        $this->headers = array_merge(['accept-version' => self::DEFAULT_STOMP_VERSION], $headers);

        $this->checkConnection();
    }

    /**
     * При уничтожении объекта - отключаемся
     */
    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * @param string $destination
     * @param string $message
     * @param array  $headers
     *
     * @throws \Exception
     */
    public function send($destination, $message, $headers = [])
    {
        $this->checkConnection();

        $transactionId = uniqid('client', true);

        $headers += ['transaction' => $transactionId];

        try {
            stomp_begin($this->stomp, $transactionId);
            stomp_send($this->stomp, $destination, $message, $headers);
            stomp_commit($this->stomp, $transactionId);
        } catch (\Exception $e) {
            stomp_abort($this->stomp, $transactionId);

            throw $e;
        }
    }

    /**
     * @return array|null
     */
    public function getNextMessage()
    {
        $this->checkConnection();

        if (!stomp_has_frame($this->stomp)) {
            return null;
        }

        return stomp_read_frame($this->stomp);
    }

    /**
     * @param $frame
     *
     * @return bool
     */
    public function ack($frame)
    {
        $id = !empty($frame['headers']['ack']) ? $frame['headers']['ack'] : $frame;
        return stomp_ack($this->stomp, $id, ['id' => $id]);
    }

    /**
     * @param $frame
     *
     * @return mixed
     */
    public function nack($frame)
    {
        $id = !empty($frame['headers']['ack']) ? $frame['headers']['ack'] : $frame;
        return stomp_nack($this->stomp, $id, ['id' => $id]);
    }

    public function checkConnection()
    {
        if (!$this->isStompResource() || $this->hasErrors()) {
            $this->reconnect();
        }
    }

    public function connect()
    {
        $this->errors = [];
        $link = null;

        foreach ($this->hosts as $host) {
            try {
                $link = stomp_connect($host, $this->login, $this->password, $this->headers);
            } catch (\Exception $e) {
                $this->errors[] = '[' . $host . ']: ' . stomp_connect_error();
            }

            if ($link) {
                break;
            }
        }

        if (!$link) {
            throw new StompAdapterException('Could`nt connect to Brocker by provided hosts: ' . implode('; ', $this->errors));
        }

        $this->stomp = $link;
    }

    public function reconnect()
    {
        $this->disconnect();
        $this->connect();
        $this->subscribeAll();
    }

    public function disconnect()
    {
        if ($this->isStompResource()) {
            $this->unsubscribeAll();
            stomp_close($this->stomp);
        }
    }

    /**
     * Подписываемся на все сохраненные подписки
     */
    public function subscribeAll()
    {
        foreach ($this->queues as $queue => $status) {
            if (empty($status)) {
                $this->subscribe($queue);
            }
        }
    }

    /**
     * Отписываемся от всех активных подписок
     */
    public function unsubscribeAll()
    {
        foreach ($this->queues as $queue => $status) {
            if (!empty($status)) {
                $this->unsubscribe($queue);
            }
        }
    }

    /**
     * Отписываемся от всех активных подписок и чистим список очередей
     */
    public function clearSubscribes()
    {
        $this->unsubscribeAll();
        $this->queues = [];
    }

    /**
     * Подписываемся на очередь
     *
     * @param string $queue
     *
     * @return mixed
     */
    public function subscribe($queue)
    {
        if (empty($this->queues[$queue])) {
            $this->queues[$queue] = uniqid('client', true);

            stomp_subscribe($this->stomp, $queue, [
                'id' => $this->queues[$queue],
                'ack' => 'client'
            ]);
        }

        return $this->queues[$queue];
    }

    /**
     * Отписываемся от очереди
     *
     * @param string $queue
     *
     * @return void
     */
    public function unsubscribe($queue)
    {
        if (empty($this->queues[$queue])) {
            return;
        }

        stomp_unsubscribe($this->stomp, $queue, [
            'id' => $this->queues[$queue]
        ]);
        $this->queues[$queue] = false;
    }

    /**
     * Проверяет, что у нас есть активный ресурс подключения
     * @return bool
     */
    protected function isStompResource()
    {
        return !empty($this->stomp) && is_resource($this->stomp);
    }

    /**
     * Проверяет на наличие ошибок
     * @return bool
     */
    protected function hasErrors()
    {
        return stomp_error($this->stomp);
    }

    /**
     * Сериализуем только важные данные
     * @return array
     */
    public function __sleep()
    {
        return ['hosts', 'login', 'password', 'headers', 'errors', 'queues'];
    }

    /**
     * Парсит строку подключения
     *
     * @param string $connectionString
     *
     * @return array
     * @throws StompAdapterException
     */
    protected function parseConnectionString($connectionString)
    {
        $hosts = [];

        $pattern = "|^(([a-zA-Z0-9]+)://)+\(*([a-zA-Z0-9\.:/i,-_]+)\)*$|i";
        if (preg_match($pattern, $connectionString, $matches)) {

            list(, , $scheme, $hostsPart) = $matches;

            if ($scheme !== 'failover') {
                $hosts[] = $hostsPart;
            } else {
                foreach (explode(',', $hostsPart) as $url) {
                    $hosts[] = $url;
                }
            }
        }

        if (empty($hosts)) {
            throw new StompAdapterException('Bad Broker URL ' . $connectionString . 'Check used scheme!');
        }

        return $hosts;
    }
}