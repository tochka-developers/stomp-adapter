# STOMP Adapter

Класс для работы с очередями по протоколу Stomp. Обертка для PHP-расширения Stomp.

Для чего?

- При разрыве соединения на стороне сервера, либо любых других проблемах с соединением данный адаптер сам 
пересоздает подключение (при этом старое разрывает безопасно) и восстанавливает все активные подписки.
- Поддерживает Stomp 1.2.
- Просто так удобнее:
```php
$stomp = new \Tochka\Esb\Stomp\StompClient($url, $login, $password);

// Отправка сообщения
$stomp->send('queue_1', 'Data');

// Подписываемся сразу на несколько очередей
$stomp->subscribe('queue_1');
$stomp->subscribe('queue_2');

// Ждем сообщений
while (true) {
    // Каждые две секунды проверяем, есть ли сообщения
    sleep(2);
    
    while ($frame = $stomp->getNextMessage()) {
        // Что-нибудь делаем с сообщением
        // ...
        
        // Отвечает серверу, что все ок
        $stomp->ack($frame);
        // или что-то пошло не так
        // $stomp->nack($frame);
    }
}
```
- При уничтожении объекта сам отписывается от всех активных подписок и закрывает соединение