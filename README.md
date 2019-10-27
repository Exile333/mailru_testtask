# mailru_testtask
Небольшое key-value хранилище, реализованное на Питоне с использованием БД Tarantool. Был использован асинхронный веб-фреймворк Sanic.

Чтобы использовать, нужно сначала запустить "tarantoolctl start kv" в директории misc_configs,
после чего перейти в директорию src и запустить "sudo python3.7 server.py"

API:
* POST /kv body: {"key": "test", "value": {SOME ARBITRARY JSON}} 
* PUT kv/{id} body: {"value": {SOME ARBITRARY JSON}}
* GET kv/{id} 
* DELETE kv/{id}
* POST  возвращает 409 если ключ уже существует
* POST, PUT возвращают 400 если боди некорректное
* PUT, GET, DELETE возвращает 404 если такого ключа нет

Все операции логируются.
