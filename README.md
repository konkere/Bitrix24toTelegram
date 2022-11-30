# Bitrix24 to Telegram

Скрипт для отправки оповещений из Битрикс24 в Телеграм.

**Python 3.10** required.

После первого запуска требуется внести данные в конфиг ``$HOME/.config/Bitrix24toTelegram/settings.conf``:

1. Обязательно:

1.1 ``botid`` и ``chatid`` в разделе ``[Telegram]``

1.2. ``webhook`` в разделе ``[Bitrix24]``, предварительно создав его в Битриксе24. Создаётся по пути: ``Разработчикам``→``Другое``→``Входящий вебхук``. Должен содержать доступы: ``CRM``, ``Пользователи``, ``Пользователи (минимальный)``, ``Пользователи (базовый)``.

2. Опционально:

2.1. ``db`` в разделе ``[System]``.

Также автоматически генерится файл ``$HOME/.config/Bitrix24toTelegram/telegram_id.list`` формата: ``<id_bitrix24>=<id_telegram>#<Имя Фамилия>``:

* ``id_bitrix24`` — берётся из Битрикс24,
* ``id_telegram`` — заполняется самостоятельно по надобности (для персональных обращений с оповещением),
* ``Имя Фамилия`` — берётся из Битрикс24 (для наглядности и удобства, нигде не используется).