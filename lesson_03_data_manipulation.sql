-- SQL-блокнот для Бесплатного онлайн курса "Основы ClickHouse"  по материалам статьи 3 урока "Основы работы с данными в ClickHouse: вставка, выборка и первые аналитические запросы"
-- полный текст статьи: https://bigdataschool.ru/blog/news/free_course_03_clickhouse_dml/
-- Автор: Bigdataschool.ru   "Школа Больших Данных"

-- Шаг 1: Подготовка окружения.
-- Убедимся, что база данных существует и мы работаем в ее контексте.
CREATE DATABASE IF NOT EXISTS my_first_db;
USE my_first_db;

-- Для примеров нам понадобится таблица. Используем улучшенную версию с партиционированием
-- из предыдущего урока, так как она лучше подходит для реальных задач.
CREATE TABLE IF NOT EXISTS access_logs_partitioned (
    timestamp   DateTime64(3),
    event_type  LowCardinality(String),
    user_id     UInt64,
    ip_address  IPv4,
    url         String,
    duration_ms UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, user_id);


-- Шаг 2: Вставка данных с помощью INSERT INTO ... VALUES.
-- Этот способ удобен для добавления небольшого количества строк вручную.
INSERT INTO my_first_db.access_logs_partitioned (timestamp, event_type, user_id, ip_address, url, duration_ms) VALUES
('2024-07-02 11:00:00.000', 'page_view', 105, '8.8.8.8', '/pricing', 450),
('2024-07-02 11:01:15.123', 'click', 105, '8.8.8.8', '/buy_now_button', 30);


-- Шаг 3: Вставка данных из файла формата JSONEachRow.
-- Следующая команда выполняется не в SQL-клиенте, а в терминале (командной строке) вашего компьютера.
-- Она показывает, как загрузить данные из файла `logs.json` в ClickHouse.

/*
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
-- Пример для выполнения в ТЕРМИНАЛЕ (BASH/CMD/POWERSHELL)                             --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

-- Сначала создайте файл `logs.json` со следующим содержимым:
{"timestamp":"2024-07-02 12:30:00","event_type":"page_view","user_id":106,"ip_address":"1.1.1.1","url":"/features","duration_ms":500}
{"timestamp":"2024-07-02 12:30:45","event_type":"click","user_id":106,"ip_address":"1.1.1.1","url":"/demo_request","duration_ms":60}

-- Затем выполните команду:
clickhouse-client --query="INSERT INTO my_first_db.access_logs_partitioned FORMAT JSONEachRow" < logs.json

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
*/


-- Шаг 4: Основы выборки данных: SELECT
