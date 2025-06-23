-- SQL-блокнот для Бесплатного онлайн курса "Основы ClickHouse"  по материалам статьи 2 урока "Типы данных и движки таблиц в ClickHouse: Фундамент для производительности"
-- полный текст статьи: https://bigdataschool.ru/blog/news/clickhouse/free_course_02_clickhouse_engines/
-- Автор: Bigdataschool.ru   "Школа Больших Данных"

-- Шаг 1: Убедимся, что мы работаем в нашей базе данных.
CREATE DATABASE IF NOT EXISTS my_first_db;
USE my_first_db;


-- Шаг 2: Улучшенная версия таблицы access_logs с партиционированием.
-- В этой версии мы добавляем ключ партиционирования `PARTITION BY toYYYYMM(timestamp)`.
-- Это значит, что ClickHouse будет физически хранить данные за каждый месяц в отдельных "папках",
-- что критически ускоряет запросы с фильтрацией по дате.
CREATE TABLE access_logs_partitioned (
    timestamp   DateTime64(3),
    event_type  LowCardinality(String),
    user_id     UInt64,
    ip_address  IPv4,
    url         String,
    duration_ms UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp) -- Добавляем партиционирование по месяцам
ORDER BY (timestamp, user_id);


-- Шаг 3: Демонстрация работы движка ReplacingMergeTree.
-- Создадим таблицу для хранения профилей пользователей, где нам важна только последняя версия.
CREATE TABLE user_profiles (
    user_id UInt64,
    email String,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at) -- `updated_at` используется как столбец-версия
ORDER BY user_id; -- Ключ, по которому будут искаться дубликаты

-- Шаг 4: Вставка данных в таблицу с профилями.
INSERT INTO user_profiles VALUES (101, 'user101@email.com', '2025-06-20 10:00:00');
INSERT INTO user_profiles VALUES (102, 'user102@email.com', '2025-06-20 10:05:00');
INSERT INTO user_profiles VALUES (101, 'user101_new@email.com', '2025-06-20 11:00:00'); -- Новая версия для user_id=101

-- Шаг 5: Проверка данных сразу после вставки (могут быть видны все 3 строки).
SELECT * FROM user_profiles ORDER BY user_id, updated_at;

-- Шаг 6: Принудительное слияние данных для демонстрации (в production используется редко).
OPTIMIZE TABLE user_profiles FINAL;

-- Шаг 7: Проверка данных после слияния (осталась только последняя версия для user_id=101).
SELECT * FROM user_profiles ORDER BY user_id, updated_at;
