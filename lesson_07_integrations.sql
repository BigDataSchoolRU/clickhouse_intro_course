-- SQL-блокнот для Бесплатного онлайн курса "Основы ClickHouse"  по материалам статьи 7 урока ""Интеграции ClickHouse: работа с MySQL, S3, Kafka и внешними словарями"
-- полный текст статьи: https://bigdataschool.ru/blog/news/clickhouse/free_course_07_clickhouse_integration/
-- Автор: Bigdataschool.ru   "Школа Больших Данных"

-- Шаг 1: Подготовка окружения.
CREATE DATABASE IF NOT EXISTS my_first_db;
USE my_first_db;


--------------------------------------------------------------------------------
-- Пример 1: Движок для внешней БД (MySQL) - ШАБЛОН
--------------------------------------------------------------------------------

-- Этот код не будет работать без реального подключения к MySQL.
-- Он приведен здесь как шаблон для демонстрации синтаксиса.
/*
CREATE TABLE users_from_mysql (
    user_id UInt64,
    user_name String,
    registration_date Date
)
ENGINE = MySQL('mysql_host:3306', 'mysql_db', 'users', 'mysql_user', 'password');

-- Пример запроса, который объединяет локальные данные с внешними
SELECT
    a.event_type,
    u.user_name,
    count()
FROM access_logs_partitioned AS a
JOIN users_from_mysql AS u ON a.user_id = u.user_id
GROUP BY a.event_type, u.user_name;
*/


--------------------------------------------------------------------------------
-- Пример 2: Запрос к файлам в S3 - ШАБЛОН
--------------------------------------------------------------------------------

-- Этот код не будет работать без реального S3 бакета и прав доступа.
-- Он приведен как шаблон для демонстрации использования функции s3().
/*
SELECT
    request_method,
    status_code,
    count()
FROM s3(
    'https://my-bucket.s3.us-east-1.amazonaws.com/logs/archive-*.csv.gz',
    'Your_AWS_ACCESS_KEY_ID',
    'Your_AWS_SECRET_ACCESS_KEY',
    'CSVWithNames',
    'request_method String, status_code UInt16'
)
GROUP BY request_method, status_code;
*/


--------------------------------------------------------------------------------
-- Пример 3: Пайплайн для интеграции с Kafka - ШАБЛОН
--------------------------------------------------------------------------------

-- Этот код не будет работать без запущенного кластера Kafka.
-- Он приведен для демонстрации архитектуры пайплайна.
/*
-- 1. Таблица-приемник из Kafka
CREATE TABLE kafka_events (
    raw_message String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9092',
    kafka_topic_list = 'events-topic',
    kafka_group_name = 'clickhouse-group1',
    kafka_format = 'JSONAsString';

-- 2. Целевая таблица для хранения
CREATE TABLE events_storage (
    timestamp DateTime,
    event_type String,
    user_id UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, event_type);

-- 3. Материализованное представление для автоматической перекачки данных
CREATE MATERIALIZED VIEW kafka_mv TO events_storage AS
SELECT
    parseDateTimeBestEffort(JSONExtractString(raw_message, 'ts')) AS timestamp,
    JSONExtractString(raw_message, 'event') AS event_type,
    JSONExtractUInt(raw_message, 'uid') AS user_id
FROM kafka_events;
*/


--------------------------------------------------------------------------------
-- Пример 4: Внешние словари - ПОЛНОСТЬЮ РАБОЧИЙ ПРИМЕР
--------------------------------------------------------------------------------

-- Создадим локальную таблицу-источник для нашего словаря.
-- В реальной жизни это могла бы быть таблица в MySQL или PostgreSQL.
CREATE TABLE user_dictionary_source (
    user_id UInt64,
    user_name String,
    country LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY user_id;

-- Наполним ее данными.
INSERT INTO user_dictionary_source VALUES (101, 'Alice', 'USA'), (102, 'Bob', 'Germany'), (105, 'Charles', 'USA');

-- Теперь создадим сам словарь с помощью DDL.
-- Он будет загружать данные из нашей таблицы-источника и хранить их в памяти
-- для быстрого доступа.
CREATE DICTIONARY user_names_dict (
    user_id UInt64,
    user_name String,
    country String
)
PRIMARY KEY user_id
SOURCE(CLICKHOUSE(TABLE 'user_dictionary_source'))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 300); -- Кэшировать данные на 5 минут

-- Теперь используем словарь для обогащения данных "на лету" с помощью функции dictGet.
-- Возьмем ID пользователей из нашей таблицы логов и получим их имена и страны из словаря.
-- Предполагается, что таблица `access_logs_partitioned` у вас уже есть.
SELECT
    user_id,
    dictGet('user_names_dict', 'user_name', user_id) AS user_name,
    dictGet('user_names_dict', 'country', user_id) AS country,
    count() as event_count
FROM access_logs_partitioned
WHERE has('user_names_dict', user_id) -- Проверяем, что ID есть в словаре
GROUP BY user_id, user_name, country;
