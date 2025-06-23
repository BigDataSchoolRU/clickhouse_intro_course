-- SQL-блокнот для Бесплатного онлайн курса "Основы ClickHouse"  по материалам статьи 8 урока "Аналитические суперсилы ClickHouse: Оконные и массивные функции"
-- полный текст статьи: https://bigdataschool.ru/blog/news/clickhouse/free_course_08_clickhouse_functions/
-- Автор: Bigdataschool.ru   "Школа Больших Данных"

-- Шаг 1: Подготовка окружения.
CREATE DATABASE IF NOT EXISTS my_first_db;
USE my_first_db;

-- Для примеров с оконными функциями нам понадобится таблица 'access_logs'.
-- Создадим ее и наполним данными, обеспечив наличие нескольких событий для одних и тех же пользователей.
DROP TABLE IF EXISTS access_logs; -- Удаляем, если существует, для чистоты эксперимента
CREATE TABLE access_logs (
    timestamp   DateTime,
    event_type  LowCardinality(String),
    user_id     UInt64,
    duration_ms UInt32,
    url         String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (user_id, timestamp);

INSERT INTO access_logs VALUES
('2025-07-01 10:00:00', 'page_view', 101, 1500, '/home'),
('2025-07-01 10:01:00', 'click', 101, 50, '/buy'),
('2025-07-01 10:02:00', 'page_view', 101, 3500, '/profile'),
('2025-07-01 11:00:00', 'page_view', 202, 500, '/'),
('2025-07-01 11:01:30', 'page_view', 202, 8000, '/products'),
('2025-07-01 11:03:00', 'click', 202, 120, '/add_to_cart');


--------------------------------------------------------------------------------
-- Раздел 1: Демонстрация Оконных функций
--------------------------------------------------------------------------------

-- Пример 1.1: Ранжирование с помощью ROW_NUMBER()
-- Находим две самые долгие по продолжительности сессии для каждого пользователя.
SELECT * FROM (
    SELECT
        user_id,
        url,
        duration_ms,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY duration_ms DESC) as session_rank
    FROM access_logs
)
WHERE session_rank <= 2;


-- Пример 1.2: Смещение с помощью LAG()
-- Рассчитываем время в секундах между последовательными событиями каждого пользователя.
SELECT
    user_id,
    timestamp,
    event_type,
    LAG(timestamp, 1) OVER (PARTITION BY user_id ORDER BY timestamp) AS prev_event_timestamp,
    toSecond(timestamp) - toSecond(prev_event_timestamp) AS time_diff_seconds
FROM access_logs
ORDER BY user_id, timestamp;


-- Пример 1.3: Агрегаты в окне с помощью COUNT() OVER ()
-- Считаем "нарастающий итог" - порядковый номер события для каждого пользователя.
SELECT
    user_id,
    timestamp,
    event_type,
    COUNT(*) OVER (PARTITION BY user_id ORDER BY timestamp) as running_event_count
FROM access_logs;


--------------------------------------------------------------------------------
-- Раздел 2: Демонстрация Массивных функций
--------------------------------------------------------------------------------

-- Пример 2.1: "Разворачивание" массива с помощью arrayJoin
-- Создадим таблицу со статьями и их тегами в виде массива.
CREATE TABLE articles (
    article_id UInt64,
    title String,
    tags Array(String)
)
ENGINE = Memory; -- Используем движок Memory для простоты примера

INSERT INTO articles VALUES
(1, 'Про ClickHouse', ['clickhouse', 'db', 'analytics']),
(2, 'Про Python', ['python', 'programming']),
(3, 'Аналитика на Python и ClickHouse', ['python', 'clickhouse', 'analytics']);

-- Используем arrayJoin, чтобы создать отдельную строку для каждого тега.
SELECT
    article_id,
    title,
    arrayJoin(tags) AS tag
FROM articles;

-- Теперь на основе "развернутой" таблицы мы можем легко посчитать популярность тегов.
SELECT
    tag,
    count() AS tag_count
FROM (
    SELECT arrayJoin(tags) AS tag
    FROM articles
)
GROUP BY tag
ORDER BY tag_count DESC;


-- Пример 2.2: Фильтрация массивов с помощью arrayFilter (функция высшего порядка)
-- Создадим таблицу с массивом посещенных URL.
CREATE TABLE site_navigations (
    user_id UInt64,
    visited_urls Array(String)
)
ENGINE = Memory;

INSERT INTO site_navigations VALUES
(101, ['https://bigdataschool.ru/a', 'https://google.com', 'https://bigdataschool.ru/b']),
(202, ['https://clickhouse.com', 'https://yandex.ru']);

-- Используем arrayFilter и лямбда-функцию (x -> ...), чтобы выбрать только
-- внутренние URL, которые начинаются с 'https://bigdataschool.ru'.
SELECT
    user_id,
    arrayFilter(x -> x LIKE 'https://bigdataschool.ru%', visited_urls) AS internal_urls
FROM site_navigations;
