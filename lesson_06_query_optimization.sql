-- SQL-блокнот для Бесплатного онлайн курса "Основы ClickHouse"  по материалам статьи 6 урока "Оптимизация запросов в ClickHouse: индексы, EXPLAIN и лучшие практики"
-- полный текст статьи: https://bigdataschool.ru/blog/news/clickhouse/free_course_06_clickhouse_indexes/
-- Автор: Bigdataschool.ru   "Школа Больших Данных"

-- Шаг 1: Подготовка окружения.
CREATE DATABASE IF NOT EXISTS my_first_db;
USE my_first_db;

-- Создадим таблицу специально для этого урока.
-- Обратите внимание на ключ сортировки ORDER BY (event_date, user_id),
-- так как все примеры по работе с первичным ключом будут основаны на нем.
CREATE TABLE access_logs_optimized (
    event_date  Date,
    timestamp   DateTime,
    event_type  LowCardinality(String),
    user_id     UInt64,
    url         String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);

-- Вставим тестовые данные за несколько месяцев, чтобы продемонстрировать
-- работу фильтрации по партициям и ключам.
INSERT INTO access_logs_optimized VALUES
('2025-05-10', '2025-05-10 10:00:00', 'page_view', 101, '/'),
('2025-06-15', '2025-06-15 11:00:00', 'page_view', 102, '/pricing'),
('2025-06-20', '2025-06-20 12:00:00', 'click', 101, '/buy'),
('2025-06-25', '2025-06-25 13:00:00', 'page_view', 103, '/features'),
('2025-07-01', '2025-07-01 14:00:00', 'login', 102, '/login');


--------------------------------------------------------------------------------
-- Раздел 1: Анализ использования Первичного ключа
--------------------------------------------------------------------------------

-- Пример "ХОРОШЕГО" запроса, который эффективно использует первичный ключ.
EXPLAIN PLAN
SELECT count()
FROM access_logs_optimized
WHERE
    event_date >= '2025-06-01' AND event_date < '2025-07-01';

-- ВЫВОД EXPLAIN: Обратите внимание на строку "Marks".
-- Число прочитанных меток будет очень маленьким по сравнению с общим числом.

-- Пример "ПЛОХОГО" запроса, который НЕ использует первичный ключ.
EXPLAIN PLAN
SELECT count()
FROM access_logs_optimized
WHERE toMonth(event_date) = 6;

-- ВЫВОД EXPLAIN: Посмотрите на "Marks" снова.
-- Количество прочитанных меток будет равно общему количеству меток в таблице (Full Scan).


--------------------------------------------------------------------------------
-- Раздел 2: Создание и использование вторичных индексов (Data Skipping)
--------------------------------------------------------------------------------

-- Добавим вторичные индексы для столбцов, которые не входят в первичный ключ.
ALTER TABLE access_logs_optimized ADD INDEX idx_event_type event_type TYPE set(3) GRANULARITY 1;
ALTER TABLE access_logs_optimized ADD INDEX idx_url url TYPE bloom_filter() GRANULARITY 1;


-- Проверим, как работает вторичный индекс.
EXPLAIN PLAN
SELECT count()
FROM access_logs_optimized
WHERE event_type = 'click';

-- ВЫВОД EXPLAIN: Снова смотрим на "Marks". Число прочитанных меток должно быть
-- меньше, чем общее число, так как ClickHouse пропустит гранулы,
-- в которых нет значения 'click'.


--------------------------------------------------------------------------------
-- Раздел 3: Демонстрация PREWHERE
--------------------------------------------------------------------------------

-- PREWHERE выполняется до чтения всех столбцов, указанных в SELECT.
-- Это эффективно, если фильтр сильно сокращает выборку.
EXPLAIN PLAN
SELECT
    user_id,
    url,
    timestamp
FROM access_logs_optimized
PREWHERE event_type = 'page_view';

-- ВЫВОД EXPLAIN: ClickHouse сначала прочитает только один столбец `event_type`,
-- отфильтрует строки, и только для оставшихся прочитает `user_id`, `url`, `timestamp`.
