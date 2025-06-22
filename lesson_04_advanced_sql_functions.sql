-- SQL-блокнот для Бесплатного онлайн курса "Основы ClickHouse"  по материалам статьи 4 урока "Продвинутые функции SQL в ClickHouse: обработка строк, дат и условная логика"
-- полный текст статьи: https://bigdataschool.ru/blog/news/free_course_04_clickhouse_advanced_DML/
-- Автор: Bigdataschool.ru   "Школа Больших Данных"

-- Шаг 1: Подготовка окружения.
CREATE DATABASE IF NOT EXISTS my_first_db;
USE my_first_db;

-- Для гарантии работоспособности примеров, убедитесь, что таблица access_logs
-- создана и содержит несколько строк, включая специфичные для наших примеров.
CREATE TABLE IF NOT EXISTS access_logs (
    timestamp   DateTime64(3),
    event_type  LowCardinality(String),
    user_id     UInt64,
    ip_address  IPv4,
    url         String,
    duration_ms UInt32
)
ENGINE = MergeTree()
ORDER BY (timestamp, user_id);

INSERT INTO access_logs (timestamp, event_type, user_id, ip_address, url, duration_ms) VALUES
('2025-06-21 10:00:00.000', 'login', 201, '192.168.1.1', '/home', 150),
('2025-06-21 10:05:00.000', 'page_view', 202, '10.0.0.8', '/contact', 500),
('2025-06-22 11:00:00.000', 'click', 202, '10.0.0.8', '/submit_form', 70);


-- Шаг 2: Демонстрация функций для работы со строками.
-- Извлекаем части из IP-адреса и создаем новое описание события с помощью конкатенации.
SELECT
    user_id,
    url,
    concat('Event type "', event_type, '" on page ', url) AS event_description,
    substring(ip_address, 1, position(ip_address, '.') - 1) AS first_ip_segment
FROM
    my_first_db.access_logs
WHERE
    ip_address = '192.168.1.1'
LIMIT 5;


-- Шаг 3: Демонстрация функций для работы с датой и временем.
-- Группируем события по дню недели, чтобы увидеть распределение активности.
SELECT
    toDayOfWeek(timestamp) AS day_of_week,
    count() AS event_count
FROM
    my_first_db.access_logs
GROUP BY
    day_of_week
ORDER BY
    day_of_week;


-- Шаг 4: Демонстрация условной логики с CASE.
-- Сегментируем события по их продолжительности на 'Short', 'Normal' и 'Very Long'.
SELECT
    user_id,
    duration_ms,
    CASE
        WHEN duration_ms > 400 THEN 'Very Long'
        WHEN duration_ms > 150 AND duration_ms <= 400 THEN 'Normal'
        ELSE 'Short'
    END AS duration_category
FROM
    my_first_db.access_logs
LIMIT 10;


-- Шаг 5: Демонстрация работы подзапроса.
-- Находим все события тех пользователей, которые хотя бы раз посещали страницу '/contact'.
SELECT
    timestamp,
    user_id,
    event_type,
    url
FROM
    my_first_db.access_logs
WHERE
    user_id IN (
        SELECT DISTINCT user_id
        FROM my_first_db.access_logs
        WHERE url = '/contact'
    )
ORDER BY user_id, timestamp;