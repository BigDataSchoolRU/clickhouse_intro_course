-- SQL-блокнот для Бесплатного онлайн курса "Основы ClickHouse"  по материалам статьи 1 урока "Что такое ClickHouse: Полный гид по колоночной СУБД для сверхбыстрой аналитики" 
-- полный текст статьи: https://bigdataschool.ru/blog/news/clickhouse/free_course_01_clickhouse_intro/
-- Автор: Bigdataschool.ru   "Школа Больших Данных"

-- Шаг 1: Создание новой базы данных.
-- Мы используем "IF NOT EXISTS", чтобы команда не вызывала ошибку при повторном выполнении.
CREATE DATABASE IF NOT EXISTS my_first_db;

-- Шаг 2: Переключение в контекст созданной базы данных.
-- Все последующие команды будут выполняться внутри 'my_first_db'.
USE my_first_db;

-- Шаг 3: Создание таблицы для логов доступа.
-- Здесь мы определяем структуру таблицы, используя оптимальные типы данных,
-- и задаем основной движок MergeTree с ключом сортировки.
CREATE TABLE access_logs (
    timestamp   DateTime64(3),
    event_type  LowCardinality(String),
    user_id     UInt64,
    ip_address  IPv4,
    url         String,
    duration_ms UInt32
)
ENGINE = MergeTree()
ORDER BY (timestamp, user_id);

-- Шаг 4: Вставка тестовых данных.
-- Мы вставляем три строки, чтобы было с чем работать в аналитических запросах.
INSERT INTO access_logs VALUES
('2024-06-19 10:00:00.123', 'page_view', 101, '192.168.1.1', '/home', 150),
('2024-06-19 10:00:01.456', 'click', 101, '192.168.1.1', '/button_a', 20),
('2024-06-19 10:00:02.789', 'page_view', 102, '10.0.0.5', '/products', 300);

-- Шаг 5: Выполнение первого аналитического запроса.
-- Этот запрос демонстрирует мощь ClickHouse: он группирует данные по типу события,
-- считает количество уникальных пользователей и среднюю продолжительность события.
SELECT
    event_type,
    count(DISTINCT user_id) AS unique_users,
    avg(duration_ms) AS avg_duration_ms
FROM access_logs
GROUP BY event_type;
