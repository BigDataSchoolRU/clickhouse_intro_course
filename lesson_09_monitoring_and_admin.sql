
-- SQL-блокнот для Бесплатного онлайн курса "Основы ClickHouse"  по материалам статьи 9 урока "Администрирование и мониторинг ClickHouse"
-- полный текст статьи: https://bigdataschool.ru/blog/news/clickhouse/free_course_09_clickhouse_admin_monitoring/
-- Автор: Bigdataschool.ru   "Школа Больших Данных"

-- Шаг 1: Подготовка окружения.
CREATE DATABASE IF NOT EXISTS my_first_db;
USE my_first_db;


--------------------------------------------------------------------------------
-- Раздел 1: Мониторинг с помощью системных таблиц
--------------------------------------------------------------------------------

-- 1.1. Просмотр текущих метрик в реальном времени.
SELECT metric, value FROM system.metrics WHERE metric IN ('Query', 'HTTPConnection', 'TCPConnection');


-- 1.2. Просмотр кумулятивных счетчиков событий.
SELECT event, value FROM system.events WHERE event IN ('Merge', 'FailedQuery', 'FailedSelectQuery');


-- 1.3. Просмотр текущих выполняемых запросов.
-- (Запустите какой-нибудь долгий SELECT в другой вкладке, чтобы увидеть его здесь).
SELECT
    query_id,
    user,
    client_hostname,
    query_duration_ms,
    memory_usage,
    query
FROM system.processes;


-- 1.4. Мониторинг процессов слияния данных.
SELECT * FROM system.merges;


-- 1.5. Мониторинг репликации (для кластерных инсталляций).
-- ВАЖНО: Этот запрос вернет данные только если у вас настроена репликация.
SELECT * FROM system.replicas;


--------------------------------------------------------------------------------
-- Раздел 2: Управление пользователями и правами доступа
--------------------------------------------------------------------------------

-- Создадим тестовую базу и таблицу, на которых будем выдавать права.
CREATE DATABASE IF NOT EXISTS my_app;
CREATE TABLE IF NOT EXISTS my_app.events (col1 String, col2 String) ENGINE = Log;

-- 2.1. Создание пользователя и роли.
CREATE USER IF NOT EXISTS analyst IDENTIFIED WITH sha256_password BY 'a_very_secure_password_123';
CREATE ROLE IF NOT EXISTS analytics_role;


-- 2.2. Выдача прав роли и назначение роли пользователю.
GRANT SELECT ON my_app.* TO analytics_role;
GRANT INSERT(col1, col2) ON my_app.events TO analytics_role;
GRANT analytics_role TO analyst;


-- 2.3. Создание профиля настроек для ограничения ресурсов.
CREATE SETTINGS PROFILE IF NOT EXISTS analytics_profile
    SETTINGS max_memory_usage = 10000000000, readonly = 0; -- 10 GB

-- Применим этот профиль к нашей роли.
ALTER ROLE analytics_role SETTINGS PROFILE 'analytics_profile';


--------------------------------------------------------------------------------
-- Раздел 3: Резервное копирование
--------------------------------------------------------------------------------

-- 3.1. Утилита `clickhouse-backup` (Примеры для командной строки)
-- Следующие команды выполняются в ТЕРМИНАЛЕ, а не в SQL-клиенте.
/*
# Создать локальный бэкап всех таблиц
clickhouse-backup create my_full_backup

# Создать бэкап только одной таблицы
clickhouse-backup create --tables "my_first_db.access_logs" my_table_backup

# Загрузить бэкап в S3 (требует предварительной настройки)
clickhouse-backup upload my_full_backup
*/


-- 3.2. Ручной метод с помощью ALTER TABLE ... FREEZE
-- Эта SQL-команда создает "мгновенный снимок" данных таблицы.
-- Данные не копируются, а создаются жесткие ссылки в директории `shadow/`.
ALTER TABLE my_app.events FREEZE;

-- После выполнения этой команды необходимо вручную скопировать и заархивировать
-- содержимое папки /var/lib/clickhouse/shadow/<номер>...
