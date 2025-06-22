-- SQL-блокнот для Бесплатного онлайн курса "Основы ClickHouse"  по материалам статьи 5 урока "Глубокое погружение в движки MergeTree"
-- полный текст статьи: https://bigdataschool.ru/blog/news/free_course_05_clickhouse_DeepDiveEngines/
-- Автор: Bigdataschool.ru   "Школа Больших Данных"

-- Шаг 1: Подготовка окружения.
CREATE DATABASE IF NOT EXISTS my_first_db;
USE my_first_db;

-- Для некоторых примеров нам понадобится таблица 'access_logs'.
-- Убедитесь, что вы создали и наполнили ее с помощью скриптов из предыдущих уроков.


--------------------------------------------------------------------------------
-- Пример 1: Демонстрация работы ReplacingMergeTree
--------------------------------------------------------------------------------

-- Создаем таблицу для профилей пользователей.
-- Движок ReplacingMergeTree(updated_at) будет оставлять только строку с максимальным
-- значением в столбце `updated_at` для каждого уникального `user_id`.
CREATE TABLE user_profiles (
    user_id UInt64,
    email String,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

-- Вставляем данные: две версии для user_id = 101.
INSERT INTO user_profiles VALUES (101, 'user101@email.com', '2025-06-20 10:00:00');
INSERT INTO user_profiles VALUES (101, 'user101_new@email.com', '2025-06-20 11:00:00');

-- Проверяем до слияния (могут быть видны обе строки)
SELECT * FROM user_profiles;

-- Принудительно запускаем слияние для демонстрации.
-- ВАЖНО: В production используется редко.
OPTIMIZE TABLE user_profiles FINAL;

-- Проверяем после слияния (останется только последняя версия)
SELECT * FROM user_profiles;


--------------------------------------------------------------------------------
-- Пример 2: Демонстрация работы SummingMergeTree
--------------------------------------------------------------------------------

-- Создаем таблицу для сбора статистики.
-- Движок SummingMergeTree() будет автоматически суммировать числовые столбцы (views, clicks)
-- для строк с одинаковым ключом сортировки (event_date, campaign_id).
CREATE TABLE campaign_stats (
    event_date Date,
    campaign_id UInt32,
    views UInt64,
    clicks UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (event_date, campaign_id);

-- Вставляем "сырые" данные о событиях несколькими порциями.
INSERT INTO campaign_stats VALUES ('2025-06-20', 1, 100, 10);
INSERT INTO campaign_stats VALUES ('2025-06-20', 2, 150, 15);
INSERT INTO campaign_stats VALUES ('2025-06-20', 1, 50, 5); -- Еще данные для кампании #1

-- Проверяем до слияния (будут видны все три строки)
SELECT * FROM campaign_stats ORDER BY event_date, campaign_id;

-- Принудительное слияние
OPTIMIZE TABLE campaign_stats FINAL;

-- Проверяем после слияния (данные для кампании #1 были просуммированы)
SELECT * FROM campaign_stats ORDER BY event_date, campaign_id;


--------------------------------------------------------------------------------
-- Пример 3: Демонстрация работы AggregatingMergeTree
--------------------------------------------------------------------------------

-- Создаем таблицу для хранения "состояний" агрегатных функций.
-- Столбец `visitors` имеет тип AggregateFunction, который хранит не сами значения,
-- а промежуточное состояние функции `uniq`.
CREATE TABLE daily_unique_users (
    day Date,
    url String,
    visitors AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (day, url);

-- Вставляем данные, предварительно агрегируя их с помощью функции `uniqState`.
-- `uniqState` создает то самое "состояние", которое будет храниться в таблице.
INSERT INTO daily_unique_users
SELECT
    toDate(timestamp) AS day,
    url,
    uniqState(user_id) AS visitors
FROM access_logs -- Используем нашу таблицу с сырыми логами
WHERE toDate(timestamp) = '2024-06-19' -- Возьмем данные за один день для примера
GROUP BY day, url;

-- Получаем итоговый результат.
-- Функция `uniqMerge` "схлопывает" все сохраненные состояния в финальное значение.
SELECT
    day,
    url,
    uniqMerge(visitors) AS unique_visitors
FROM daily_unique_users
GROUP BY day, url;


--------------------------------------------------------------------------------
-- Пример 4: Демонстрация работы CollapsingMergeTree (бонусный пример)
--------------------------------------------------------------------------------

-- Создаем таблицу для отслеживания "состояний", например, активных сессий.
-- Движок CollapsingMergeTree(Sign) будет удалять пары строк с одинаковым ключом
-- и противоположными значениями в столбце Sign (1 и -1).
CREATE TABLE user_session_log (
    user_id UInt64,
    session_start DateTime,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY user_id, session_start;

-- Вставляем события:
-- Пользователь 301: начал и закончил сессию.
-- Пользователь 302: только начал сессию.
INSERT INTO user_session_log VALUES (301, '2025-06-21 12:00:00', 1);  -- Начало сессии
INSERT INTO user_session_log VALUES (301, '2025-06-21 12:00:00', -1); -- Конец сессии
INSERT INTO user_session_log VALUES (302, '2025-06-21 13:00:00', 1);  -- Начало сессии

-- Проверяем до слияния (видны все 3 строки)
SELECT * FROM user_session_log ORDER BY user_id;

-- Принудительное слияние
OPTIMIZE TABLE user_session_log FINAL;

-- Проверяем после слияния: сессия пользователя 301 "схлопнулась",
-- а активная сессия пользователя 302 осталась.
SELECT * FROM user_session_log ORDER BY user_id;