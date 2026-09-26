-- Виконується ОДИН раз, коли том postgres порожній (docker-entrypoint-initdb.d).
-- Створює сховище поруч із метаданими Airflow: окрема база, окрема роль.
--
-- Контракт Bronze задано тут навмисно: таблицю створює платформа, а не Spark.
-- Spark лише дописує в неї (append) — тому типи колонок фіксовані й однакові
-- у всіх студентів, і checkpoint-числа зі SPEC.md порівнювані.

CREATE ROLE taxi WITH LOGIN PASSWORD 'taxi';
CREATE DATABASE taxi_dwh OWNER taxi;

\connect taxi_dwh

CREATE SCHEMA IF NOT EXISTS bronze AUTHORIZATION taxi;
CREATE SCHEMA IF NOT EXISTS silver AUTHORIZATION taxi;
CREATE SCHEMA IF NOT EXISTS gold   AUTHORIZATION taxi;

-- Сирий шар: рівно те, що приїхало з landing-зони, плюс метадані ingestion.
-- payload лишається ТЕКСТОМ: Bronze нічого не парсить (контракт шару).
CREATE TABLE bronze.raw_events (
    event_id      text        NOT NULL,
    event_type    text        NOT NULL,
    ride_id       text,
    occurred_at   timestamptz,
    source        text,
    payload       text,
    _source_file  text        NOT NULL,
    _ingested_at  timestamptz NOT NULL
);

CREATE INDEX ix_raw_events_occurred_at ON bronze.raw_events (occurred_at);
CREATE INDEX ix_raw_events_source_file ON bronze.raw_events (_source_file);

ALTER TABLE bronze.raw_events OWNER TO taxi;
GRANT ALL ON SCHEMA public TO taxi;
