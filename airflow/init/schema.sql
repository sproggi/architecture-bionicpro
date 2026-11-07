CREATE DATABASE IF NOT EXISTS reports;

CREATE TABLE IF NOT EXISTS reports.dim_users (
    user_id       String,
    user_name     String,
    email         String,
    prosthesis_id String,
    model         String,
    purchase_date Date,
    updated_at    DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS reports.fact_usage_daily (
    dt            Date,
    user_id       String,
    prosthesis_id String,
    usage_hours   Float32,
    sessions      UInt32,
    errors        UInt32,
    battery_min   UInt8,
    battery_max   UInt8,
    updated_at    DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (user_id, dt);
