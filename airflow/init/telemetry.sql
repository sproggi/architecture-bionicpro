CREATE TABLE IF NOT EXISTS telemetry (
    id            BIGSERIAL PRIMARY KEY,
    user_id       TEXT NOT NULL,
    prosthesis_id TEXT NOT NULL,
    session_id    UUID NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    duration_sec  INTEGER NOT NULL CHECK (duration_sec > 0),
    error_count   INTEGER NOT NULL DEFAULT 0,
    battery_level SMALLINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry (ts);
CREATE INDEX IF NOT EXISTS idx_telemetry_user_ts ON telemetry (user_id, ts);

INSERT INTO telemetry (user_id, prosthesis_id, session_id, ts, duration_sec, error_count, battery_level)
SELECT
    u.user_id,
    u.prosthesis_id,
    gen_random_uuid(),
    (now() - make_interval(days => d.day_idx)) + make_interval(hours => s.hour_idx),
    1200 + (seed % 3600),
    seed % 3,
    (40 + seed % 40)::smallint
FROM (VALUES ('u-101', 'p-201'), ('u-102', 'p-202'), ('u-103', 'p-203')) AS u(user_id, prosthesis_id)
CROSS JOIN generate_series(0, 3) AS d(day_idx)
CROSS JOIN generate_series(0, 4) AS s(hour_idx)
CROSS JOIN LATERAL (
    SELECT abs(hashtext(u.user_id || d.day_idx::text || s.hour_idx::text)) % 10000 AS seed
) AS seed_t(seed);
