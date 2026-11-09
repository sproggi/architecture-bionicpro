from datetime import date

import clickhouse_driver

from . import config

DAILY_COLUMNS = ("dt", "usage_hours", "sessions", "errors", "battery_min", "battery_max")


def get_client() -> clickhouse_driver.Client:
    return clickhouse_driver.Client(
        host=config.CH_HOST,
        port=config.CH_PORT,
        user=config.CH_USER,
        password=config.CH_PASSWORD,
        database=config.CH_DB,
    )


def availability() -> dict | None:
    rows = get_client().execute("SELECT min(dt), max(dt) FROM fact_usage_daily FINAL")
    (data_min, data_max) = rows[0]
    if data_min is None:
        return None
    return {"data_available_from": data_min, "data_available_until": data_max}


def _fetch_user(client, user_id: str) -> dict | None:
    rows = client.execute(
        """
        SELECT user_id, user_name, email, prosthesis_id, model, purchase_date
        FROM dim_users FINAL
        WHERE user_id = %(user_id)s
        LIMIT 1
        """,
        {"user_id": user_id},
    )
    if not rows:
        return None
    (user_id, user_name, email, prosthesis_id, model, purchase_date) = rows[0]
    return {
        "user_id": user_id,
        "user_name": user_name,
        "email": email,
        "prosthesis_id": prosthesis_id,
        "model": model,
        "purchase_date": purchase_date.isoformat() if purchase_date else None,
    }


def _fetch_daily(client, user_id: str, period_from: date, period_to: date) -> list[tuple]:
    return client.execute(
        """
        SELECT dt, usage_hours, sessions, errors, battery_min, battery_max
        FROM fact_usage_daily FINAL
        WHERE user_id = %(user_id)s AND dt BETWEEN %(dt_from)s AND %(dt_to)s
        ORDER BY dt
        """,
        {"user_id": user_id, "dt_from": period_from, "dt_to": period_to},
    )


def _summarize(daily: list[tuple]) -> dict:
    usage_hours = sum(row[1] for row in daily)
    sessions = sum(row[2] for row in daily)
    errors = sum(row[3] for row in daily)
    battery_values_min = [row[4] for row in daily]
    battery_values_max = [row[5] for row in daily]
    return {
        "days": len(daily),
        "total_usage_hours": round(usage_hours, 2),
        "avg_usage_hours_per_day": round(usage_hours / len(daily), 2),
        "total_sessions": sessions,
        "total_errors": errors,
        "battery_min_avg": round(sum(battery_values_min) / len(battery_values_min), 1),
        "battery_max_avg": round(sum(battery_values_max) / len(battery_values_max), 1),
    }


def build_report(user_id: str, period_from: date, period_to: date) -> dict | None:
    client = get_client()
    daily = _fetch_daily(client, user_id, period_from, period_to)
    if not daily:
        return None
    user = _fetch_user(client, user_id) or {"user_id": user_id}
    return {
        "user": user,
        "period": {"from": period_from.isoformat(), "to": period_to.isoformat()},
        "summary": _summarize(daily),
        "daily": [
            {
                "dt": row[0].isoformat(),
                "usage_hours": round(float(row[1]), 2),
                "sessions": int(row[2]),
                "errors": int(row[3]),
                "battery_min": int(row[4]),
                "battery_max": int(row[5]),
            }
            for row in daily
        ],
    }
