import json
import os
from datetime import datetime, timedelta, timezone

import clickhouse_driver
import psycopg2
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator

WATERMARK_KEY = "telemetry_watermark"
OLAP_DB = os.environ.get("CH_DB", "reports")

FACT_COLUMNS = [
    "dt",
    "user_id",
    "prosthesis_id",
    "usage_hours",
    "sessions",
    "errors",
    "battery_min",
    "battery_max",
    "updated_at",
]

DIM_COLUMNS = [
    "user_id",
    "user_name",
    "email",
    "prosthesis_id",
    "model",
    "purchase_date",
    "updated_at",
]


def get_postgres_dsn():
    return (
        f"host={os.environ.get('PG_HOST', 'telemetry_db')} "
        f"port={os.environ.get('PG_PORT', '5432')} "
        f"dbname={os.environ.get('PG_DB', 'telemetry')} "
        f"user={os.environ.get('PG_USER', 'telemetry')} "
        f"password={os.environ.get('PG_PASSWORD', 'telemetry_password')}"
    )


def get_clickhouse_client():
    return clickhouse_driver.Client(
        host=os.environ.get("CH_HOST", "clickhouse"),
        port=int(os.environ.get("CH_PORT", "9000")),
        user=os.environ.get("CH_USER", "default"),
        password=os.environ.get("CH_PASSWORD", ""),
        database=OLAP_DB,
    )


def bitrix24_list(method, params=None):
    base_url = os.environ.get("BITRIX24_BASE_URL", "").rstrip("/")
    auth_token = os.environ.get("BITRIX24_AUTH_TOKEN", "")
    if not base_url or not auth_token:
        raise AirflowException("BITRIX24_BASE_URL / BITRIX24_AUTH_TOKEN are not set")

    items = []
    start = 0
    while True:
        payload = dict(params or {})
        payload["start"] = start
        response = requests.post(
            f"{base_url}/rest/{method}",
            params={"auth": auth_token},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        items.extend(data.get("result", []))
        start = data.get("result_next")
        if start is None:
            break
    return items


def extract_telemetry(**context):
    watermark = Variable.get(WATERMARK_KEY, default_var=None)
    if not watermark:
        watermark = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")

    sql = """
        SELECT user_id,
               prosthesis_id,
               session_id::text AS session_id,
               to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS ts,
               duration_sec,
               error_count,
               battery_level::int AS battery_level
        FROM telemetry
        WHERE ts > %(watermark)s
        ORDER BY ts
    """

    with psycopg2.connect(get_postgres_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, {"watermark": watermark})
        columns = [column.name for column in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    if rows:
        Variable.set(WATERMARK_KEY, max(row["ts"] for row in rows))

    context["ti"].xcom_push(key="telemetry", value=json.dumps(rows))
    print(f"extract_telemetry: {len(rows)} rows after {watermark}")
    return len(rows)


def extract_crm(**context):
    deals = bitrix24_list("crm.deal.list")
    contacts = bitrix24_list("crm.contact.list")
    contact_by_id = {contact["ID"]: contact for contact in contacts}

    customers = []
    for deal in deals:
        contact_id = (deal.get("CONTACT_IDS") or [None])[0]
        contact = contact_by_id.get(contact_id, {})
        customers.append(
            {
                "user_id": contact_id,
                "user_name": " ".join(
                    part
                    for part in (contact.get("LAST_NAME", ""), contact.get("NAME", ""))
                    if part
                ),
                "email": contact.get("EMAIL", ""),
                "prosthesis_id": deal.get("PROSTHESIS_ID", ""),
                "model": deal.get("PRODUCT", ""),
                "purchase_date": (deal.get("DATE_CLOSED") or "1970-01-01")[:10],
            }
        )

    context["ti"].xcom_push(key="customers", value=json.dumps(customers))
    print(f"extract_crm: {len(customers)} customers from {len(deals)} deals")
    return len(customers)


def transform_join(**context):
    ti = context["ti"]
    telemetry = json.loads(ti.xcom_pull(task_ids="extract_telemetry", key="telemetry") or "[]")
    customers = json.loads(ti.xcom_pull(task_ids="extract_crm", key="customers") or "[]")
    customer_by_user = {c["user_id"]: c for c in customers if c.get("user_id")}

    aggregated = {}
    for row in telemetry:
        key = (row["user_id"], row["prosthesis_id"], row["ts"][:10])
        bucket = aggregated.setdefault(
            key,
            {"usage_sec": 0, "sessions": set(), "errors": 0, "battery": []},
        )
        bucket["usage_sec"] += row["duration_sec"]
        bucket["sessions"].add(row["session_id"])
        bucket["errors"] += row["error_count"]
        bucket["battery"].append(row["battery_level"])

    fact_rows = []
    for (user_id, prosthesis_id, dt), bucket in sorted(aggregated.items()):
        fact_rows.append(
            {
                "dt": dt,
                "user_id": user_id,
                "prosthesis_id": prosthesis_id,
                "usage_hours": round(bucket["usage_sec"] / 3600.0, 4),
                "sessions": len(bucket["sessions"]),
                "errors": bucket["errors"],
                "battery_min": max(0, min(255, int(min(bucket["battery"])))),
                "battery_max": max(0, min(255, int(max(bucket["battery"])))),
                "updated_at": f"{dt} 00:00:00",
            }
        )

    seen_users = set(customer_by_user)
    seen_users.update(user_id for user_id, _, _ in aggregated)
    snapshot_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    dim_rows = []
    for user_id in sorted(seen_users):
        customer = customer_by_user.get(user_id, {})
        dim_rows.append(
            {
                "user_id": user_id,
                "user_name": customer.get("user_name", ""),
                "email": customer.get("email", ""),
                "prosthesis_id": customer.get("prosthesis_id", ""),
                "model": customer.get("model", ""),
                "purchase_date": customer.get("purchase_date", "1970-01-01"),
                "updated_at": snapshot_at,
            }
        )

    ti.xcom_push(key="fact_rows", value=json.dumps(fact_rows))
    ti.xcom_push(key="dim_rows", value=json.dumps(dim_rows))
    print(f"transform_join: {len(fact_rows)} fact rows, {len(dim_rows)} dim rows")
    return len(fact_rows)


def to_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()


def to_datetime(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


FACT_ROW_CONVERTERS = {
    "dt": to_date,
    "usage_hours": float,
    "sessions": int,
    "errors": int,
    "battery_min": int,
    "battery_max": int,
    "updated_at": to_datetime,
}

DIM_ROW_CONVERTERS = {
    "purchase_date": to_date,
    "updated_at": to_datetime,
}


def load_olap(**context):
    ti = context["ti"]
    fact_rows = json.loads(ti.xcom_pull(task_ids="transform_join", key="fact_rows") or "[]")
    dim_rows = json.loads(ti.xcom_pull(task_ids="transform_join", key="dim_rows") or "[]")

    client = get_clickhouse_client()
    client.execute(
        f"INSERT INTO {OLAP_DB}.fact_usage_daily ({', '.join(FACT_COLUMNS)}) VALUES",
        [
            tuple(
                FACT_ROW_CONVERTERS.get(column, lambda value: value)(row[column])
                for column in FACT_COLUMNS
            )
            for row in fact_rows
        ],
    )
    client.execute(
        f"INSERT INTO {OLAP_DB}.dim_users ({', '.join(DIM_COLUMNS)}) VALUES",
        [
            tuple(
                DIM_ROW_CONVERTERS.get(column, lambda value: value)(row[column])
                for column in DIM_COLUMNS
            )
            for row in dim_rows
        ],
    )
    client.execute(f"OPTIMIZE TABLE {OLAP_DB}.fact_usage_daily FINAL")
    client.execute(f"OPTIMIZE TABLE {OLAP_DB}.dim_users FINAL")
    print(f"load_olap: {len(fact_rows)} fact rows, {len(dim_rows)} dim rows loaded")


def quality_checks(**context):
    ti = context["ti"]
    fact_rows = json.loads(ti.xcom_pull(task_ids="transform_join", key="fact_rows") or "[]")
    client = get_clickhouse_client()

    errors = []

    fact_count = client.execute(f"SELECT count() FROM {OLAP_DB}.fact_usage_daily FINAL")[0][0]
    dim_count = client.execute(f"SELECT count() FROM {OLAP_DB}.dim_users FINAL")[0][0]
    if fact_count == 0:
        errors.append("fact_usage_daily is empty")
    if dim_count == 0:
        errors.append("dim_users is empty")

    missing_users = client.execute(
        f"""
        SELECT count()
        FROM (SELECT DISTINCT user_id FROM {OLAP_DB}.fact_usage_daily FINAL) f
        LEFT ANTI JOIN (SELECT user_id FROM {OLAP_DB}.dim_users FINAL) d
            ON f.user_id = d.user_id
        """
    )[0][0]
    if missing_users:
        errors.append(f"{missing_users} fact users are missing in dim_users")

    sparse_users = client.execute(
        f"SELECT countIf(user_name = '' OR email = '') FROM {OLAP_DB}.dim_users FINAL"
    )[0][0]
    if dim_count and sparse_users * 10 > dim_count:
        errors.append(f"{sparse_users} dim users have no CRM attributes (> 10%)")

    expected_days = {row["dt"] for row in fact_rows}
    loaded_days = {
        day
        for (day,) in client.execute(
            f"SELECT toString(dt) FROM {OLAP_DB}.fact_usage_daily FINAL GROUP BY dt"
        )
    }
    missing_days = expected_days - loaded_days
    if missing_days:
        errors.append(f"days missing in mart: {sorted(missing_days)}")

    print(f"quality_checks: fact={fact_count}, dim={dim_count}, missing_users={missing_users}, sparse={sparse_users}")
    if errors:
        raise AirflowException("; ".join(errors))


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="reporting_mart",
    description="ETL: telemetry (PostgreSQL) + CRM (Bitrix24 REST) -> reporting mart (ClickHouse)",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["reports", "etl", "clickhouse"],
) as dag:
    extract_telemetry_task = PythonOperator(
        task_id="extract_telemetry",
        python_callable=extract_telemetry,
    )
    extract_crm_task = PythonOperator(
        task_id="extract_crm",
        python_callable=extract_crm,
    )
    transform_join_task = PythonOperator(
        task_id="transform_join",
        python_callable=transform_join,
    )
    load_olap_task = PythonOperator(
        task_id="load_olap",
        python_callable=load_olap,
    )
    quality_checks_task = PythonOperator(
        task_id="quality_checks",
        python_callable=quality_checks,
    )

    [extract_telemetry_task, extract_crm_task] >> transform_join_task >> load_olap_task >> quality_checks_task
