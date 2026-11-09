import csv
import io
from datetime import date

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import config
from .auth import current_user
from .report import availability, build_report, get_client

app = FastAPI(title="BionicPRO Reports API", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.FRONTEND_ORIGIN],
    allow_methods=["GET"],
    allow_headers=["Authorization"],
)


@app.get("/health")
def health() -> dict:
    try:
        get_client().execute("SELECT 1")
        clickhouse_ok = True
    except Exception:
        clickhouse_ok = False
    return {"status": "ok" if clickhouse_ok else "degraded", "clickhouse": clickhouse_ok}


@app.get("/api/v1/reports/availability")
def reports_availability(user_id: str = Depends(current_user)) -> dict:
    available = availability()
    if available is None:
        raise HTTPException(status_code=404, detail="No report data has been processed yet")
    return available


@app.get("/api/v1/reports/usage")
def usage_report(
    period_from: date = Query(..., alias="from"),
    period_to: date = Query(..., alias="to"),
    format: str = Query("json", pattern="^(json|csv)$"),
    user_id: str = Depends(current_user),
) -> Response:
    if period_from > period_to:
        raise HTTPException(status_code=422, detail="from must be <= to")
    if (period_to - period_from).days > config.MAX_PERIOD_DAYS:
        raise HTTPException(status_code=422, detail=f"period exceeds {config.MAX_PERIOD_DAYS} days")

    available = availability()
    if available is None:
        raise HTTPException(status_code=404, detail="No report data has been processed yet")

    processed_until = available["data_available_until"]
    if period_from > processed_until:
        raise HTTPException(
            status_code=404,
            detail=f"Data for the requested period has not been processed by ETL yet "
            f"(available: {available['data_available_from']} .. {processed_until})",
        )
    if period_to > processed_until:
        return JSONResponse(
            status_code=422,
            content={
                "detail": f"Requested period extends beyond the data processed by Airflow "
                          f"(available up to {processed_until})",
                "data_available_until": processed_until.isoformat(),
            },
        )

    report = build_report(user_id, period_from, period_to)
    if report is None:
        raise HTTPException(status_code=404, detail="No report data for the given user and period")

    if format == "csv":
        return _to_csv(report)
    return JSONResponse(report)


def _to_csv(report: dict) -> Response:
    user = report["user"]
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        ["dt", "usage_hours", "sessions", "errors", "battery_min", "battery_max",
         "user_id", "user_name", "model", "prosthesis_id", "purchase_date"]
    )
    for row in report["daily"]:
        writer.writerow(
            [row["dt"], row["usage_hours"], row["sessions"], row["errors"],
             row["battery_min"], row["battery_max"],
             user["user_id"], user.get("user_name", ""), user.get("model", ""),
             user.get("prosthesis_id", ""), user.get("purchase_date") or ""]
        )
    return Response(
        content=buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="usage-report-{user["user_id"]}.csv"'},
    )
