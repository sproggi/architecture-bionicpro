# BionicPRO — подсистема отчётности

Личный отчёт пользователя об использовании протеза: данные с датчиков (телеметрия чипа)
объединяются с данными CRM (клиент, протез, условия покупки) в OLAP-витрине и выдаются
пользователю по запросу через API.

- Дизайн решения: [`docs/reports-architecture.md`](docs/reports-architecture.md)
- Диаграммы: [`c4-reports.drawio`](c4-reports.drawio) (контур отчётности), [`c4-updated.drawio`](c4-updated.drawio) (обновлённая общая архитектура)

## Как это работает

```
        ┌──────────── ETL (ежедневно 02:00, T+1) ────────────┐
        │  PostgreSQL (телеметрия) ──┐                        │
        │                            ├─ Airflow DAG ─> ClickHouse-витрина
        │  CRM (Битрикс24 REST) ─────┘     reporting_mart     │
        └─────────────────────────────────────────────────────┘
        Пользователь ─> Frontend (React, Keycloak PKCE S256)
                   ─> API отчётов (JWT, user_id из токена)
                   ─> SELECT по витрине (только свой отчёт)
                   ─> CSV/JSON в браузер
```

## Компоненты и порты

| Компонент | Роль | Порт (хост) |
|---|---|---|
| `frontend/` | React SPA: вход через Keycloak, выбор периода, «Download Report» | 3000 |
| `keycloak/` | OIDC-провайдер, realm `reports-realm` (PKCE S256, JWKS) | 8080 (admin: `admin`/`admin`) |
| `api/` | Python/FastAPI: валидация JWT, генерация отчёта из OLAP, Swagger `/docs` | 8000 |
| `airflow/` | ETL: DAG `reporting_mart` (extract → join → load → quality checks) | 8099 (UI) |
| ClickHouse | витрина `reports.fact_usage_daily`, `reports.dim_users` | 8123 (HTTP) |
| PostgreSQL (телеметрия) | исходник: сырые данные датчиков (заполняется мок-данными) | 5434 |
| PostgreSQL (Keycloak) | БД Keycloak | 5433 |
| CRM mock | имитация REST API Битрикс24 (`/crm/deal/list`, `/crm/contact/list`) | только внутри сети |

## Безопасность

- Фронтенд: **PKCE S256** через Keycloak; токен не передаётся в API как чужой — браузер сам шлёт `Authorization: Bearer <JWT>`.
- API: подпись JWT проверяется по **JWKS Keycloak (RS256)**, с кэшированием ключей.
- **Row-level access**: `user_id` берётся только из claim токена (`user_id`, fallback `sub`). Параметр запроса игнорируется — пользователь физически не может скачать чужой отчёт.
- Доступ к отчётам — только с realm-ролью **`prothetic_user`** (иначе `403`).
- **Граница данных**: API отдаёт только то, что уже обработал Airflow (`max(dt)` витрины). Запрос за будущий период → `422` + `data_available_until`; период целиком в будущем → `404`.

## Запуск

```bash
# 1) Keycloak + фронтенд (создаёт общую сеть bionicpro)
docker compose up -d --build

# 2) Пайплайн: Airflow + ClickHouse + телеметрия + CRM mock + API отчётов
cd airflow
docker compose up -d --build
```


После первого запуска:

- фронтенд: http://localhost:3000
- Keycloak: http://localhost:8080 (realm `reports-realm`)
- API / Swagger: http://localhost:8000/docs
- Airflow UI: http://localhost:8099 (DAG `reporting_mart` запущен без паузы;
  данные витрины наполняются при первом прогоне DAG — вручную запустить:
  UI → `reporting_mart` → ▶)

Если менялся `keycloak/realm-export.json` — сбросьте БД Keycloak:
`docker compose rm -f keycloak keycloak_db` и удалите каталог `./postgres-keycloak-data`, затем `docker compose up -d`.

## Демо-пользователи

| Логин | Пароль | Realm-роль | user_id в витрине |
|---|---|---|---|
| `prothetic1` | `prothetic123` | `prothetic_user` | `u-101` (Ivanov Ivan, BionicPRO X1) |
| `prothetic2` | `prothetic123` | `prothetic_user` | `u-102` (Petrova Anna) |
| `prothetic3` | `prothetic123` | `prothetic_user` | `u-103` (Sidorov Pavel) |
| `user1` / `user2` | `password123` | `user` (без доступа к отчётам → `403`) | — |

## Проверка в браузере

1. Откройте http://localhost:3000, войдите как `prothetic1` / `prothetic123`.
2. Выберите период (данные доступны с `2026-11-04` по последнюю дату, обработанную Airflow) и нажмите **Download Report** — скачается CSV.
3. Запрос за период в будущем (после `data_available_until`) — UI покажет, что данные ещё не обработаны, и/или сгенерирует отчёт до границы обработанных данных.
4. Войдите как `user1` — запрос отчёта вернёт `403` (нет роли).

## API

```
GET /health                          # статус + доступность ClickHouse
GET /api/v1/reports/availability     # какие даты обработал Airflow (auth)
GET /api/v1/reports/usage?from=2026-11-01&to=2026-11-07&format=json|csv   # auth
```

| Код | Смысл |
|---|---|
| `200` | отчёт (JSON: user + summary + daily; CSV: плоский файл с атрибутами клиента) |
| `401` | нет токена / невалидный / просрочен |
| `403` | нет realm-роли `prothetic_user` |
| `404` | данных за период нет (период ещё не обработан ETL или пользователь новый) |
| `422` | период выходит за обработанные данные (`data_available_until` в теле) / `from > to` / период > 366 дней |

Проверка без браузера:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/realms/reports-realm/protocol/openid-connect/token \
  -d "grant_type=password&client_id=reports-frontend&username=prothetic1&password=prothetic123" \
  | jq -r .access_token)

curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/reports/usage?from=2026-11-01&to=2026-11-07&format=csv"
```

## Структура репозитория

```
├── c4-reports.drawio / c4-updated.drawio   # диаграммы
├── docs/reports-architecture.md            # полный дизайн решения
├── docker-compose.yaml                     # Keycloak + фронтенд
├── keycloak/realm-export.json              # realm: роли, пользователи (user_id), клиенты, claim-маппер
├── frontend/                               # React + Tailwind, nginx
│   └── src/components/ReportPage.tsx       # вход, период, Download Report
├── api/                                    # FastAPI, читает витрину
│   └── app/{main,auth,report,config}.py
└── airflow/
    ├── docker-compose.yml                  # Airflow + ClickHouse + PG телеметрии + CRM mock + api
    ├── dags/reporting_mart.py              # extract_telemetry/extract_crm → join → load → quality
    ├── init/{telemetry.sql,schema.sql}     # мок-телеметрия / DDL витрины
    └── mock_crm.py                         # имитация Битрикс24 REST
```

## Ключевые решения

| Вопрос | Выбор |
|---|---|
| Оркестрация ETL | Airflow: расписание `0 2 * * *`, `max_active_runs=1`, инкремент по watermark, идемпотентная перезапись |
| OLAP | ClickHouse, «звезда»: `fact_usage_daily` (ORDER BY `user_id, dt`, `ReplacingMergeTree`) + `dim_users` |
| Актуальность | T+1: витрина содержит только обработанные дни; API честно сообщает границу |
| API | FastAPI + clickhouse-driver; запрос отчёта — два индексируемых SELECT, агрегация уже в витрине |
| Источники | PostgreSQL (телеметрия, read-only), REST CRM; существующий поток «чип → API → PG» не затрагивается |
