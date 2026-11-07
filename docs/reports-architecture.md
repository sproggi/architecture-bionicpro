# Архитектура решения: подготовка и получение отчётов (BionicPRO)

Диаграмма: [`c4-reports.drawio`](../c4-reports.drawio) (сплошные линии — новый контур отчётности, пунктир — существующие потоки).

## 1. Цель

Дать пилоту/покупателю протеза личный отчёт об использовании протеза, объединяющий:

- **данные с датчиков** (телеметрия чипа: время использования, сессии, ошибки, заряд батареи);
- **данные CRM** (клиент, протез, дата/условия покупки).

Отчёт **предварительно** формируется ETL-процессом (Apache Airflow) в аналитической витрине (OLAP-БД) и выдаётся пользователю через **бэкенд-сервис API**, обозначенный на исходной архитектуре.

## 2. Ключевые решения

| Вопрос | Выбор | Обоснование |
|---|---|---|
| Оркестрация ETL | Apache Airflow | расписание, ретраи, идемпотентность, UI мониторинга, аудит запусков (требование задачи) |
| OLAP-БД витрины | ClickHouse | колоночное хранилище, быстрые агрегации по миллионам строк телеметрии, компактное хранение |
| Бэкенд API | Python | сервис API уже обозначен на исходной архитектуре («e.g. SpringBoot») |
| Источник CRM | REST API Битрикс24 | нc4-updated.drawioе нагружает БД CRM (Oracle) напрямую, официальный интерфейс |
| Источник телеметрии | PostgreSQL (текущая БД телеметрии) | существующий поток: чип → API → БД; ETL читает её инкрементально |
| Аутентификация | Keycloak (существующий, realm `reports-realm`) | фронтенд уже настроен на PKCE S256 |
| Модель данных | витрина «звезда», T+1 (предвычисление) | запрос отчёта — лёгкая выборка, а не тяжёлая агрегация на лету |

## 3. Компоненты

| Компонент | Роль |
|---|---|
| Web-приложение отчётов (React, nginx) | существующий `reports-frontend`: вход через Keycloak, кнопка «Download Report», скачивание JSON/PDF |
| Keycloak | выпускает JWT (OIDC, PKCE S256); отдаёт JWKS для валидации на API |
| **API отчётов (Python)** | бэкенд-сервис API с исходной архитектуры; валидирует JWT, берёт `user_id` из токена, выполняет запрос к витрине, возвращает отчёт |
| **Apache Airflow** (Webserver, Scheduler, Workers, мета-БД PostgreSQL) | ETL-оркестрация: DAG `reporting_mart` по расписанию |
| **Витрина отчётности (ClickHouse)** | готовая OLAP-витрина: `fact_usage_daily`, `dim_users` |
| PostgreSQL (телеметрия) | исходник: сырые данные датчиков (существующий поток) |
| CRM Битрикс24 | исходник: клиенты, сделки, протезы (существующая система) |

## 4. ETL-процесс (Apache Airflow)

DAG `reporting_mart`: `schedule="0 2 * * *"`, `catchup=False`, `max_active_runs=1` (идемпотентность, нет параллельных перезаписей).

```
extract_telemetry ─┐
                   ├─> transform_join ─> load_olap ─> quality_checks
extract_crm ───────┘
```

1. **`extract_telemetry`** — инкрементальная выгрузка из PostgreSQL:
   `SELECT ... FROM telemetry WHERE ts > :watermark`. Водяной знак (watermark) хранится в Airflow Variable; чтение только read-only учёткой.
2. **`extract_crm`** — выгрузка из Битрикс24 REST API: `/crm/deal/list`, `/crm/contact/list` + поля товара/протеза; постраничная выгрузка, обновление по `modified`.
3. **`transform_join`** — нормализация и объединение по `user_id` / `prosthesis_id`; агрегация до уровня «пользователь × день»: часы использования, число сессий, число ошибок, min/max заряда батареи; привязка данных CRM (модель, дата покупки).
4. **`load_olap`** — загрузка в ClickHouse: `DROP PARTITION` + `INSERT` за период запуска (идемпотентность: повторный запуск за ту же дату не дублирует данные) либо `ReplacingMergeTree` по `updated_at`.
5. **`quality_checks`** — контрольные проверки: количество строк, покрытие пользователей, доля NULL; при провале — DAG в состоянии failed + алерт (email/Telegram).

Повторные прогоны — через Clear/Backfill по датам; идемпотентность на уровне партиций.

## 5. Витрина отчётности (ClickHouse)

```sql
CREATE DATABASE reports;

CREATE TABLE reports.dim_users (
    user_id        String,
    user_name      String,
    email          String,
    prosthesis_id  String,
    model          String,
    purchase_date  Date,
    updated_at     DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

CREATE TABLE reports.fact_usage_daily (
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
```

Разумно добавить материализованные агрегаты (месячные суммы) — `MaterializedView` — если потребуется отчёт за большие периоды.

## 6. Получение отчёта через API

**Эндпоинт:**

```
GET /api/v1/reports/usage?from=2026-10-01&to=2026-10-31&format=json|pdf
```

**Безопасность (row-level):**

- проверка `Authorization: Bearer <JWT>`: RS256, ключи из JWKS Keycloak (кэш JWKS);
- `user_id` берётся **только из claim токена** (`sub` или кастомный claim) — не из параметров запроса: пользователь физически не может запросить чужой отчёт;
- права: realm-роль, дающая доступ к отчётам (например `prothetic_user` из существующего realm).

**Логика:**

```sql
SELECT dt, usage_hours, sessions, errors, battery_min, battery_max
FROM reports.fact_usage_daily FINAL
WHERE user_id = ? AND dt BETWEEN ? AND ?
ORDER BY dt
```

**Ответы:** `200` — JSON (ряд «день → метрики») или PDF (генерация iText/OpenPDF); `401` — нет/невалидный токен; `404` — нет данных за период.

Опционально: кэш (Redis) по ключу `(user_id, from, to)` на время актуальности витрины (до следующего ETL-запуска).

## 7. Сценарии

**A. Подготовка (ежедневно, ~02:00):**
Airflow → (read-only) PostgreSQL — телеметрия; Airflow → REST API Битрикс24 — данные CRM; transform (join по user/prosthesis, агрегация по дням); load → ClickHouse (идемпотентно); quality checks; алерт при сбое.

**B. Получение отчёта (по запросу пользователя):**
Пользователь → фронтенд (браузер) → Keycloak (OIDC + PKCE S256, выдача JWT) → `GET /reports` с Bearer → API валидирует JWT (JWKS) → берёт `user_id` из токена → запрос к ClickHouse → JSON/PDF → скачивание в браузере.

## 8. Безопасность и данные

- JWT RS256 + JWKS; доступ к отчётам — только по роли; изоляция данных на уровне строки (user_id из claim).
- Airflow живёт во внутренней сети; к PostgreSQL — read-only учётка; к CRM — отдельный API-токен с минимальными правами.
- В витрину попадают только нужные атрибуты (PII-минимизация); политика хранения — N лет с партиционированием по месяцам.

## 9. Нефункциональные требования

| Параметр | Значение |
|---|---|
| Актуальность данных | T+1 (запуск 02:00); при необходимости — почасовой schedule |
| Хранение витрины | N лет, партиции по месяцам, `ReplacingMergeTree` |
| Наблюдаемость | Airflow: статусы DAG, алерты на failure; quality checks; метрики времени загрузов |
| Масштабирование | горизонтальное масштабирование Airflow workers; ClickHouse — кластеризация при росте объёма телеметрии |

## 10. Разграничение с исходной архитектурой

- Существующий поток «чип → API → PostgreSQL» **не меняется** — ETL лишь читает результат.
- Существующий авторизационный контур (BFF/Country Router/IdP для мобильного приложения) **не затрагивается**; приложение отчётов использует уже реализованный в репозитории паттерн «фронтенд + Keycloak (PKCE S256)».
- Новый сервис API отчётов — это тот же бэкенд-сервис API, что на исходной диаграмме; расширяется эндпоинтом `/reports` и подключением к ClickHouse.
