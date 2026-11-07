import os

CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CH_PORT", "9000"))
CH_USER = os.environ.get("CH_USER", "reports")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "reports_password")
CH_DB = os.environ.get("CH_DB", "reports")

KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL", "").rstrip("/")
KEYCLOAK_REALM = os.environ.get("KEYCLOAK_REALM", "reports-realm")
REQUIRE_AUTH = os.environ.get("REQUIRE_AUTH", "true").lower() == "true"
REQUIRED_ROLE = os.environ.get("REQUIRED_ROLE", "prothetic_user")

MAX_PERIOD_DAYS = int(os.environ.get("MAX_PERIOD_DAYS", "366"))
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "http://localhost:3000")
