import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "retailpulse_secret_key_2024")
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://retailpulse:retailpulse123@postgres/retailpulse"
)
WTF_CSRF_ENABLED = True
FEATURE_FLAGS = {"ALERT_REPORTS": True, "DASHBOARD_NATIVE_FILTERS": True}
ENABLE_PROXY_FIX = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False
