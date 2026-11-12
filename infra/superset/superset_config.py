"""Local Superset deployment; PostgreSQL stores dashboards and credentials."""
import os
from urllib.parse import quote_plus

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:"
    + quote_plus(os.environ["SUPERSET_DB_PASSWORD"])
    + "@superset-db:5432/superset"
)
SQLALCHEMY_TRACK_MODIFICATIONS = False
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False  # Local HTTP on 127.0.0.1 only.
