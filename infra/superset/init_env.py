"""Append missing local Superset secrets to .env, preserving existing settings."""
import secrets
from pathlib import Path

path = Path(".env")
content = path.read_text() if path.exists() else ""
keys = {line.split("=", 1)[0].strip() for line in content.splitlines() if "=" in line}
missing = [
    f"{key}={secrets.token_urlsafe(42)}"
    for key in ("SUPERSET_SECRET_KEY", "SUPERSET_DB_PASSWORD", "SUPERSET_ADMIN_PASSWORD")
    if key not in keys
]
if missing:
    with path.open("a") as stream:
        stream.write(("\n" if content and not content.endswith("\n") else "") + "\n".join(missing) + "\n")
    path.chmod(0o600)
    print("Secrets Superset ajoutés dans .env (valeurs existantes conservées).")
