"""Create the initial administrator without resetting an existing account."""
import os
from superset.app import create_app
from superset import security_manager

app = create_app()
with app.app_context():
    username = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
    if not security_manager.find_user(username=username):
        user = security_manager.add_user(
            username=username,
            first_name="StreamLoom",
            last_name="Admin",
            email="admin@streamloom.local",
            role=security_manager.find_role("Admin"),
            password=os.environ["SUPERSET_ADMIN_PASSWORD"],
        )
        if not user:
            raise RuntimeError("Impossible de créer le compte administrateur Superset")
