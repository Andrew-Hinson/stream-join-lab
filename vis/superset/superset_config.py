from flask import Flask
from flask_login import current_user, login_user

SECRET_KEY = "stream-join-lab-not-secret"
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
AUTH_ROLE_PUBLIC = "Admin"
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
WEBSERVER_TIMEOUT = 120
SQLLAB_TIMEOUT = 120
SUPERSET_WEBSERVER_TIMEOUT = 120
CONTENT_SECURITY_POLICY_WARNING = False


def FLASK_APP_MUTATOR(app: Flask) -> None:
    @app.before_request
    def _anon_admin() -> None:
        if current_user.is_authenticated:
            return
        from superset import security_manager

        user = security_manager.find_user(username="admin")
        if user is not None:
            login_user(user)

