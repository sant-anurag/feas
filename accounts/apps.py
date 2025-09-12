# accounts/apps.py
from django.apps import AppConfig

class AccountsConfig(AppConfig):
    name = 'accounts'

    def ready(self):
        # register signals
        try:
            import accounts.signals  # noqa: F401
        except Exception:
            # don't break startup if signals fail
            pass
