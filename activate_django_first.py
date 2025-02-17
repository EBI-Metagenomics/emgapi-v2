import django

django.setup()

from django.conf import settings

__all__ = ["EMG_CONFIG"]

EMG_CONFIG = settings.EMG_CONFIG
