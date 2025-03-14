import logging

from django.conf import settings
from httpx import BasicAuth

EMG_CONFIG = settings.EMG_CONFIG

try:
    emg_webin_auth = BasicAuth(
        username=EMG_CONFIG.webin.emg_webin_account,
        password=EMG_CONFIG.webin.emg_webin_password,
    )
except TypeError:
    logging.warning("ENA Webin auth is not configured")
    emg_webin_auth = None

try:
    dcc_auth = BasicAuth(
        username=EMG_CONFIG.webin.dcc_account, password=EMG_CONFIG.webin.dcc_password
    )
except TypeError:
    logging.warning("ENA DCC auth is not configured")
    dcc_auth = None
