from django.conf import settings
from httpx import BasicAuth

EMG_CONFIG = settings.EMG_CONFIG

emg_webin_auth = BasicAuth(
    username=EMG_CONFIG.webin.emg_webin_account,
    password=EMG_CONFIG.webin.emg_webin_password,
)
dcc_auth = BasicAuth(
    username=EMG_CONFIG.webin.dcc_account, password=EMG_CONFIG.webin.dcc_password
)
