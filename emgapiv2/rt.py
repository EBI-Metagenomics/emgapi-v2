from django.conf import settings
from rt import rest2

rt_client = rest2.Rt(url=settings.EMG_CONFIG.rt.url, token=settings.EMG_CONFIG.rt.token)
rt_config = settings.EMG_CONFIG.rt
