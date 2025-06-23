from pathlib import Path

from django.conf import settings

import hmac
import hashlib
from urllib.parse import urlencode, urljoin
from time import time


class SecureStorage:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def generate_secure_link(self, path: str | Path, expiry_seconds: int = 3600) -> str:
        secret = settings.SECURE_LINK_SECRET_KEY

        _path = str(path)
        expires = str(int(time()) + expiry_seconds)
        message = _path + expires
        signature = hmac.new(
            secret.encode(), message.encode(), hashlib.sha256
        ).hexdigest()
        query = urlencode({"expires": expires, "token": signature})
        return urljoin(self.base_url, _path) + "?" + query


private_storage = SecureStorage(settings.EMG_CONFIG.service_urls.private_data_url_root)
