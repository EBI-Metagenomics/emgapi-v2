"""Token creation for authenticated Webin accounts."""

from django.conf import settings
from ninja_jwt.tokens import SlidingToken


def create_webin_token(webin_id: str) -> SlidingToken:
    """Create a token for an authenticated Webin account."""
    token = SlidingToken()
    token["username"] = webin_id
    admin_account = settings.EMG_CONFIG.webin.emg_webin_account
    if admin_account and webin_id.casefold() == admin_account.casefold():
        token["is_admin"] = True
    return token
