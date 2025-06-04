import logging
import re
from typing import Optional

import httpx
from django.conf import settings
from ninja import Schema
from ninja.security import SessionAuthSuperUser as DjangoSuperUserAuth
from ninja_jwt.authentication import JWTStatelessUserAuthentication

__all__ = [
    "WebinJWTAuth",
    "DjangoSuperUserAuth",
    "authenticate_webin_user",
    "validate_webin_username",
    "WebinTokenRequest",
    "WebinTokenResponse",
    "WebinUser",
]

from ninja_jwt.models import TokenUser


logger = logging.getLogger(__name__)


class WebinTokenRequest(Schema):
    username: str
    password: str


class WebinTokenResponse(Schema):
    token: str
    token_type: str = "sliding"


class WebinUser(TokenUser): ...


class WebinJWTAuth(JWTStatelessUserAuthentication):
    def get_user(self, validated_token):
        username = validated_token.get("username")
        if not username:
            return None
        return WebinUser(validated_token)


def validate_webin_username(username: str) -> bool:
    """
    Validate that a username is a valid Webin username.

    Args:
        username: The username to validate

    Returns:
        True if the username is valid, False otherwise
    """
    if "webin" not in (unl := username.lower()):
        logger.debug(f"No webin in {unl}")
        return False

    # Check if it's a standard Webin ID (e.g., "Webin-12345")
    if re.match(r"^Webin-\d+$", username):
        logger.debug(f"Webin format of {username} matches standard Webin-xxx format")
        return True

    # Check if it's a broker-prefixed username (e.g., "mg-Webin-12345")
    config = settings.EMG_CONFIG.webin
    if username.startswith(config.broker_prefix) and re.match(
        r"^" + re.escape(config.broker_prefix) + r"Webin-\d+$", username
    ):
        logger.debug(f"Webin format of {username} matches broker-prefixed format")
        return True

    logger.debug(f"Webin format of {username} doesn't match any valid format")
    return False


def authenticate_webin_user(username: str, password: str) -> Optional[str]:
    """
    Authenticate a Webin user against the ENA authentication endpoint.

    Args:
        username: The Webin username
        password: The Webin password

    Returns:
        The Webin ID if authentication is successful, None otherwise
    """
    if not validate_webin_username(username):
        logger.debug(
            f"Username {username} is not a valid Webin username. Not authenticating."
        )
        return None

    config = settings.EMG_CONFIG.webin

    data = {
        "authRealms": ["ENA", "EGA"],
        "username": username,
        "password": password,
    }

    try:
        logger.debug(f"Authenticating {username} via {config.auth_endpoint}")
        response = httpx.post(str(config.auth_endpoint), json=data)

        if response.status_code == 200:
            # Extract the Webin ID from the username
            # If it's a broker username, remove the prefix
            if username.startswith(config.broker_prefix):
                webin_id = username[len(config.broker_prefix) :]
            else:
                webin_id = username

            return webin_id

        return None
    except httpx.RequestError:
        return None
