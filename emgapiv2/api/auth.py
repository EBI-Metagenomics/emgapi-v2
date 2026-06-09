import logging
import re
from json import JSONDecodeError
from typing import Optional

import httpx
from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from django.http import HttpRequest
from ninja import Schema
from ninja.security import SessionAuthSuperUser
from ninja.security.base import AuthBase
from ninja_jwt.authentication import JWTStatelessUserAuthentication
from pydantic import BaseModel, ConfigDict, Field, computed_field

__all__ = [
    "WebinJWTAuth",
    "DjangoSuperUserAuth",
    "authenticate_webin_user",
    "validate_webin_username",
    "get_webin_account_details",
    "get_webin_account_details_via_broker",
    "WebinAccountDetails",
    "WebinTokenRequest",
    "WebinTokenResponse",
    "WebinUser",
    "WebinTokenRefreshRequest",
]

DjangoSuperUserAuth = SessionAuthSuperUser

from ninja_jwt.models import TokenUser
from ninja_jwt.schema import TokenRefreshSlidingInputSchema

logger = logging.getLogger(__name__)


class WebinTokenRequest(Schema):
    username: str
    password: str


class WebinTokenRefreshRequest(TokenRefreshSlidingInputSchema):
    token: str


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


class WebinAccountDetails(BaseModel):
    email_address: str = Field(alias="emailAddress")
    first_name: Optional[str] = Field(default=None, alias="firstName")
    surname: Optional[str] = None
    main_contact: bool = Field(alias="mainContact")
    consortium: Optional[str] = None

    model_config = ConfigDict(extra="ignore")

    @computed_field
    @property
    def requester_name(self) -> str:
        return (
            f"{self.first_name} {self.surname}".strip() or self.consortium or "Unknown"
        )


def get_webin_account_details(
    username: str, password: str
) -> list[WebinAccountDetails] | None:
    """
    Get account details (submission account contact details) for a Webin user, from ENA.
    Requires two steps: authenticate and fetch a Webin token, and then use token to fetch account details.
    :param username: e.g. Webin-1
    :param password: Webin password
    :return:
    """
    config = settings.EMG_CONFIG.webin

    data = {
        "authRealms": ["ENA", "EGA"],
        "username": username,
        "password": password,
    }
    try:
        token_response = httpx.post(str(config.token_endpoint), json=data)
        if not token_response.status_code == 200:
            logger.error(f"Error fetching token for {username}: {token_response.text}")
            return None

        token = token_response.text
        accounts_response = httpx.get(
            str(config.account_details_endpoint),
            headers={"Authorization": f"Bearer {token}"},
        )
        if not accounts_response.status_code == 200:
            logger.error(
                f"Error fetching account details for {username}: {accounts_response.text}"
            )
            return None

        accounts = accounts_response.json()
        submission_contacts = accounts.get("submissionContacts")
    except (httpx.RequestError, JSONDecodeError) as e:
        logger.error(f"Error fetching account details: {e}")
        return None

    return [
        WebinAccountDetails.model_validate(contact) for contact in submission_contacts
    ]


def get_webin_account_details_via_broker(
    username: str,
) -> list[WebinAccountDetails] | None:
    """
    Wrapper on get_webin_account_details, to use a broker account prefix/password to get webin details.
    :param username: E.g. Webin-1
    """
    config = settings.EMG_CONFIG.webin
    broker_prefix = config.broker_prefix
    return get_webin_account_details(
        f"{broker_prefix}{username}", config.broker_password
    )


class NoAuth(AuthBase):
    """
    This auth class is for public/private endpoints where an unauth'd user should get
    """

    openapi_type = "none"

    def __call__(self, request: HttpRequest, **kwargs):
        return self.authenticate(request, **kwargs)

    def authenticate(self, request: HttpRequest, *args, **kwargs):
        return AnonymousUser()
