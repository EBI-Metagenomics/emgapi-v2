import logging

from ninja.errors import HttpError
from ninja_extra import api_controller, http_post
from ninja_extra.permissions import AllowAny
from ninja_jwt.controller import NinjaJWTSlidingController
from ninja_jwt.tokens import SlidingToken

from emgapiv2.api import ApiSections
from emgapiv2.api.auth import (
    authenticate_webin_user,
    WebinUser,
    WebinTokenRequest,
    WebinTokenResponse,
)

logger = logging.getLogger(__name__)


@api_controller("auth", tags=[ApiSections.AUTH], permissions=[AllowAny])
class WebinJwtController(NinjaJWTSlidingController):
    def get_user(self, username: str, password: str):
        logger.debug(f"Getting user for {username}")
        if authenticate_webin_user(username, password):
            return WebinUser(username=username)
        return None

    @http_post(
        "/sliding",
        response=WebinTokenResponse,
        url_name="token_obtain_sliding",
        operation_id="token_obtain_sliding",
        summary="Obtain an authentication token using Webin credentials.",
        description="Obtain an authentication JWT token using Webin credentials. "
        "This token is sliding, i.e. it can be used both to access private data endpoints "
        "and to refresh itself after expiry.",
    )
    def obtain_token(self, user_token: WebinTokenRequest):
        username = user_token.username
        password = user_token.password

        # Authenticate via external (ENA Webin) API
        if not authenticate_webin_user(username, password):
            raise HttpError(401, "Invalid credentials")

        token = SlidingToken()
        token["username"] = username

        return WebinTokenResponse(token=str(token), token_type="sliding")
