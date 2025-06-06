import pytest

from ninja_jwt.tokens import SlidingToken

from emgapiv2.api.auth import (
    WebinJWTAuth,
    authenticate_webin_user,
    validate_webin_username,
)
from emgapiv2.config import EMGConfig


@pytest.fixture
def webin_auth():
    return WebinJWTAuth()


@pytest.fixture
def auth_token(webin_private_study):
    # Generate a token for the webin user
    token = SlidingToken()
    token["username"] = webin_private_study.webin_submitter
    return str(token)


@pytest.mark.django_db
def test_validate_webin_username():
    # Test valid usernames
    assert validate_webin_username("Webin-12345") is True

    # Test with broker prefix
    config = EMGConfig().webin
    assert validate_webin_username(f"{config.broker_prefix}Webin-12345") is True

    # Test invalid usernames
    assert validate_webin_username("user123") is False
    assert validate_webin_username("mg-user123") is False


@pytest.mark.django_db
def test_authenticate_webin_user(httpx_mock):
    httpx_mock.add_response(
        url="http://fake-auth.example.com/auth",
        status_code=200,
    )
    # Mock the response from the authentication endpoint
    # Test successful authentication
    webin_id = authenticate_webin_user("Webin-12345", "password")
    assert webin_id == "Webin-12345"

    httpx_mock.add_response(
        url="http://fake-auth.example.com/auth",
        status_code=200,
    )
    # Test with broker prefix
    config = EMGConfig().webin
    webin_id = authenticate_webin_user(f"{config.broker_prefix}Webin-12345", "password")
    assert webin_id == "Webin-12345"

    httpx_mock.add_response(
        url="http://fake-auth.example.com/auth",
        status_code=401,
    )
    # Test failed authentication
    webin_id = authenticate_webin_user("Webin-12345", "wrong-password")
    assert webin_id is None

    # Test invalid username
    webin_id = authenticate_webin_user("user123", "password")
    assert webin_id is None


@pytest.mark.django_db
def test_token_endpoint(ninja_api_client, webin_private_study, httpx_mock):
    httpx_mock.add_response(
        url="http://fake-auth.example.com/auth",
        status_code=200,
    )

    # Test successful token generation
    response = ninja_api_client.post(
        "/auth/sliding",
        json={"username": webin_private_study.webin_submitter, "password": "password"},
    )
    assert response.status_code == 200
    data = response.json()
    assert "token" in data
    assert data["token_type"] == "sliding"

    # Verify the token
    token = data["token"]
    validated_token = SlidingToken(token)
    assert validated_token.get("username") == webin_private_study.webin_submitter

    # Test refresh
    response = ninja_api_client.post(
        "/auth/sliding/refresh",
        json={"token": token},
    )
    assert response.status_code == 200
    data = response.json()
    assert "token" in data
    validated_token = SlidingToken(token)
    assert validated_token.get("username") == webin_private_study.webin_submitter

    # Test failed authentication
    httpx_mock.add_response(
        url="http://fake-auth.example.com/auth",
        status_code=401,
    )
    response = ninja_api_client.post(
        "/auth/sliding",
        json={
            "username": webin_private_study.webin_submitter,
            "password": "wrong-password",
        },
    )
    assert response.status_code == 401


@pytest.mark.django_db
def test_private_endpoints_with_superuser(ninja_api_client, admin_user, user):

    # Test access to private endpoints
    response = ninja_api_client.get("/my-data/studies/", user=admin_user)
    assert response.status_code == 200

    response = ninja_api_client.get("/my-data/analyses/", user=admin_user)
    assert response.status_code == 200

    # Test access to private endpoints
    response = ninja_api_client.get("/my-data/studies/", user=user)
    assert response.status_code == 401

    response = ninja_api_client.get("/my-data/analyses/", user=user)
    assert response.status_code == 401


@pytest.mark.django_db
def test_private_endpoints_with_webin_token(
    ninja_api_client, webin_private_study, auth_token
):
    # Create a study for the webin user

    # Authenticate with token
    headers = {"Authorization": f"Bearer {auth_token}"}

    # Test access to private endpoints
    response = ninja_api_client.get("/my-data/studies/", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["items"][0]["accession"] == webin_private_study.accession

    # Test with invalid token
    headers = {"Authorization": "Bearer invalid-token"}
    response = ninja_api_client.get("/my-data/studies/", headers=headers)
    assert response.status_code == 401
