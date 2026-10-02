"""Authentication module for the API."""

import logging

from fastapi_auth0 import Auth0, Auth0User
from jose import jwt

from airweave.core.config import AuthMode, settings


# Add a method to auth0 instance to verify tokens directly
async def get_user_from_token(token: str):
    """Verify a token and return the Auth0User.

    Args:
        token: The JWT token to verify.

    Returns:
        Auth0User if token is valid, None otherwise
    """
    # If auth is disabled, just return a mock user
    if settings.AUTH_MODE == AuthMode.LOCAL:
        return Auth0User(sub="mock-user-id", email=settings.FIRST_SUPERUSER)

    if settings.AUTH_MODE != AuthMode.AUTH0:
        return None

    try:
        if not token:
            return None

        # Validate the token using the same logic as in the auth0.get_user method
        unverified_header = jwt.get_unverified_header(token)
        rsa_key = {}

        # Find the correct key
        for key in auth0.jwks["keys"]:
            if key["kid"] == unverified_header["kid"]:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"],
                }
                break

        if not rsa_key:
            logging.warning("Invalid kid header (wrong tenant or rotated public key)")
            return None

        # Decode and verify the token
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=auth0.algorithms,
            audience=auth0.audience,
            issuer=f"https://{auth0.domain}/",
        )

        # Create an Auth0User from the payload
        return auth0.auth0_user_model(**payload)
    except Exception as e:
        logging.error(f"Error verifying token: {e}")
        return None


# Initialize Auth0 only if authentication is enabled
if settings.AUTH_MODE == AuthMode.AUTH0:
    auth0 = Auth0(
        domain=settings.AUTH0_DOMAIN,
        api_audience=settings.AUTH0_AUDIENCE,
        auto_error=False,
    )
else:
    # Create a mock Auth0 instance that doesn't make network calls
    class InactiveAuth0:
        """Dependency for modes without Auth0; only explicit local mode returns a demo user."""

        def __init__(self):
            """Initialize a network-free Auth0 dependency."""
            self.domain = "mock-domain.auth0.com"
            self.audience = "https://mock-api/"
            self.algorithms = ["RS256"]
            self.jwks = {"keys": []}
            self.auth0_user_model = Auth0User

        async def get_user(self):
            """Return a demo user only when insecure local mode is explicit."""
            # For development and testing
            if settings.AUTH_MODE == AuthMode.LOCAL:
                return Auth0User(sub="mock-user-id", email=settings.FIRST_SUPERUSER)
            return None

    auth0 = InactiveAuth0()
    logging.info("Auth0 disabled for authentication mode %s", settings.AUTH_MODE.value)
