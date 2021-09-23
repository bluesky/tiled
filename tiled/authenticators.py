import logging
import secrets

from jose import jwt, jwk, JWTError

from .utils import modules_available


logger = logging.getLogger(__name__)


class DummyAuthenticator:
    """
    For test and demo purposes only!

    Accept any username and any password.

    """

    handles_credentials = True

    async def authenticate(self, username: str, password: str):
        return username


class DictionaryAuthenticator:
    """
    For test and demo purposes only!

    Check passwords from a dictionary of usernames mapped to passwords.
    """

    handles_credentials = True

    def __init__(self, users_to_passwords):
        self._users_to_passwords = users_to_passwords

    async def authenticate(self, username: str, password: str):
        true_password = self._users_to_passwords.get(username)
        if not true_password:
            # Username is not valid.
            return
        if secrets.compare_digest(true_password, password):
            return username


class PAMAuthenticator:
    handles_credentials = True

    def __init__(self, service="login"):
        if not modules_available("pamela"):
            raise ModuleNotFoundError(
                "This PAMAuthenticator requires the module 'pamela' to be installed."
            )
        # TODO Try to open a PAM session.
        self.service = service

    async def authenticate(self, username: str, password: str):
        import pamela

        try:
            pamela.authenticate(username, password, service=self.service)
        except pamela.PAMError:
            # Authentication failed.
            return
        else:
            return username


class OIDCAuthenticator:
    handles_credentials = False
    configuration_schema = """
client_id:
  type: string
client_secret:
  type: string
redirect_uri:
  type: string
public_keys:
  type: array
  item:
    type: object
    properties:
      - alg:
          type: string
      - e
          type: string
      - kid
          type: string
      - kty
          type: string
      - n
          type: string
      - use
          type: string
    required:
      - alg
      - e
      - kid
      - kty
      - n
      - use
"""

    def __init__(
        self,
        client_id,
        client_secret,
        redirect_uri,
        public_keys,
        token_uri,
        authorization_endpoint,
        confirmation_message,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.confirmation_message = confirmation_message
        self.redirect_uri = redirect_uri
        self.public_keys = public_keys
        self.token_uri = token_uri
        self.authorization_endpoint = authorization_endpoint.format(
            client_id=client_id, redirect_uri=redirect_uri
        )

    async def authenticate(self, request):
        code = request.query_params["code"]
        response = await exchange_code(
            self.token_uri, code, self.client_id, self.client_secret, self.redirect_uri
        )
        response_body = response.json()
        if response.is_error:
            logger.error("Authentication error: %r", response_body)
            return None
        response_body = response.json()
        id_token = response_body["id_token"]
        access_token = response_body["access_token"]
        # Match the kid in id_token to a key in the list of public_keys.
        key = find_key(id_token, self.public_keys)
        try:
            verified_body = jwt.decode(
                id_token,
                key,
                access_token=access_token,
                audience=self.client_id,
            )
        except JWTError:
            logger.exception(
                "Authentication error. Unverified token: %r",
                jwt.get_unverified_claims(id_token),
            )
            return None
        return verified_body["sub"]


class KeyNotFoundError(Exception):
    pass


def find_key(token, keys):
    """
    Find a key from the configured keys based on the kid claim of the token

    Parameters
    ----------
    token : token to search for the kid from
    keys:  list of keys

    Raises
    ------
    KeyNotFoundError:
        returned if the token does not have a kid claim

    Returns
    ------
    key: found key object
    """

    unverified = jwt.get_unverified_header(token)
    kid = unverified.get("kid")
    if not kid:
        raise KeyNotFoundError("No 'kid' in token")

    for key in keys:
        if key["kid"] == kid:
            return jwk.construct(key)
    return KeyNotFoundError(
        f"Token specifies {kid} but we have {[k['kid'] for k in keys]}"
    )


async def exchange_code(token_uri, auth_code, client_id, client_secret, redirect_uri):
    """Method that talks to an IdP to exchange a code for an access_token and/or id_token
    Args:
        token_url ([type]): [description]
        auth_code ([type]): [description]
    """
    if not modules_available("httpx"):
        raise ModuleNotFoundError(
            "This authenticator requires 'httpx'. (pip install httpx)"
        )
    import httpx

    response = httpx.post(
        url=token_uri,
        data={
            "grant_type": "authorization_code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "code": auth_code,
            "client_secret": client_secret,
        },
    )
    return response
