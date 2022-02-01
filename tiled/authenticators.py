import logging
import secrets

from fastapi import APIRouter, Request
from jose import JWTError, jwk, jwt
from starlette.responses import RedirectResponse

from .server.authentication import Mode
from .utils import modules_available

logger = logging.getLogger(__name__)


class DummyAuthenticator:
    """
    For test and demo purposes only!

    Accept any username and any password.

    """

    mode = Mode.password

    async def authenticate(self, username: str, password: str):
        return username


class DictionaryAuthenticator:
    """
    For test and demo purposes only!

    Check passwords from a dictionary of usernames mapped to passwords.
    """

    mode = Mode.password
    configuration_schema = """
$schema": http://json-schema.org/draft-07/schema#
type: object
additionalProperties: false
properties:
  users_to_password:
    type: object
  description: |
    Mapping usernames to password. Environment variable expansion should be
    used to avoid placing passwords directly in configuration.
"""

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

    mode = Mode.password
    configuration_schema = """
$schema": http://json-schema.org/draft-07/schema#
type: object
additionalProperties: false
properties:
  service:
    type: string
    description: PAM service. Default is 'login'.
"""

    def __init__(self, service="login"):
        if not modules_available("pamela"):
            raise ModuleNotFoundError(
                "This PAMAuthenticator requires the module 'pamela' to be installed."
            )
        self.service = service
        # TODO Try to open a PAM session.

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

    mode = Mode.external
    configuration_schema = """
$schema": http://json-schema.org/draft-07/schema#
type: object
additionalProperties: false
properties:
  client_id:
    type: string
  client_secret:
    type: string
  redirect_uri:
    type: string
  token_uri:
    type: string
  authorization_endpoint:
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
                id_token, key, access_token=access_token, audience=self.client_id
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


class SAMLAuthenticator:

    mode = Mode.external

    def __init__(
        self,
        saml_settings,  # See EXAMPLE_SAML_SETTINGS below.
        attribute_name,  # which SAML attribute to use as 'id' for Idenity
        confirmation_message=None,
    ):
        self.saml_settings = saml_settings
        self.attribute_name = attribute_name
        self.confirmation_message = confirmation_message
        self.authorization_endpoint = "/login"

        router = APIRouter()

        if not modules_available("onelogin"):
            # The PyPI package name is 'python3-saml'
            # but it imports as 'onelogin'.
            # https://github.com/onelogin/python3-saml
            raise ModuleNotFoundError(
                "This SAMLAuthenticator requires 'python3-saml' to be installed."
            )

        from onelogin.saml2.auth import OneLogin_Saml2_Auth

        @router.get("/login")
        async def saml_login(request: Request):
            req = await prepare_saml_from_fastapi_request(request)
            auth = OneLogin_Saml2_Auth(req, self.saml_settings)
            # saml_settings = auth.get_settings()
            # metadata = saml_settings.get_sp_metadata()
            # errors = saml_settings.validate_metadata(metadata)
            # if len(errors) == 0:
            #   print(metadata)
            # else:
            #   print("Error found on Metadata: %s" % (', '.join(errors)))
            callback_url = auth.login()
            response = RedirectResponse(url=callback_url)
            return response

        self.include_routers = [router]

    async def authenticate(self, request):
        if not modules_available("onelogin"):
            raise ModuleNotFoundError(
                "This SAMLAuthenticator requires the module 'oneline' to be installed."
            )
        from onelogin.saml2.auth import OneLogin_Saml2_Auth

        req = await prepare_saml_from_fastapi_request(request, True)
        auth = OneLogin_Saml2_Auth(req, self.saml_settings)
        auth.process_response()  # Process IdP response
        errors = auth.get_errors()  # This method receives an array with the errors
        if errors:
            raise Exception(
                "Error when processing SAML Response: %s %s"
                % (", ".join(errors), auth.get_last_error_reason())
            )
        if auth.is_authenticated():
            # Return a string that the Identity can use as id.
            attribute_as_list = auth.get_attributes()[self.attribute_name]
            # Confused in what situation this would have more than one item....
            assert len(attribute_as_list) == 1
            return attribute_as_list[0]
        else:
            return None


async def prepare_saml_from_fastapi_request(request, debug=False):
    form_data = await request.form()
    rv = {
        "http_host": request.client.host,
        "server_port": request.url.port,
        "script_name": request.url.path,
        "post_data": {},
        "get_data": {}
        # Advanced request options
        # "https": "",
        # "request_uri": "",
        # "query_string": "",
        # "validate_signature_from_qs": False,
        # "lowercase_urlencoding": False
    }
    if request.query_params:
        rv["get_data"] = (request.query_params,)
    if "SAMLResponse" in form_data:
        SAMLResponse = form_data["SAMLResponse"]
        rv["post_data"]["SAMLResponse"] = SAMLResponse
    if "RelayState" in form_data:
        RelayState = form_data["RelayState"]
        rv["post_data"]["RelayState"] = RelayState
    return rv
