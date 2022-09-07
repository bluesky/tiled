import asyncio
import functools
import logging
import re
import secrets
from collections.abc import Iterable

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


class LDAPAuthenticator:
    """
    The authenticator code is based on https://github.com/jupyterhub/ldapauthenticator
    The parameter ``use_tls`` was added for convenience of testing.

    Parameters
    ----------
    server_address: str or list(str)
        Address(es) of the LDAP server(s) to contact. A string value may represent a single
        server, a list of strings may represent one or more servers. If a server address
        includes port, then the value of ``server_port`` is ignored, otherwise ``server_port``
        or the default port is used to access the server.

        Could be an IP address or hostname.
    server_port: int or None
        Port on which to contact the LDAP server. Default port is used if ``None``.

        Defaults to ``636`` if ``use_ssl`` is set, ``389`` otherwise.
    use_ssl: boolean
        Use SSL to communicate with the LDAP server.

        Deprecated in version 3 of LDAP. Your LDAP server must be configured to support this, however.
    use_tls: boolean
        Enable/disable TLS if ``use_ssl`` is False. By default TLS is enabled. It should not be disabled
        in production systems.

    connect_timeout: float
        Timeout used for connecting to the LDAP server. Default: 5.

    receive_timeout: float
        Timeout used for communication with the LDAP server, e.g. this timeout is used to wait for
        completion of 2FA. For smooth operation it should probably exceed timeout set at LDAP server.
        Default: 60.

    bind_dn_template: list or str
        Template from which to construct the full dn
        when authenticating to LDAP. ``{username}`` is replaced
        with the actual username used to log in.

        If your LDAP is set in such a way that the userdn can not
        be formed from a template, but must be looked up with an attribute
        (such as uid or ``sAMAccountName``), please see ``lookup_dn``. It might
        be particularly relevant for ActiveDirectory installs.

        Unicode Example:

        .. code-block::

            "uid={username},ou=people,dc=wikimedia,dc=org"

        List Example:

        .. code-block::

            [
                "uid={username},ou=people,dc=wikimedia,dc=org",
                "uid={username},ou=Developers,dc=wikimedia,dc=org"
                ]
    allowed_groups: list or None
        List of LDAP group DNs that users could be members of to be granted access.

        If a user is in any one of the listed groups, then that user is granted access.
        Membership is tested by fetching info about each group and looking for the User's
        dn to be a value of one of `member` or `uniqueMember`, *or* if the username being
        used to log in with is value of the `uid`.

        Set to an empty list or None to allow all users that have an LDAP account to log in,
        without performing any group membership checks.
    valid_username_regex: str
        Regex for validating usernames - those that do not match this regex will be rejected.

        This is primarily used as a measure against LDAP injection, which has fatal security
        considerations. The default works for most LDAP installations, but some users might need
        to modify it to fit their custom installs. If you are modifying it, be sure to understand
        the implications of allowing additional characters in usernames and what that means for
        LDAP injection issues. See https://www.owasp.org/index.php/LDAP_injection for an overview
        of LDAP injection.
    lookup_dn: boolean
        Form user's DN by looking up an entry from directory

        By default, LDAPAuthenticator finds the user's DN by using `bind_dn_template`.
        However, in some installations, the user's DN does not contain the username, and
        hence needs to be looked up. You can set this to True and then use ``user_search_base``
        and ``user_attribute`` to accomplish this.
    user_search_base: str
        Base for looking up user accounts in the directory, if `lookup_dn` is set to True.

        LDAPAuthenticator will search all objects matching under this base where the `user_attribute`
        is set to the current username to form the userdn.

        For example, if all users objects existed under the base ou=people,dc=wikimedia,dc=org, and
        the username users use is set with the attribute `uid`, you can use the following config:

        .. code-block::

            c.LDAPAuthenticator.lookup_dn = True
            c.LDAPAuthenticator.lookup_dn_search_filter = '({login_attr}={login})'
            c.LDAPAuthenticator.lookup_dn_search_user = 'ldap_search_user_technical_account'
            c.LDAPAuthenticator.lookup_dn_search_password = 'secret'
            c.LDAPAuthenticator.user_search_base = 'ou=people,dc=wikimedia,dc=org'
            c.LDAPAuthenticator.user_attribute = 'sAMAccountName'
            c.LDAPAuthenticator.lookup_dn_user_dn_attribute = 'cn'
            c.LDAPAuthenticator.bind_dn_template = '{username}'
    user_attribute: str
        Attribute containing user's name, if ``lookup_dn`` is set to True.

        See ``user_search_base`` for info on how this attribute is used.

        For most LDAP servers, this is uid.  For Active Directory, it is
        sAMAccountName.
    lookup_dn_search_filter: str or None
        How to query LDAP for user name lookup, if ``lookup_dn`` is set to True.
    lookup_dn_search_user: str or None
        Technical account for user lookup, if ``lookup_dn`` is set to True.

        If both lookup_dn_search_user and lookup_dn_search_password are None,
        then anonymous LDAP query will be done.
    lookup_dn_search_password: str or None
        Technical account for user lookup, if ``lookup_dn`` is set to True.
    lookup_dn_user_dn_attribute: str or None
        Attribute containing user's name needed for  building DN string, if ``lookup_dn`` is set to True.

        See ``user_search_base`` for info on how this attribute is used.

        For most LDAP servers, this is username.  For Active Directory, it is cn.
    escape_userdn: boolean
        If set to True, escape special chars in userdn when authenticating in LDAP.

        On some LDAP servers, when userdn contains chars like '(', ')', '\'
        authentication may fail when those chars
        are not escaped.
    search_filter: str
        LDAP3 Search Filter whose results are allowed access
    attributes: list or None
        List of attributes to be searched
    auth_state_attributes: list or None
        List of attributes to be returned in auth_state for a user
    use_lookup_dn_username: boolean
        If set to true uses the ``lookup_dn_user_dn_attribute`` attribute as username instead of
        the supplied one.

        This can be useful in an heterogeneous environment, when supplying a UNIX username
        to authenticate against AD.

    Examples
    --------

    Using the authenticator class (the code runs in ``asyncio`` loop):

    .. code-block::

        from bluesky_httpserver.authenticators import LDAPAuthenticator
        authenticator = LDAPAuthenticator(
            "localhost", 1389, bind_dn_template="cn={username},ou=users,dc=example,dc=org", use_tls=False
        )
        await authenticator.authenticate("user01", "password1")
        await authenticator.authenticate("user02", "password2")


    Simple example of a config file (e.g. ``config_ldap.yml``):

    .. code-block::

        uvicorn:
            host: localhost
            port: 60610
        authentication:
            providers:
                - provider: ldap_local
                authenticator: bluesky_httpserver.authenticators:LDAPAuthenticator
                args:
                    server_address: localhost
                    server_port: 1389
                    bind_dn_template: "cn={username},ou=users,dc=example,dc=org"
                    use_tls: false
                    use_ssl: false
            tiled_admins:
                - provider: ldap_local
                id: user02
    """

    mode = Mode.password

    def __init__(
        self,
        server_address,
        server_port=None,
        *,
        use_ssl=False,
        use_tls=True,
        connect_timeout=5,
        receive_timeout=60,
        bind_dn_template=None,
        allowed_groups=None,
        valid_username_regex=r"^[a-z][.a-z0-9_-]*$",
        lookup_dn=False,
        user_search_base=None,
        user_attribute=None,
        lookup_dn_search_filter="({login_attr}={login})",
        lookup_dn_search_user=None,
        lookup_dn_search_password=None,
        lookup_dn_user_dn_attribute=None,
        escape_userdn=False,
        search_filter="",
        attributes=None,
        auth_state_attributes=None,
        use_lookup_dn_username=True,
    ):
        self.use_ssl = use_ssl
        self.use_tls = use_tls
        self.connect_timeout = connect_timeout
        self.receive_timeout = receive_timeout
        self.bind_dn_template = bind_dn_template
        self.allowed_groups = allowed_groups
        self.valid_username_regex = valid_username_regex
        self.lookup_dn = lookup_dn
        self.user_search_base = user_search_base
        self.user_attribute = user_attribute
        self.lookup_dn_search_filter = lookup_dn_search_filter
        self.lookup_dn_search_user = lookup_dn_search_user
        self.lookup_dn_search_password = lookup_dn_search_password
        self.lookup_dn_user_dn_attribute = lookup_dn_user_dn_attribute
        self.escape_userdn = escape_userdn
        self.search_filter = search_filter
        self.attributes = attributes if attributes else []
        self.auth_state_attributes = (
            auth_state_attributes if auth_state_attributes else []
        )
        self.use_lookup_dn_username = use_lookup_dn_username

        if isinstance(server_address, str):
            server_address_list = [server_address]
        elif isinstance(server_address, Iterable):
            server_address_list = list(server_address)
        else:
            raise TypeError(
                f"Unsupported type of `server_address` (list): server_address={server_address} "
                f"type(server_address)={type(server_address)}"
            )
        if not server_address_list:
            raise ValueError(
                "No servers are specified: 'server_address' is an empty list"
            )

        self.server_address_list = server_address_list
        self.server_port = (
            server_port if server_port is not None else self._server_port_default()
        )

    def _server_port_default(self):
        if self.use_ssl:
            return 636  # default SSL port for LDAP
        else:
            return 389  # default plaintext port for LDAP

    async def resolve_username(self, username_supplied_by_user):

        import ldap3

        search_dn = self.lookup_dn_search_user
        if self.escape_userdn:
            search_dn = ldap3.utils.conv.escape_filter_chars(search_dn)
        conn = await asyncio.get_running_loop().run_in_executor(
            None, self.get_connection, search_dn, self.lookup_dn_search_password
        )
        is_bound = await asyncio.get_running_loop().run_in_executor(None, conn.bind)
        if not is_bound:
            msg = "Failed to connect to LDAP server with search user '{search_dn}'"
            self.log.warning(msg.format(search_dn=search_dn))
            return (None, None)

        search_filter = self.lookup_dn_search_filter.format(
            login_attr=self.user_attribute, login=username_supplied_by_user
        )
        msg = "\n".join(
            [
                "Looking up user with:",
                "    search_base = '{search_base}'",
                "    search_filter = '{search_filter}'",
                "    attributes = '{attributes}'",
            ]
        )
        logger.debug(
            msg.format(
                search_base=self.user_search_base,
                search_filter=search_filter,
                attributes=self.user_attribute,
            )
        )

        search_func = functools.partial(
            conn.search,
            search_base=self.user_search_base,
            search_scope=ldap3.SUBTREE,
            search_filter=search_filter,
            attributes=[self.lookup_dn_user_dn_attribute],
        )
        await asyncio.get_running_loop().run_in_executor(None, search_func)

        response = conn.response
        if len(response) == 0 or "attributes" not in response[0].keys():
            msg = (
                "No entry found for user '{username}' "
                "when looking up attribute '{attribute}'"
            )
            logger.warning(
                msg.format(
                    username=username_supplied_by_user, attribute=self.user_attribute
                )
            )
            return (None, None)

        user_dn = response[0]["attributes"][self.lookup_dn_user_dn_attribute]
        if isinstance(user_dn, list):
            if len(user_dn) == 0:
                return (None, None)
            elif len(user_dn) == 1:
                user_dn = user_dn[0]
            else:
                msg = (
                    "A lookup of the username '{username}' returned a list "
                    "of entries for the attribute '{attribute}'. Only the "
                    "first among these ('{first_entry}') was used. The other "
                    "entries ({other_entries}) were ignored."
                )
                logger.warning(
                    msg.format(
                        username=username_supplied_by_user,
                        attribute=self.lookup_dn_user_dn_attribute,
                        first_entry=user_dn[0],
                        other_entries=", ".join(user_dn[1:]),
                    )
                )
                user_dn = user_dn[0]

        return (user_dn, response[0]["dn"])

    def get_connection(self, userdn, password):

        import ldap3

        # NOTE: setting 'acitve=False' essentially disables exclusion of inactive servers from the pool.
        # It probably does not matter if the pool contains only one server, but it could have implications
        # when there are multiple servers in the pool. It is not clear what those implications are.
        # But using the default 'activate=True' results in the thread being blocked indefinitely
        # at the step of creating 'ldap3.Connection' regardless of timeouts in case all the servers are
        # inactive (e.g. the pool has one server and it is unaccessible), which is unacceptable.
        # Further investigation may be needed in the future.
        server_pool = ldap3.ServerPool(None, ldap3.RANDOM, active=False)
        for address in self.server_address_list:
            if re.search(r".+:\d+", address):
                # Port is found in the address
                address_split = address.split(":")
                server_addr = ":".join(address_split[:-1])
                server_port = int(address_split[-1])
            else:
                # Use the default port
                server_addr = address
                server_port = self.server_port

            server = ldap3.Server(
                server_addr,
                port=server_port,
                use_ssl=self.use_ssl,
                connect_timeout=self.connect_timeout,
            )
            server_pool.add(server)

        auto_bind_no_ssl = (
            ldap3.AUTO_BIND_TLS_BEFORE_BIND if self.use_tls else ldap3.AUTO_BIND_NO_TLS
        )
        auto_bind = ldap3.AUTO_BIND_NO_TLS if self.use_ssl else auto_bind_no_ssl
        conn = ldap3.Connection(
            server_pool,
            user=userdn,
            password=password,
            auto_bind=auto_bind,
            receive_timeout=self.receive_timeout,
        )
        return conn

    async def get_user_attributes(self, conn, userdn):
        attrs = {}
        if self.auth_state_attributes:
            search_func = functools.partial(
                conn.search,
                userdn,
                "(objectClass=*)",
                attributes=self.auth_state_attributes,
            )
            found = await asyncio.get_running_loop().run_in_executor(None, search_func)
            if found:
                attrs = conn.entries[0].entry_attributes_as_dict
        return attrs

    async def authenticate(self, username: str, password: str):

        import ldap3

        username_saved = username  # Save the user name passed as a parameter

        # Protect against invalid usernames as well as LDAP injection attacks
        if not re.match(self.valid_username_regex, username):
            logger.warning(
                "username:%s Illegal characters in username, must match regex %s",
                username,
                self.valid_username_regex,
            )
            return None

        # No empty passwords!
        if password is None or password.strip() == "":
            logger.warning("username:%s Login denied for blank password", username)
            return None

        # bind_dn_template should be of type List[str]
        bind_dn_template = self.bind_dn_template
        if isinstance(bind_dn_template, str):
            bind_dn_template = [bind_dn_template]

        # sanity check
        if not self.lookup_dn and not bind_dn_template:
            logger.warning(
                "Login not allowed, please configure 'lookup_dn' or 'bind_dn_template'."
            )
            return None

        if self.lookup_dn:
            username, resolved_dn = await self.resolve_username(username)
            if not username:
                return None
            if str(self.lookup_dn_user_dn_attribute).upper() == "CN":
                # Only escape commas if the lookup attribute is CN
                username = re.subn(r"([^\\]),", r"\1\,", username)[0]
            if not bind_dn_template:
                bind_dn_template = [resolved_dn]

        is_bound = False
        for dn in bind_dn_template:
            if not dn:
                logger.warning("Ignoring blank 'bind_dn_template' entry!")
                continue
            userdn = dn.format(username=username)
            if self.escape_userdn:
                userdn = ldap3.utils.conv.escape_filter_chars(userdn)
            msg = "Attempting to bind {username} with {userdn}"
            logger.debug(msg.format(username=username, userdn=userdn))
            msg = "Status of user bind {username} with {userdn} : {is_bound}"
            try:
                conn = await asyncio.get_running_loop().run_in_executor(
                    None, self.get_connection, userdn, password
                )
            except ldap3.core.exceptions.LDAPBindError as exc:
                is_bound = False
                msg += "\n{exc_type}: {exc_msg}".format(
                    exc_type=exc.__class__.__name__,
                    exc_msg=exc.args[0] if exc.args else "",
                )
            else:
                if conn.bound:
                    is_bound = True
                else:
                    is_bound = await asyncio.get_running_loop().run_in_executor(
                        None, conn.bind
                    )

            msg = msg.format(username=username, userdn=userdn, is_bound=is_bound)
            logger.debug(msg)
            if is_bound:
                break

        if not is_bound:
            msg = "Invalid password for user '{username}'"
            logger.warning(msg.format(username=username))
            return None

        if self.search_filter:
            search_filter = self.search_filter.format(
                userattr=self.user_attribute, username=username
            )

            search_func = functools.partial(
                conn.search,
                search_base=self.user_search_base,
                search_scope=ldap3.SUBTREE,
                search_filter=search_filter,
                attributes=self.attributes,
            )
            await asyncio.get_running_loop().run_in_executor(None, search_func)

            n_users = len(conn.response)
            if n_users == 0:
                msg = "User with '{userattr}={username}' not found in directory"
                logger.warning(
                    msg.format(userattr=self.user_attribute, username=username)
                )
                return None
            if n_users > 1:
                msg = (
                    "Duplicate users found! "
                    "{n_users} users found with '{userattr}={username}'"
                )
                logger.warning(
                    msg.format(
                        userattr=self.user_attribute, username=username, n_users=n_users
                    )
                )
                return None

        if self.allowed_groups:
            logger.debug("username:%s Using dn %s", username, userdn)
            found = False
            for group in self.allowed_groups:
                group_filter = (
                    "(|"
                    "(member={userdn})"
                    "(uniqueMember={userdn})"
                    "(memberUid={uid})"
                    ")"
                )
                group_filter = group_filter.format(userdn=userdn, uid=username)
                group_attributes = ["member", "uniqueMember", "memberUid"]

                search_func = functools.partial(
                    conn.search,
                    group,
                    search_scope=ldap3.BASE,
                    search_filter=group_filter,
                    attributes=group_attributes,
                )
                found = await asyncio.get_running_loop().run_in_executor(
                    None, search_func
                )
                if found:
                    break

            if not found:
                # If we reach here, then none of the groups matched
                msg = "username:{username} User not in any of the allowed groups"
                logger.warning(msg.format(username=username))
                return None

        if not self.use_lookup_dn_username:
            username = username_saved

        user_info = await self.get_user_attributes(conn, userdn)
        if user_info:
            logger.debug("username:%s attributes:%s", username, user_info)
            return {"name": username, "auth_state": user_info}
        return username
