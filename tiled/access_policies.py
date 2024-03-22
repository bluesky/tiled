from .queries import KeysFilter
from .scopes import SCOPES
from .utils import Sentinel, SpecialUsers, import_object

ALL_ACCESS = Sentinel("ALL_ACCESS")
ALL_SCOPES = set(SCOPES)
PUBLIC_SCOPES = {"read:metadata", "read:data"}
NO_ACCESS = Sentinel("NO_ACCESS")


class DummyAccessPolicy:
    "Impose no access restrictions."

    def allowed_scopes(self, node, principal):
        return ALL_SCOPES

    def filters(self, node, principal, scopes):
        return []


class SimpleAccessPolicy:
    """
    A mapping of user names to lists of entries they have access to.

    This simple policy does not provide fine-grained control of scopes.
    Any restriction on scopes is applied the same to all users, except
    for an optional list of 'admins'.

    This is used in the test suite; it may be suitable for very simple
    deployments.

    >>> SimpleAccessPolicy({"alice": ["A", "B"], "bob": ["B"]}, provider="toy")
    """

    ALL = ALL_ACCESS

    def __init__(
        self, access_lists, *, provider, scopes=None, public=None, admins=None
    ):
        self.access_lists = {}
        self.provider = provider
        self.scopes = scopes if (scopes is not None) else ALL_SCOPES
        self.public = set(public or [])
        self.admins = set(admins or [])
        for key, value in access_lists.items():
            if isinstance(value, str):
                value = import_object(value)
            self.access_lists[key] = value

    def _get_id(self, principal):
        # Get the id (i.e. username) of this Principal for the
        # associated authentication provider.
        for identity in principal.identities:
            if identity.provider == self.provider:
                id = identity.id
                break
        else:
            raise ValueError(
                f"Principcal {principal} has no identity from provider {self.provider}. "
                f"Its identities are: {principal.identities}"
            )
        return id

    def allowed_scopes(self, node, principal):
        # If this is being called, filter_access has let us get this far.
        if principal is SpecialUsers.public:
            allowed = PUBLIC_SCOPES
        elif principal.type == "service":
            allowed = self.scopes
        elif self._get_id(principal) in self.admins:
            allowed = ALL_SCOPES
        # The simple policy does not provide for different Principals to
        # have different scopes on different Nodes. If the Principal has access,
        # they have the same hard-coded access everywhere.
        else:
            allowed = self.scopes
        return allowed

    def filters(self, node, principal, scopes):
        queries = []
        if principal is SpecialUsers.public:
            queries.append(KeysFilter(self.public))
        else:
            # Services have no identities; just use the uuid.
            if principal.type == "service":
                id = str(principal.uuid)
            else:
                id = self._get_id(principal)
            if id in self.admins:
                return queries
            if not scopes.issubset(self.scopes):
                return NO_ACCESS
            access_list = self.access_lists.get(id, [])
            if not ((principal is SpecialUsers.admin) or (access_list == self.ALL)):
                try:
                    allowed = set(access_list or [])
                except TypeError:
                    # Provide rich debugging info because we have encountered a confusing
                    # bug here in a previous implementation.
                    raise TypeError(
                        f"Unexpected access_list {access_list} of type {type(access_list)}. "
                        f"Expected iterable or {self.ALL}, instance of {type(self.ALL)}."
                    )
                queries.append(KeysFilter(allowed))
        return queries
