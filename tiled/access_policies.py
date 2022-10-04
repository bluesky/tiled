from .queries import KeysFilter
from .scopes import SCOPES
from .utils import Sentinel, SpecialUsers, import_object

ALL_SCOPES = set(SCOPES)
NO_ACCESS = Sentinel("NO_ACCESS")


class DummyAccessPolicy:
    "Impose no access restrictions."

    def allowed_scopes(self, node, principal):
        return ALL_SCOPES

    def filters(self, node, principal, scopes):
        return []


class SimpleAccessPolicy:
    """
    A mapping of user names to lists of entries they have full access.

    This simple policy does not provide fine-grained control of scopes.

    >>> SimpleAccessPolicy({"alice": ["A", "B"], "bob": ["B"]}, provider="toy")
    """

    ALL = object()  # sentinel

    def __init__(self, access_lists, *, provider, scopes=None, public=None):
        self.access_lists = {}
        self.provider = provider
        self.scopes = scopes if (scopes is not None) else ALL_SCOPES
        self.public = set(public or [])
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
        # The simple policy does not provide for different Principals to
        # have different scopes on different Nodes. If the Principal has access,
        # they have the same hard-coded access everywhere.
        return self.scopes

    def filters(self, node, principal, scopes):
        if not scopes.issubset(self.scopes):
            return NO_ACCESS
        id = self._get_id(principal)
        access_list = self.access_lists.get(id, [])
        queries = []
        if not ((principal is SpecialUsers.admin) or (access_list is self.ALL)):
            allowed = set(access_list or []) | self.public
            queries.append(KeysFilter(allowed))
        return queries
