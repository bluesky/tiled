from logging import Filter, LogRecord

from ..utils import SpecialUsers
from .app import current_principal


class PrincipalFilter(Filter):
    """Logging filter to attach username or Service Principal UUID to LogRecord"""

    def filter(self, record: LogRecord) -> bool:
        principal = current_principal.get()
        if isinstance(principal, SpecialUsers):
            short_name = f"{principal.value}"
        elif principal.type == "service":
            short_name = f"service:{principal.uuid}"
        else:  # principal.type == "user"
            short_name = ",".join(
                f"'{identity.id}'" for identity in principal.identities
            )
        record.principal = short_name
        return True
