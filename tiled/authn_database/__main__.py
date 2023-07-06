import os

from ..alembic_utils import temp_alembic_ini
from .alembic_constants import ALEMBIC_DIR, ALEMBIC_INI_TEMPLATE_PATH


def main(args=None):
    """
    This is runs the alembic CLI with a dynamically genericated config file.

    A database can be specified via TILED_DATABASE_URI, but it is not necessary to set
    it for operations that do not connect to any database, such as defining new database
    revisions (i.e. migrations).

    To define a new revision:

    $ python -m tiled.authn_database revision -m "description..."

    """
    import subprocess
    import sys

    if args is None:
        args = sys.argv[1:]
    with temp_alembic_ini(
        ALEMBIC_INI_TEMPLATE_PATH, ALEMBIC_DIR, os.getenv("TILED_DATABASE_URI", "")
    ) as config_file:
        return subprocess.check_output(["alembic", "-c", config_file, *args])


if __name__ == "__main__":
    main()
