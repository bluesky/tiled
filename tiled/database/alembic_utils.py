import contextlib
import os.path
import tempfile

_here = os.path.abspath(os.path.dirname(__file__))

ALEMBIC_INI_TEMPLATE_PATH = os.path.join(_here, "alembic.ini.template")
ALEMBIC_DIR = os.path.join(_here, "migrations")


def write_alembic_ini(path, database_uri):
    """Write a complete alembic.ini from our template.

    Parameters
    ----------
    path : str
        path to the alembic.ini file that should be written.
    db_url : str
        The SQLAlchemy database url, e.g. `sqlite:///tiled.sqlite`.
    """
    with open(ALEMBIC_INI_TEMPLATE_PATH) as f:
        alembic_ini_tpl = f.read()

    with open(path, "w") as f:
        f.write(
            alembic_ini_tpl.format(
                migration_script_directory=ALEMBIC_DIR,
                # If there are any %s in the URL, they should be replaced with %%, since ConfigParser
                # by default uses %() for substitution. You'll get %s in your URL when you have usernames
                # with special chars (such as '@') that need to be URL encoded. URL Encoding is done with %s.
                # YAY for nested templates?
                database_uri=str(database_uri).replace("%", "%%"),
            )
        )


@contextlib.contextmanager
def temp_alembic_ini(database_uri):
    """
    Context manager for temporary alembic configuration file

    Temporarily write an alembic.ini file for use with alembic migration scripts.
    Context manager yields alembic.ini path.

    Parameters
    ----------
    datbase_uri : str
        The SQLAlchemy database url, e.g. `sqlite:///tiled.sqlite`.

    Returns
    -------
    alembic_ini: str
        The path to the temporary alembic.ini that we have created.
        This file will be cleaned up on exit from the context manager.
    """
    with tempfile.TemporaryDirectory() as td:
        alembic_ini = os.path.join(td, "alembic.ini")
        write_alembic_ini(alembic_ini, database_uri)
        yield alembic_ini


def main(args=None):
    """
    This is runs the alembic CLI with a dynamically genericated config file.

    A database can be specified via TILED_DATABASE_URI, but it is not necessary to set
    it for operations that do not connect to any database, such as defining new database
    revisions (i.e. migrations).

    To define a new revision:

    $ python -m tiled.database.alembic_utils revision -m "description..."

    """
    import subprocess
    import sys

    if args is None:
        args = sys.argv[1:]
    with temp_alembic_ini(os.getenv("TILED_DATABASE_URI", "")) as config_file:
        return subprocess.check_output(["alembic", "-c", config_file, *args])


if __name__ == "__main__":
    main()
