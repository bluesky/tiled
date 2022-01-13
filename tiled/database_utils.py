import contextlib
import os.path
import subprocess
import sys
import tempfile

_here = os.path.abspath(os.path.dirname(__file__))

ALEMBIC_INI_TEMPLATE_PATH = os.path.join(_here, "alembic.ini.template")
ALEMBIC_DIR = os.path.join(_here, "alembic")


def write_alembic_ini(path, database_uri):
    """Write a complete alembic.ini from our template.

    Parameters
    ----------
    path : str
        path to the alembic.ini file that should be written.
    db_url : str
        The SQLAlchemy database url, e.g. `sqlite:///jupyterhub.sqlite`.
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
def _temp_alembic_ini(database_uri):
    """
    Context manager for temporary JupyterHub tiled directory

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


def run_alembic(args=None):
    """
    Run the alembic command against a temporary alembic.ini file.

    The file will is generated from a template to include the database URI and
    the location of the alembic migration scripts.
    """
    if args is None:
        args = sys.argv[1:]
    database_uri = "sqlite:///tiled.sqlite"  # TEMP
    with _temp_alembic_ini(database_uri) as alembic_ini:
        subprocess.check_call(["alembic", "-c", alembic_ini] + args)


if __name__ == "__main__":
    sys.exit(run_alembic())
