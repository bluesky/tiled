import os

_here = os.path.abspath(os.path.dirname(__file__))
ALEMBIC_INI_TEMPLATE_PATH = os.path.join(_here, "alembic.ini.template")
ALEMBIC_DIR = os.path.join(_here, "migrations")
