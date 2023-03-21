import pytest

from ..catalog.adapter import Adapter, DatabaseNotFound


def test_constructors(tmpdir):
    # Create an adapter with a database in memory.
    Adapter.in_memory()
    # Cannot connect to database that does not exist.
    # with pytest.raises(DatabaseNotFound):
    #     Adapter.from_uri(f"sqlite+aiosqlite:///{tmpdir}/database.sqlite")
    # Create one.
    Adapter.create_from_uri(f"sqlite+aiosqlite:///{tmpdir}/database.sqlite")
    # Now connecting works.
    Adapter.from_uri(f"sqlite+aiosqlite:///{tmpdir}/database.sqlite")
