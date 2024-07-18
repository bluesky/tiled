"""Create sqlite table for fulltext search

Revision ID: ed3a4223a600
Revises: e756b9381c14
Create Date: 2024-04-11 16:41:01.369520

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ed3a4223a600"
down_revision = "e756b9381c14"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "sqlite":
        statements = [
            # Create an external content fts5 table.
            # See https://www.sqlite.org/fts5.html Section 4.4.3.
            """
            CREATE VIRTUAL TABLE metadata_fts5 USING fts5(metadata, content='nodes', content_rowid='id');
            """,
            # Insert all existing node content into the fts5 table.
            """
            INSERT INTO metadata_fts5(rowid, metadata)
            SELECT id, metadata FROM nodes;
            """,
            # Triggers keep the index synchronized with the nodes table.
            """
            CREATE TRIGGER nodes_metadata_fts5_sync_ai AFTER INSERT ON nodes BEGIN
              INSERT INTO metadata_fts5(rowid, metadata)
              VALUES (new.id, new.metadata);
            END;
            """,
            """
            CREATE TRIGGER nodes_metadata_fts5_sync_ad AFTER DELETE ON nodes BEGIN
              INSERT INTO metadata_fts5(metadata_fts5, rowid, metadata)
              VALUES('delete', old.id, old.metadata);
            END;
            """,
            """
            CREATE TRIGGER nodes_metadata_fts5_sync_au AFTER UPDATE ON nodes BEGIN
              INSERT INTO metadata_fts5(metadata_fts5, rowid, metadata)
              VALUES('delete', old.id, old.metadata);
              INSERT INTO metadata_fts5(rowid, metadata)
              VALUES (new.id, new.metadata);
            END;
            """,
        ]
        for statement in statements:
            op.execute(sa.text(statement))


def downgrade():
    pass
