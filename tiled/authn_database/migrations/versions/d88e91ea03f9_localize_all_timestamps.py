"""Localize all timestamps

Revision ID: d88e91ea03f9
Revises: 13024b8a6b74
Create Date: 2025-02-18 14:15:48.967976

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d88e91ea03f9"
down_revision = "13024b8a6b74"
branch_labels = None
depends_on = None


# mapping table names to list of column given as (name, nullable)
datetime_columns = {
    "principals": [("time_created", True), ("time_updated", True)],
    "identities": [
        ("latest_login", True),
        ("time_created", False),
        ("time_updated", True),
    ],
    "roles": [("time_created", True), ("time_updated", True)],
    "api_keys": [
        ("expiration_time", True),
        ("latest_activity", True),
        ("time_created", False),
        ("time_updated", True),
    ],
    "sessions": [
        ("expiration_time", False),
        ("time_created", False),
        ("time_last_refreshed", True),
        ("time_updated", True),
    ],
    "pending_sessions": [("expiration_time", False)],
}


def upgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "sqlite":
        # No action required. SQLAlchemy handles timezones at the application
        # level and does not store anything like +00 or +Z in the database.
        return

    for table, columns in datetime_columns.items():
        for column, nullable in columns:
            # Create a temporary column with localized datetimes.
            # Hardcode nullable=True because the values are not initialized.
            # We will set nullable properly at the end.
            op.add_column(
                table,
                # NOTE: Later it was noticed that server_default was missed here.
                # The following migration (0c705a02954c) fixed this.
                sa.Column(
                    f"{column}_localized", sa.DateTime(timezone=True), nullable=True
                ),
            )

            # Copy date from naive to localized column.
            connection.execute(
                sa.text(
                    f"""
                    UPDATE {table}
                    SET {column}_localized = {column} AT TIME ZONE 'UTC'
                    WHERE {column} IS NOT NULL
                """
                )
            )

            # Drop the original (naive) column.
            op.drop_column(table, column)

            # Rename the new column to the original name, and set nullable to
            # the correct value.
            op.alter_column(
                table, f"{column}_localized", new_column_name=column, nullable=nullable
            )


def downgrade():
    connection = op.get_bind()
    if connection.engine.dialect.name == "sqlite":
        # No action required. SQLAlchemy handles timezones at the application
        # level and does not store anything like +00 or +Z in the database.
        return

    for table, columns in datetime_columns.items():
        for column, nullable in columns:
            # Create a temporary column with naive datetimes.
            # Hardcode nullable=True because the values are not initialized.
            # We will set nullable properly at the end.
            op.add_column(
                table,
                sa.Column(
                    f"{column}_naive", sa.DateTime(timezone=False), nullable=False
                ),
            )

            # Copy date from localized to naive column.
            connection.execute(
                sa.text(
                    f"""
                    UPDATE {table}
                    SET {column}_naive = {column} AT TIME ZONE 'UTC'
                    WHERE {column} IS NOT NULL
                """
                )
            )

            # Drop the original (localized) column.
            op.drop_column(table, column)

            # Rename the new column to the original name, and set nullable to
            # the correct value.
            op.alter_column(
                table, f"{column}_naive", new_column_name=column, nullable=nullable
            )
