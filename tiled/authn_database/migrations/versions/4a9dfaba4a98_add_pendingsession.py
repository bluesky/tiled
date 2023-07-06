"""Add PendingSession.

Revision ID: 4a9dfaba4a98
Revises: 56809bcbfcb0
Create Date: 2022-10-22 19:11:37.926595

"""
from alembic import op
from sqlalchemy import Column, DateTime, ForeignKey, Integer, LargeBinary, Unicode

# revision identifiers, used by Alembic.
revision = "4a9dfaba4a98"
down_revision = "56809bcbfcb0"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "pending_sessions",
        Column(
            "hashed_device_code",
            LargeBinary(32),
            primary_key=True,
            index=True,
            nullable=False,
        ),
        Column("user_code", Unicode(8), index=True, nullable=False),
        Column("expiration_time", DateTime(timezone=False), nullable=False),
        Column("session_id", Integer, ForeignKey("sessions.id"), nullable=True),
    )


def downgrade():
    op.drop_table("pending_sessions")
