"""add_webhooks_tables

Revision ID: 85a47342e78e
Revises: 4cf6011a8db5
Create Date: 2026-04-22

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func, text

# Inline definition so this migration remains self-contained and does not
# depend on the live application ORM module (which may change in the future).
JSONVariant = sa.JSON().with_variant(JSONB(), "postgresql")

# revision identifiers, used by Alembic.
revision = "85a47342e78e"
down_revision = "4cf6011a8db5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "webhooks",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("node_id", sa.Integer(), nullable=False),
        sa.Column("url", sa.Unicode(length=2048), nullable=False),
        sa.Column("secret", sa.Unicode(length=512), nullable=True),
        sa.Column("events", JSONVariant, nullable=True),
        sa.Column(
            "active",
            sa.Boolean(),
            nullable=False,
            server_default=text("true"),
        ),
        sa.Column(
            "time_created",
            sa.DateTime(timezone=False),
            server_default=func.now(),
        ),
        sa.Column(
            "time_updated",
            sa.DateTime(timezone=False),
            onupdate=func.now(),
            server_default=func.now(),
        ),
        sa.ForeignKeyConstraint(["node_id"], ["nodes.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_webhooks_node_id"), "webhooks", ["node_id"], unique=False)

    op.create_table(
        "webhook_deliveries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("webhook_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Unicode(length=128), nullable=False),
        sa.Column("event_type", sa.Unicode(length=128), nullable=False),
        sa.Column("payload", JSONVariant, nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default=text("0"),
        ),
        sa.Column("delivered_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column(
            "outcome",
            sa.Unicode(length=16),
            nullable=False,
            server_default=text("'pending'"),
        ),
        sa.Column("error_detail", sa.Unicode(length=4096), nullable=True),
        sa.Column(
            "time_created",
            sa.DateTime(timezone=False),
            server_default=func.now(),
        ),
        sa.Column(
            "time_updated",
            sa.DateTime(timezone=False),
            onupdate=func.now(),
            server_default=func.now(),
        ),
        sa.ForeignKeyConstraint(["webhook_id"], ["webhooks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_webhook_deliveries_webhook_id"),
        "webhook_deliveries",
        ["webhook_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_webhook_deliveries_event_id"),
        "webhook_deliveries",
        ["event_id"],
        unique=False,
    )


def downgrade():
    op.drop_index(
        op.f("ix_webhook_deliveries_event_id"), table_name="webhook_deliveries"
    )
    op.drop_index(
        op.f("ix_webhook_deliveries_webhook_id"), table_name="webhook_deliveries"
    )
    op.drop_table("webhook_deliveries")
    op.drop_index(op.f("ix_webhooks_node_id"), table_name="webhooks")
    op.drop_table("webhooks")
