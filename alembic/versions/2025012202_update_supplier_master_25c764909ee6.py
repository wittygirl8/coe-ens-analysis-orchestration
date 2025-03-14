"""update Supplier_master

Revision ID: 25c764909ee6
Revises: e59305da1d21
Create Date: 2025-01-22 12:02:40.787826

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "25c764909ee6"
down_revision = "e59305da1d21"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "supplier_master_data",
        sa.Column("validation_indicator", sa.Boolean(), nullable=False),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("supplier_master_data", "validation_indicator")
    # ### end Alembic commands ###
