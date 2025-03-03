from sqlalchemy import insert, or_, select
from sqlalchemy.exc import SQLAlchemyError  # To catch SQLAlchemy-specific errors
import logging

from app.models import *
from app.core.utils.db_utils import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def ensid_screening_status_initialisation(session_id_value: str, session):

    print("IN ENSID SCREENING STATUS INIT")
    # Get all ens_id of session_id - insert into ensid_screening_status: # ensid_screening_status will be updated with relevant columns at the end of each component

    required_columns = ["ens_id"]  # We want only [{"ens_id": "ABC-123"},{"ens_id": "ABZ-122"},
    ens_ids_rows = await get_ens_ids_for_session_id("supplier_master_data", required_columns, session_id_value, session)
    print("GOT ENS ID ROWS")
    print(ens_ids_rows)
    ens_ids_rows = [{**entry,
                     "overall_status": STATUS.STARTED,
                     "orbis_retrieval_status": STATUS.NOT_STARTED,
                     "screening_modules_status": STATUS.NOT_STARTED,
                     "report_generation_status": STATUS.NOT_STARTED
                     } for entry in ens_ids_rows]

    insert_status = await upsert_ensid_screening_status(ens_ids_rows, session_id_value, session)
    # print(insert_status)

    return {"ens_id": "", "module": "session_init", "status": "completed"}  # TODO CHANGE THIS
