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
    container_creation_status=create_container_to_azure_blob(session_id_value)
    if not container_creation_status:
        print("ERROR CREATING CONTAINER")  # TODO HANDLE ERROR HERE

    return {"ens_id": "", "module": "session_init", "status": "completed"}  # TODO CHANGE THIS

from azure.storage.blob import BlobServiceClient
import logging
import os
def create_container_to_azure_blob(session_id):
    """
    Creates the container if it doesn't exist.
    """

    connection_string = os.getenv("BLOB_STORAGE__CONNECTION_STRING")
    container_name = session_id

    try:
        # Initialize the BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)

        # Create the container if it does not exist
        if not container_client.exists():
            container_client.create_container()
            logging.info(f"Created container: {container_name}")
            print("container created")
        return True

    except Exception as e:
        logging.error(f"{container_name} container failed to add to Azure Blob Storage. Error: {str(e)}")
        return False