import asyncio
import requests
from app.core.config import get_settings
from app.core.security.jwt import create_jwt_token
from app.schemas.logger import logger

async def orbis_company(data, session):

    logger.warning("Retrieving Orbis - Company Data..")

    # Define the query parameters as variables
    session_id = data["session_id"]
    ens_id = data["ens_id"]
    bvd_id = data["bvd_id"]
    try:
        # Generate JWT token
        jwt_token = create_jwt_token("orchestration", "analysis")
    except Exception as e:
        logger.error(f"Error generating JWT token: {e}")
        raise
    orbis_url = get_settings().urls.orbis_engine
    url = f"{orbis_url}/api/v1/orbis/companies?sessionId={session_id}&ensId={ens_id}&bvdId={bvd_id}"
    # Prepare headers with the JWT token
    headers = {
        "Authorization": f"Bearer {jwt_token.access_token}"
    }
    payload = {}
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        logger.debug(f"Company Data status code: {response.status_code}")

        logger.warning("Performing Orbis Company Retrieval... Completed")
        if response.status_code == 200:
            print("in if")
            return {"module": "data_orbis_company", "status": "completed"}
        else:
            print("in else")
            return {"module": "data_orbis_company", "status": "failed"}
    except:
        logger.warning("in else")
        return {"module": "data_orbis_company", "status": "failed"}
