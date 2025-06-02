import httpx
import requests
from app.core.security.jwt import create_jwt_token
from app.core.utils.db_utils import *
from app.core.config import get_settings
from app.schemas.logger import logger
import asyncio
from urllib.parse import urlencode
# GRID GRID - BUT SEARCHING BY NAME (COMPANY NAME OR PERSON NAME)

async def gridbyname_person(data, session):
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")
    bvd_id_value = data.get("bvd_id")

    required_columns = ["management"]
    retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value,
                                                session_id_value, session)
    retrieved_data = retrieved_data[0]
    management = retrieved_data.get("management", []) or []  # TODO Need to filter only those with indicator

    if management is None:
        return {"module": "Orbis Grid Search", "status": "completed"}

    required_columns = ["city", "country"]
    retrieved_data_1 = await get_dynamic_ens_data("supplier_master_data", required_columns, ens_id_value,
                                                session_id_value, session)
    retrieved_data_1 = retrieved_data_1[0]
    city = retrieved_data_1.get("city","")
    country = retrieved_data_1.get("country","")
    done_name = []
    tasks = []
    try:
        # Generate JWT token
        jwt_token = create_jwt_token("orchestration", "analysis")
    except Exception as e:
        logger.error(f"Error generating JWT token: {e}")
        raise
    orbis_url = get_settings().urls.orbis_engine

    # Prepare headers with the JWT token
    headers = {
        "Authorization": f"Bearer {jwt_token.access_token}"
    }

    semaphore = asyncio.Semaphore(3)
    async def process(contact, client):
        """
        Function to call multiple grid personnel checks simultaneously
        :param contact:
        :param client:
        :return: Returns True if the Process FAILED
        """
        async with semaphore:
            contact_id = contact.get("id")
            personnel_name = contact.get("name")
            indicators = [contact.get("pep_indicator",""), contact.get("media_indicator",""), contact.get("sanctions_indicator",""), contact.get("watchlist_indicator","")]

            if ("Yes" in indicators) and (personnel_name not in done_name):

                params = {
                    "sessionId": session_id_value,
                    "ensId": ens_id_value,
                    "contactId": contact_id,
                    "personnelName": personnel_name,
                    "city": city,
                    "country": country
                }

                query_string = urlencode(params)
                url = f"{orbis_url}/api/v1/orbis/grid/personnels?{query_string}"

                try:
                    done_name.append(personnel_name)
                    response = await client.get(url, headers=headers)
                    if (response.status_code != 200) and (response.status_code != 201):
                        logger.warning(f"Error found for GRID search by Name Request - {personnel_name}: {response.status_code}")
                        return True
                except Exception as e:
                    logger.error(f"Error - Request failed for GRID search by Name - {personnel_name}: {str(e)}")
                    return True
            return False
    if len(management)>0:
        async with httpx.AsyncClient() as client:
            tasks = []
            for contact in management:
                tasks.append(process(contact, client))
            results = await asyncio.gather(*tasks)
            logger.info("Performing Orbis ID Retrieval... Completed")
    else:
        results = [False]
    print("----- results-------")
    if all(results) and (len(results) > 3):
        logger.error("Performing Orbis ID Retrieval... Failed for All Persons")
        return {"module": "Orbis Grid Search", "status": "failed"}

    logger.info("Performing Orbis ID Retrieval... COMPLETED")
    return {"module": "Orbis Grid Search", "status": "completed"}

async def gridbyname_organisation(data, session):

    logger.info("Retrieving Orbis by Company Grid Analysis")

    ens_id_value = data["ens_id"]
    session_id_value = data["session_id"]
    bvd_id = data["bvd_id"]

    required_columns = ["name"]
    retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value,
                                                session_id_value, session)
    retrieved_data = retrieved_data[0]
    name = retrieved_data.get("name", "")

    required_columns = ["city", "country"]
    retrieved_data_1 = await get_dynamic_ens_data("supplier_master_data", required_columns, ens_id_value,
                                                session_id_value, session)
    retrieved_data_1 = retrieved_data_1[0]
    city = retrieved_data_1.get("city", "Unknown")
    country = retrieved_data_1.get("country", "")
    if city == "":
        city = "Unknown"
    try:
        # Generate JWT token
        jwt_token = create_jwt_token("orchestration", "analysis")
    except Exception as e:
        logger.error(f"Error generating JWT token: {e}")
        raise
    orbis_url = get_settings().urls.orbis_engine

    params = {
        "sessionId": session_id_value,
        "ensId": ens_id_value,
        "bvdId": bvd_id,
        "orgName": name,
        "city": city,
        "country": country
    }

    query_string = urlencode(params)
    url = f"{orbis_url}/api/v1/orbis/grid/companies?{query_string}"

    headers = {
                "Authorization": f"Bearer {jwt_token.access_token}"
            }
    data = {}
    try:
        response = requests.request("GET", url, headers=headers, data=data)
        if (response.status_code != 200) and (response.status_code != 201):
            return {"module": "Orbis Grid Search", "status": "failed", "success": False, "data": False, "adv_count": 0}
        response_json = response.json()
        success = response_json.get("success", False)
        data = False
        adv_count = response_json.get("adv_count", 0)
        if success == True:
            data = response_json.get("data", False)
            if data == '':
                data = False
            elif data != False:
                data = True
            else:
                data = False

        # Print the response text
        # print(response.text)

        logger.info("Performing Orbis GRID for Company ... Completed")

        return {"module": "Grid Grid ID Search", "status": "completed", "success": success, "data": data, "adv_count": adv_count}
    except Exception as e:
        logger.error(f" gridbyname_organisation: {str(e)}")
        return {"module": "Orbis Grid Search", "status": "failed", "success": False, "data": False, "adv_count": 0}