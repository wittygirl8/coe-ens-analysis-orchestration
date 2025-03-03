import requests
from app.core.security.jwt import create_jwt_token
from app.core.utils.db_utils import *
from app.core.config import get_settings
# GRID GRID - BUT SEARCHING BY NAME (COMPANY NAME OR PERSON NAME)

async def gridbyname_person(data, session):
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")
    bvd_id_value = data.get("bvd_id")

    required_columns = ["management"]
    retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value,
                                                session_id_value, session)
    retrieved_data = retrieved_data[0]
    management = retrieved_data.get("management", [])  # TODO Need to filter only those with indicator

    required_columns = ["city", "country"]
    retrieved_data_1 = await get_dynamic_ens_data("supplier_master_data", required_columns, ens_id_value,
                                                session_id_value, session)
    retrieved_data_1 = retrieved_data_1[0]
    city = retrieved_data_1.get("city","")
    country = retrieved_data_1.get("country","")

    done_name = []
    for contact in management:
        contact_id = contact.get("id")
        personnel_name = contact.get("name")
        indicators = [contact.get("pep_indicator",""), contact.get("media_indicator",""), contact.get("sanctions_indicator",""), contact.get("watchlist_indicator","")]
        # print(indicators)

        if ("Yes" in indicators) and (personnel_name not in done_name):
            try:
                # Generate JWT token
                jwt_token = create_jwt_token("orchestration", "analysis")
            except Exception as e:
                print("Error generating JWT token:", e)
                raise
            orbis_url = get_settings().urls.orbis_engine
            url = f"{orbis_url}/api/v1/orbis/grid/personnels?sessionId={session_id_value}&ensId={ens_id_value}&contactId={contact_id}&personnelName={personnel_name}&city={city}&country={country}"

            # Prepare headers with the JWT token
            headers = {
                "Authorization": f"Bearer {jwt_token.access_token}"
            }
            data = {}

            response = requests.request("GET", url, headers=headers, data=data)

            # Print the response text
            # print(response.text)
            done_name.append(personnel_name)
    print("Performing Orbis ID Retrieval... Completed")
    return {}

async def gridbyname_organisation(data, session):

    print("Retrieving Orbis by Company Grid Analysis")

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
    city = retrieved_data_1.get("city","")
    country = retrieved_data_1.get("country","")
    try:
        # Generate JWT token
        jwt_token = create_jwt_token("orchestration", "analysis")
    except Exception as e:
        print("Error generating JWT token:", e)
        raise
    orbis_url = get_settings().urls.orbis_engine
    url = f"{orbis_url}/api/v1/orbis/grid/companies?sessionId={session_id_value}&ensId={ens_id_value}&bvdId={bvd_id}&orgName={name}&city={city}&country={country}"

    headers = {
                "Authorization": f"Bearer {jwt_token.access_token}"
            }
    data = {}

    response = requests.request("GET", url, headers=headers, data=data)

    # Print the response text
    # print(response.text)

    print("Performing Orbis GRID for Company ... Completed")

    return {}
