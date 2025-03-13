import requests
import json
from app.core.security.jwt import create_jwt_token
from app.core.utils.db_utils import *
from app.core.config import get_settings
# GRID GRID - BUT SEARCHING BY BVD ID / CONTACT ID (CONTACT ID DOESNT WORK)

async def gridbyid_person(data, session): # THIS DOESNT WORK IN GRID GRID - DONT USE
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")
    bvd_id_value = data.get("bvd_id")

    required_columns = ["management"]
    retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value,
                                                session_id_value, session)
    retrieved_data = retrieved_data[0]

    management = retrieved_data.get("management", [])
    for contact in management:
        contact_id = contact.get("contact_id")

        # url = f"http://localhost:3000/api/v1/orbis/companies?sessionId={session_id_value}&ensId={ens_id_value}&bvdId={bvd_id_value}" #TODO
        headers = {}
        data = {}

        response = requests.request("GET", url, headers=headers, data=data)

        # Print the response text
        print(response.text)

    print("Performing Orbis ID Retrieval... Completed")

async def gridbyid_organisation(data, session): #TODO

    print("Retrieving GRID by Company BVD ID Analysis")

    ens_id = data["ens_id"]
    session_id = data["session_id"]
    bvd_id = data["bvd_id"]
    try:
        # Generate JWT token
        jwt_token = create_jwt_token("orchestration", "analysis")
    except Exception as e:
        print("Error generating JWT token:", e)
        raise
    orbis_url = get_settings().urls.orbis_engine
    url = f"{orbis_url}/api/v1/orbis/grid/id/companies?sessionId={session_id}&ensId={ens_id}&bvdId={bvd_id}"
    # Prepare headers with the JWT token
    headers = {
        "Authorization": f"Bearer {jwt_token.access_token}"
    }
    data = {}

    response = requests.request("GET", url, headers=headers, data=data)
    response_json = response.json()
    success = response_json.get("success", False)
    data = False
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

    # {
    #     "success": true,
    #     "message": "No event for the particular entity",
    # }

    print("Performing Orbis GRID for Company ... Completed")

    return {"module": "Orbis Grid Search", "status": "completed", "success": success, "data": data}
