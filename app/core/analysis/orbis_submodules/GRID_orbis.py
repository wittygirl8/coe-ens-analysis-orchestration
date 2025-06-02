import asyncio
import requests
from app.core.config import get_settings
from app.core.security.jwt import create_jwt_token
from app.schemas.logger import logger
async def orbis_grid_search(data, session):

    logger.info("Retrieving Orbis - Grid Search...")

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
    url = f"{orbis_url}/api/v1/orbis/orbisgrid/companies?sessionId={session_id}&ensId={ens_id}&bvdId={bvd_id}"

    payload = {}
    headers = {
        "Authorization": f"Bearer {jwt_token.access_token}"
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response_json=response.json()
        if (response.status_code != 200) and (response.status_code != 201):
            return {"module": "Orbis Grid Search", "status": "failed", "success": False, "data": False, "adv_count": 0}
        adv_count = response_json.get("adv_count", 0)
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

        logger.info("Performing Orbis Grid Search... Completed")
        return {"module": "Orbis Grid Search", "status": "completed", "success": success, "data": data, "adv_count": adv_count}
    except:
        return {"module": "data_orbis_company", "status": "failed"}
