import asyncio
import requests
from app.core.config import get_settings
from app.core.security.jwt import create_jwt_token

async def orbis_news_search(data, session):

    print("Retrieving Orbis - Grid Search...")

    session_id = data["session_id"]
    ens_id = data["ens_id"]
    bvd_id = data["bvd_id"]
    try:
        # Generate JWT token
        jwt_token = create_jwt_token("orchestration", "analysis")
    except Exception as e:
        print("Error generating JWT token:", e)
        raise
    orbis_url = get_settings().urls.orbis_engine
    url = f"{orbis_url}/api/v1/orbis/news?sessionId={session_id}&ensId={ens_id}&bvdId={bvd_id}"

    payload = {}
    headers = {
        "Authorization": f"Bearer {jwt_token.access_token}"
    }
    response = requests.request("GET", url, headers=headers, data=payload)

    print("Performing Orbis Grid Search... Completed")
    print("news:", response)

    return {"module": "Orbis Grid Search", "status": "completed"}