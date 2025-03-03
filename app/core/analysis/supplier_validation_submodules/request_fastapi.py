import requests
from dotenv import load_dotenv
import os

load_dotenv()

def request_fastapi(data, flag):

    bulk_request = os.getenv("VALIDATION_URL_BULK") # TODO Update this
    single_request = os.getenv("VALIDATION_URL_SINGLE") # TODO Update this

    if flag.lower() == 'bulk':
        try:
            data={
                'bulk_request': data
            }
            response = requests.post(bulk_request, json=data)
            if response.status_code == 200:
                return response.json()  # Return the response from FastAPI
            else:
                return {"error": f"Failed to call FastAPI. Status code: {response.status_code}"}
        except Exception as e:
            return {"error": f"Error calling FastAPI: {str(e)}"}
    else:
        try:
            response = requests.post(single_request, json=data)
            if response.status_code == 200:
                return response.json()  # Return the response from FastAPI
            else:
                return {"error": f"Failed to call FastAPI. Status code: {response.status_code}"}
        except Exception as e:
            return {"error": f"Error calling FastAPI: {str(e)}"}