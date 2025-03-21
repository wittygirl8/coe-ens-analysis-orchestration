# http://127.0.0.1:8000/#/

# Imports 
import json
import openai
from openai import AzureOpenAI
from dotenv import load_dotenv
import os

import asyncio
from app.core.security.jwt import create_jwt_token
from app.core.utils.db_utils import *
from app.core.analysis.supplier_validation_submodules.request_fastapi import request_fastapi
from app.core.analysis.supplier_validation_submodules.LLM import run_ts_analysis
from app.core.analysis.supplier_validation_submodules.utilities import *
from app.models import *
import requests
from urllib.parse import quote
import logging 
from app.core.config import get_settings

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  
    ]
)

# Create a logger instance
log = logging.getLogger(__name__)

AZURE_ENDPOINT=os.getenv("OPENAI__AZURE_ENDPOINT")
API_KEY= os.getenv("OPENAI__API_KEY")
CONFIG=os.getenv("OPENAI__CONFIG")
SCRAPER=os.getenv("SCRAPER__SCRAPER_URL")

require_llm_response_speed = True
if require_llm_response_speed or (CONFIG.lower() == "demo"):
    model_deployment_name = "gpt-4-32k"
else:
    model_deployment_name = "gpt-4o"

client = AzureOpenAI(
    azure_endpoint=AZURE_ENDPOINT,
    api_key=API_KEY,
    api_version="2024-07-01-preview"
)

async def supplier_name_validation(data, session, search_engine:str):

    results = []  
    
    incoming_ens_id = data["ens_id"]
    incoming_country = data["uploaded_country"]
    incoming_name = data["uploaded_name"]
    session_id = data["session_id"]
    national_id = data["uploaded_national_id"]

    incoming_address = data.get("uploaded_address", "") if len(data.get("uploaded_address", ""))>0 else "None"
    incoming_city = data.get("uploaded_city", "") if len(data.get("uploaded_city", ""))>0 else "None"
    incoming_postcode = data.get("uploaded_postcode", "") if len(data.get("uploaded_postcode", ""))>0 else "None"
    incoming_email = data.get("uploaded_email_or_website", "") if len(data.get("uploaded_email_or_website", ""))>0 else "None"
    incoming_p_o_f = data.get("uploaded_phone_or_fax", "") if len(data.get("uploaded_phone_or_fax", ""))>0 else "None"
    incoming_state = data.get("uploaded_state", "") if len(data.get("uploaded_state", ""))>0 else "None"

    log.info("================================================")
    log.info(f"[SNV] ens_id = {incoming_ens_id}")

    # required_columns = ["uploaded_name", "uploaded_country", "ens_id"]
    # db_data = await get_dynamic_ens_data("upload_supplier_master_data", required_columns, incoming_ens_id, session_id=session_id, session=session)
    # print("\n\ndb data>>> \n\n", db_data)

    def get_possible_suppliers(payload, static_case=None):
        try:
            # Generate JWT token
            jwt_token = create_jwt_token("orchestration", "analysis")
            print("TOKEN:",jwt_token)
        except Exception as e:
            print("Error generating JWT token:", e)
            raise
        orbis_url = get_settings().urls.orbis_engine

        base_url = f"{orbis_url}/api/v1/orbis/truesight/companies"

        # Ensure all values are properly URL-encoded
        query_params = {
            "orgName": quote(payload["orgName"]),
            "orgCountry": quote(payload["orgCountry"]),
            "sessionId": quote(payload["sessionId"]),
            "ensId": quote(payload["ensId"]),
            "nationalId": quote(payload["nationalId"]),
            "state": quote(payload["state"]),
            "city": quote(payload["city"]),
            "address": quote(payload["address"]),
            "postCode": quote(payload["postcode"]),
            "emailOrWebsite": quote(payload["email"]),
            "phoneOrFax": quote(payload["phone_or_fax"])
        }
        query_string = "&".join(f"{key}={value}" for key, value in query_params.items())
        url = f"{base_url}?{query_string}"

        if static_case == False:
            try:
                headers = {
                    "Authorization": f"Bearer {jwt_token.access_token}"
                }
                print("headers", headers)
                response = requests.get(url, headers=headers)
                print("Passed orbis call")
                # Raise an error if the response status is not 200
                if response.status_code != 200:
                    raise requests.HTTPError(f"API request failed with status code {response.status_code}: {response.text}")

                try:
                    response_json = response.json()  # Try parsing JSON
                except ValueError as e:
                    raise ValueError("API response is not valid JSON") from e

                # Check if "data" key exists
                if "data" not in response_json:
                    raise KeyError("Missing 'data' key in API response")

                # Extract supplier data from response
                supplier_data, potential_pass, matched = filter_supplier_data(response_json, max_results=2)
                # Debugging: Print parsed response (only in dev mode)
                # print("\n\nAPI Parsed JSON >>>", json.dumps(response_json, indent=2))
                print("MATCHED -----")
                print(json.dumps(supplier_data, indent=2))
                return supplier_data, potential_pass, matched

            except requests.exceptions.RequestException as e:
                raise RuntimeError(f"Failed to fetch supplier data: {e}")

        else:
            # Running cases (loading a local JSON file)
            potentials = r"app\core\analysis\supplier_validation_submodules\files\response_potential_case.json"
            with open(potentials, 'r', encoding='utf-8') as file:
                case_A = json.load(file)

            supplier_data, potential_pass, matched = filter_supplier_data(case_A, max_results=2)
            return supplier_data, potential_pass, matched
        
    # TODO: Add more fields here, address, postcode etc. Check with Prakruthi
    match_payload = {
        "orgName": str(incoming_name),
        "orgCountry": str(incoming_country),
        "ensId": str(incoming_ens_id),
        "sessionId": str(data["session_id"]),
        "nationalId":str(national_id),
        "address": str(incoming_address),
        "city": str(incoming_city),
        "postcode": str(incoming_postcode),
        "email": str(incoming_email),
        "phone_or_fax":str(incoming_p_o_f),
        "state": str(incoming_state)
    }


    supplier_data, potential_pass, matched= get_possible_suppliers(match_payload, static_case=False)
    log.info(f"[SNV] Matched Status: {matched}")
    log.info(f"[SNV] Potential pass: {potential_pass}")
 
    try:
        
        # No match on both L1 and L2
        # if not supplier_data:
            
        #     updated_data = {
        #         "validation_status": ValidationStatus.VALIDATED,
        #         "orbis_matched_status": OribisMatchStatus.NO_MATCH,
        #         "truesight_status": TruesightStatus.NO_MATCH
        #     }
        #     update_status = await update_dynamic_ens_data("upload_supplier_master_data", updated_data, ens_id=incoming_ens_id, session_id=session_id, session=session)
        #     results = []
        #     api_response = {
        #             "ens_id": incoming_ens_id,
        #             "L2_verification": TruesightStatus.NO_MATCH,
        #             "L2_confidence": None,
        #             "verification_details": None,
        #             "comments": "There was no data found for this entity"
        #         }
        #     results.append(api_response)
        #     return True, results

        if not matched and not potential_pass:

            log.info("[SNV]  == Performing L2 Validation == ")
            payload = {
                "country": incoming_country,
                "name": incoming_name,
                "language": "en",
                "request_type": "single"
            }

            sample = request_fastapi(payload, flag='single')  

            if search_engine == "google":
                country_codes = r"app\core\analysis\supplier_validation_submodules\files\codes_google.json"
                with open(country_codes, 'r', encoding='utf-8') as file:
                        country_data = json.load(file)
                country_from_codes = get_country_google(str(incoming_country), country_data=country_data)
            elif search_engine == "bing":
                country_codes = r"app\core\analysis\supplier_validation_submodules\files\codes_bing.json"
                with open(country_codes, 'r', encoding='utf-8') as file:
                        country_data = json.load(file)
                country_from_codes = get_country_bing(str(incoming_country), country_data=country_data)

            analysis = []
            if len(sample.get("data", [])) > 0:
                for item in sample["data"]:
                    # print("\n\n News Article :\n\n",item.get("full_article"))
                    url = str(item.get("link"))
                    ts_flag, token_usage = run_ts_analysis(
                        client=client,
                        model=model_deployment_name,
                        article=item.get("full_article"),
                        name=str(incoming_name),
                        country=str(country_from_codes),
                        url=url,
                    )
                    ts_flag['link'] = url
                    analysis.append(ts_flag)
                    log.info(f"[SNV] TS analysis: {analysis}")

                # Aggregate results
                agg_output = aggregate_verified_flag(analysis)
                log.info(f"[SNV] Aggregated verified Flag: {agg_output}")
                # Calculate metric
                metric = calculate_metric(
                    num_true=agg_output['num_yes'],
                    num_analyzed=agg_output['num_analysed'],
                    max_articles=10
                )
                agg_output["ens_id"] = incoming_ens_id
                agg_output["token_usage"] = token_usage
            else:
                agg_output = {
                    "num_yes": 0,
                    "num_analysed": 0,
                    "ens_id": incoming_ens_id,
                    "verified": "No",
                    "token_usage": None
                }
                metric = 0.0
        
            # TODO: Worst case scenario - passing out the top scoring element from orbis match
            if supplier_data:

                temp = supplier_data[0]  # Extract the first entry
                # TEMP LOGIC TO IMPROVE MATCHES
                for match in supplier_data:
                    print(national_id)
                    print(str(match.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')))
                    if str(match.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')) == national_id:
                        print("THIS MATCH -------------")
                        temp = match

                print(json.dumps(temp, indent=2))

                agg_verified = agg_output['verified']  # This can be True, False, or None
                # Map the boolean value to the appropriate TruesightStatus enum value
                if agg_verified == "Yes":
                    truesight_status_value = TruesightStatus.VALIDATED
                elif agg_verified == "No":
                    truesight_status_value = TruesightStatus.NOT_VALIDATED
                    # Update data for the current supplier

                updated_data = {
                "validation_status": ValidationStatus.VALIDATED,
                "orbis_matched_status": OribisMatchStatus.NO_MATCH,
                "truesight_status": truesight_status_value,
                "truesight_percentage":0,
                "matched_percentage": temp.get('MATCH', {}).get('0', {}).get('SCORE', 0),
                "suggested_bvd_id": str(temp.get('BVDID', 'N/A')),
                "suggested_name": str(temp.get('MATCH', {}).get('0', {}).get('NAME', 'N/A')),
                "suggested_address": str(temp.get('MATCH', {}).get('0', {}).get('ADDRESS', 'N/A')),
                "suggested_name_international": str(temp.get('MATCH', {}).get('0', {}).get('NAME_INTERNATIONAL', 'N/A')),
                "suggested_postcode": str(temp.get('MATCH', {}).get('0', {}).get('POSTCODE', 'N/A')),
                "suggested_city": str(temp.get('MATCH', {}).get('0', {}).get('CITY', 'N/A')),
                "suggested_country": str(temp.get('MATCH', {}).get('0', {}).get('COUNTRY', 'N/A')),
                "suggested_phone_or_fax": str(temp.get('MATCH', {}).get('0', {}).get('PHONEORFAX', 'N/A')),
                "suggested_email_or_website": str(temp.get('MATCH', {}).get('0', {}).get('EMAILORWEBSITE', 'N/A')),
                "suggested_national_id": str(temp.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')),
                "suggested_state": str(temp.get('MATCH', {}).get('0', {}).get('STATE', 'N/A')),
                "suggested_address_type": str(temp.get('MATCH', {}).get('0', {}).get('ADDRESS_TYPE', 'N/A'))
                }

                print("ENSID BEFORE-------", incoming_ens_id)
                processed_ens_id, duplicate = await check_and_update_unique_value(
                table_name="upload_supplier_master_data",
                column_name="suggested_bvd_id",
                bvd_id_to_check=f"{temp.get('BVDID', 'N/A')}",
                ens_id=incoming_ens_id,
                session=session
                )
                incoming_ens_id = processed_ens_id
                print("ENS ID AFTER", incoming_ens_id)
                if duplicate["status"] == "unique":
                    updated_data["pre_existing_bvdid"]=False
                elif duplicate["status"] == "duplicate":
                    updated_data["pre_existing_bvdid"]=True

                api_response = {
                    "ens_id": incoming_ens_id,
                    "L2_verification": "Required",
                    "L2_confidence": f"{metric * 100:.2f}",
                    "verification_details": updated_data
                }

            else:
                # TODO: Truesight will make an api call back to orbis once it finds that entity's unique identifier on the web, but for now: we say [no match - no match] 
                agg_verified = agg_output['verified']  # This can be True, False, or None
                # Map the boolean value to the appropriate TruesightStatus enum value
                if agg_verified == "Yes":
                    truesight_status_value = TruesightStatus.VALIDATED
                elif agg_verified == "No":
                    truesight_status_value = TruesightStatus.NOT_VALIDATED
                    # Update data for the current supplier
                updated_data = {
                    "validation_status": ValidationStatus.NOT_VALIDATED,
                    "orbis_matched_status": OribisMatchStatus.NO_MATCH,
                    "truesight_status": TruesightStatus.NO_MATCH,
                    "matched_percentage": 0,
                    "suggested_bvd_id": "",
                    "truesight_percentage":int(round(metric * 100, 2)),
                    "suggested_name": incoming_name,
                    "suggested_address": "",
                    "suggested_name_international":"",
                    "suggested_postcode":"",
                    "suggested_city":"",
                    "suggested_country": incoming_country,
                    "suggested_phone_or_fax":"",
                    "suggested_email_or_website":"",
                    "suggested_national_id":"",
                    "suggested_state":"",
                    "suggested_address_type":""
                }

                print("ENSID BEFORE-------", incoming_ens_id)
                processed_ens_id, duplicate = await check_and_update_unique_value(
                    table_name="upload_supplier_master_data",
                    column_name="suggested_bvd_id",
                    bvd_id_to_check="",
                    ens_id=incoming_ens_id,
                    session=session
                )
                incoming_ens_id = processed_ens_id
                print("ENS ID AFTER", incoming_ens_id)
                if duplicate["status"] == "unique":
                    updated_data["pre_existing_bvdid"]=False
                elif duplicate["status"] == "duplicate":
                    updated_data["pre_existing_bvdid"]=True

                api_response = {
                    "ens_id": incoming_ens_id,
                    "L2_verification": "Required",
                    "L2_confidence": f"{metric * 100:.2f}",
                    "verification_details": updated_data,
                    "comments":"There is highly unlikely data in the orbis match json."
                }

            # Update database
            update_status = await update_dynamic_ens_data("upload_supplier_master_data", updated_data, ens_id=incoming_ens_id, session_id=data["session_id"], session=session)
            api_response["status"] = "Updated in DB" if update_status["status"]=="success" else "Failed to update DB"

            # Append results for this supplier
            results.append(api_response)

        else:
            log.info("[SNV] == Bypassing L2 Validation == ")
            # This will work if there is only 1 record at a time where Hint is "Selected" or 1 high scoring potential match 
            temp = supplier_data[0]  # Extract the first entry
            # TEMP LOGIC TO IMPROVE MATCHES
            for match in supplier_data:
                print(national_id)
                print(str(match.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')))
                if str(match.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')) == national_id:
                    print("THIS MATCH -------------")
                    temp = match

            print(json.dumps(temp, indent=2))

            updated_data = {
                "validation_status": ValidationStatus.VALIDATED,
                "orbis_matched_status": OribisMatchStatus.MATCH,
                "truesight_status": TruesightStatus.NOT_REQUIRED,
                "truesight_percentage":0,
                "matched_percentage": temp.get('MATCH', {}).get('0', {}).get('SCORE', 0),
                "bvd_id": str(temp.get('BVDID', 'N/A')),
                "name": str(temp.get('MATCH', {}).get('0', {}).get('NAME', 'N/A')),
                "address": str(temp.get('MATCH', {}).get('0', {}).get('ADDRESS', 'N/A')),
                "name_international": str(temp.get('MATCH', {}).get('0', {}).get('NAME_INTERNATIONAL', 'N/A')),
                "postcode": str(temp.get('MATCH', {}).get('0', {}).get('POSTCODE', 'N/A')),
                "city": str(temp.get('MATCH', {}).get('0', {}).get('CITY', 'N/A')),
                "country": str(temp.get('MATCH', {}).get('0', {}).get('COUNTRY', 'N/A')),
                "phone_or_fax": str(temp.get('MATCH', {}).get('0', {}).get('PHONEORFAX', 'N/A')),
                "email_or_website": str(temp.get('MATCH', {}).get('0', {}).get('EMAILORWEBSITE', 'N/A')),
                "national_id": str(temp.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')),
                "state": str(temp.get('MATCH', {}).get('0', {}).get('STATE', 'N/A')),
                "address_type": str(temp.get('MATCH', {}).get('0', {}).get('ADDRESS_TYPE', 'N/A')),
                # TODO: Temp fix for bulk accept 
                "suggested_name": str(temp.get('MATCH', {}).get('0', {}).get('NAME', 'N/A')),
                "suggested_address": str(temp.get('MATCH', {}).get('0', {}).get('ADDRESS', 'N/A')),
                "suggested_name_international": str(temp.get('MATCH', {}).get('0', {}).get('NAME_INTERNATIONAL', 'N/A')),
                "suggested_postcode": str(temp.get('MATCH', {}).get('0', {}).get('POSTCODE', 'N/A')),
                "suggested_city": str(temp.get('MATCH', {}).get('0', {}).get('CITY', 'N/A')),
                "suggested_country": str(temp.get('MATCH', {}).get('0', {}).get('COUNTRY', 'N/A')),
                "suggested_phone_or_fax": str(temp.get('MATCH', {}).get('0', {}).get('PHONEORFAX', 'N/A')),
                "suggested_email_or_website": str(temp.get('MATCH', {}).get('0', {}).get('EMAILORWEBSITE', 'N/A')),
                "suggested_national_id": str(temp.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')),
                "suggested_state": str(temp.get('MATCH', {}).get('0', {}).get('STATE', 'N/A')),
                "suggested_address_type": str(temp.get('MATCH', {}).get('0', {}).get('ADDRESS_TYPE', 'N/A'))
            }

            print("ENSID BEFORE-------", incoming_ens_id)
            processed_ens_id, duplicate = await check_and_update_unique_value(
                table_name="upload_supplier_master_data",
                column_name="bvd_id",
                bvd_id_to_check=temp.get('BVDID', 'N/A'),
                ens_id=incoming_ens_id,
                session=session
            )
            incoming_ens_id = processed_ens_id
            print("ENS ID AFTER", incoming_ens_id)
            if duplicate["status"] == "unique":
                updated_data["pre_existing_bvdid"]=False
            elif duplicate["status"] == "duplicate":
                updated_data["pre_existing_bvdid"]=True

            api_response = {
                "ens_id": incoming_ens_id,
                "L2_verification": "Not Required",
                "L2_confidence": None,
                "verification_details": updated_data
            }

            update_status = await update_dynamic_ens_data("upload_supplier_master_data", updated_data, ens_id=incoming_ens_id, session_id=session_id, session=session)
            api_response["status"] = "Updated in DB" if update_status["status"]=="success" else "Failed to update DB"
            
            results.append(api_response)

            log.info(f"[SNV] Process completed for {incoming_ens_id}")

        return True, results

    except Exception as e:

        if not matched and not potential_pass:
            api_response = {
                    "ens_id": incoming_ens_id,
                    "L2_verification": TruesightStatus.NOT_REQUIRED,
                    "L2_confidence": None,
                    "verification_details": None,
                    "error": "SupplierNameValidation - {e}"
                }
        else:
            api_response = {
                    "ens_id": incoming_ens_id,
                    "L2_verification": TruesightStatus.VALIDATED, # TODO: NEED TO BE REQUIRED INSTEAD OF VALIDATED
                    "L2_confidence": None,
                    "verification_details": None,
                    "error": f"SupplierNameValidation - {e}"
                }
            
        results.append(api_response)
        log.info(f"[SNV] Process completed for {incoming_ens_id}")
        return False, results

