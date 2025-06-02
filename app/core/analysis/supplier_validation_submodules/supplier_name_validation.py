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
from app.core.config import get_settings
from itertools import groupby
from operator import itemgetter
from app.schemas.logger import logger
load_dotenv()


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

    incoming_address = data.get("uploaded_address", "") if len(str(data.get("uploaded_address", "")))>0 else "None"
    incoming_city = data.get("uploaded_city", "") if len(str(data.get("uploaded_city", "")))>0 else "None"
    incoming_postcode = data.get("uploaded_postcode", "") if len(str(data.get("uploaded_postcode", "")))>0 else "None"
    incoming_email = data.get("uploaded_email_or_website", "") if len(str(data.get("uploaded_email_or_website", "")))>0 else "None"
    incoming_p_o_f = data.get("uploaded_phone_or_fax", "") if len(str(data.get("uploaded_phone_or_fax", "")))>0 else "None"
    incoming_state = data.get("uploaded_state", "") if len(str(data.get("uploaded_state", "")))>0 else "None"

    logger.info("================================================")
    logger.info(f"[SNV] ens_id = {incoming_ens_id}")

    def get_possible_suppliers(payload, static_case=None):
        try:
            # Generate JWT token
            jwt_token = create_jwt_token("orchestration", "analysis")
            logger.debug(f"TOKEN: {jwt_token}")
        except Exception as e:
            logger.error("Error generating JWT token: %s", e)
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
                response = requests.get(url, headers=headers)
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
                matched_supplier_data, potential_pass, matched = filter_supplier_data(response_json, payload["nationalId"], max_results=2)

                return matched_supplier_data, potential_pass, matched

            except requests.exceptions.RequestException as e:
                raise RuntimeError(f"Failed to fetch supplier data: {e}")

        else:
            # Running cases (loading a local JSON file)
            potentials = r"app\core\analysis\supplier_validation_submodules\files\response_potential_case.json"
            with open(potentials, 'r', encoding='utf-8') as file:
                case_A = json.load(file)

            matched_supplier_data, potential_pass, matched = filter_supplier_data(case_A, "", max_results=2)
            return matched_supplier_data, potential_pass, matched

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

    matched_supplier_data, potential_pass, matched = get_possible_suppliers(match_payload, static_case=False)
 
    try:

        if not matched and not potential_pass:

            logger.warning("------- NO ORBIS MATCH FOUND for %s ", incoming_name)

            agg_verified = "No"
            metric = 0
            agg_verified, agg_output, metric = await truesight_l2_validation(incoming_ens_id, incoming_country, incoming_name, search_engine)

            # Map the boolean value to the appropriate TruesightStatus enum value
            if agg_verified == "Yes":
                truesight_status_value = TruesightStatus.VALIDATED
            elif agg_verified == "No":
                truesight_status_value = TruesightStatus.NOT_VALIDATED

            # Update data for the current supplier - current L2 not functional
            updated_data = {
                "validation_status": ValidationStatus.NOT_VALIDATED,
                "orbis_matched_status": OribisMatchStatus.NO_MATCH,
                "truesight_status": TruesightStatus.NO_MATCH,
                "final_validation_status": FinalValidatedStatus.AUTO_REJECT,
                "matched_percentage": 0,
                "suggested_bvd_id": "",
                "truesight_percentage": int(round(metric * 100, 2)),
                "suggested_name": incoming_name,
                "suggested_address": "",
                "suggested_name_international": "",
                "suggested_postcode": "",
                "suggested_city": "",
                "suggested_country": incoming_country,
                "suggested_phone_or_fax": "",
                "suggested_email_or_website": "",
                "suggested_national_id": "",
                "suggested_state": "",
                "suggested_address_type": ""
            }

            updated_data["pre_existing_bvdid"] = False

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
            logger.info(" ------- ORBIS MATCH IDENTIFIED: ")

            logger.info(json.dumps(matched_supplier_data, indent=2))

            final_validation_status = FinalValidatedStatus.REVIEW
            if matched:
                logger.info(f"[SNV] Matched Status (Direct Match): {matched} with score {matched_supplier_data.get('MATCH', {}).get('0', {}).get('SCORE', 0)}")
                final_validation_status = FinalValidatedStatus.AUTO_ACCEPT
            elif potential_pass:
                logger.info(f"[SNV] Potential pass (Needs Review): {potential_pass} with score {matched_supplier_data.get('MATCH', {}).get('0', {}).get('SCORE', 0)}")
                final_validation_status = FinalValidatedStatus.REVIEW


            updated_data = {
                "validation_status": ValidationStatus.VALIDATED,
                "orbis_matched_status": OribisMatchStatus.MATCH,
                "truesight_status": TruesightStatus.NOT_REQUIRED,
                "final_validation_status":final_validation_status,
                "truesight_percentage":0,
                "matched_percentage": matched_supplier_data.get('MATCH', {}).get('0', {}).get('SCORE', 0) * 100,
                "bvd_id": str(matched_supplier_data.get('BVDID', 'N/A')),
                "name": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('NAME', 'N/A')),
                "address": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('ADDRESS', 'N/A')),
                "name_international": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('NAME_INTERNATIONAL', 'N/A')),
                "postcode": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('POSTCODE', 'N/A')),
                "city": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('CITY', 'N/A')),
                "country": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('COUNTRY', 'N/A')),
                "phone_or_fax": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('PHONEORFAX', 'N/A')),
                "email_or_website": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('EMAILORWEBSITE', 'N/A')),
                "national_id": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')),
                "state": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('STATE', 'N/A')),
                "address_type": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('ADDRESS_TYPE', 'N/A')),
                "suggested_name": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('NAME', 'N/A')),
                "suggested_address": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('ADDRESS', 'N/A')),
                "suggested_name_international": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('NAME_INTERNATIONAL', 'N/A')),
                "suggested_postcode": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('POSTCODE', 'N/A')),
                "suggested_city": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('CITY', 'N/A')),
                "suggested_country": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('COUNTRY', 'N/A')),
                "suggested_phone_or_fax": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('PHONEORFAX', 'N/A')),
                "suggested_email_or_website": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('EMAILORWEBSITE', 'N/A')),
                "suggested_national_id": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')),
                "suggested_state": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('STATE', 'N/A')),
                "suggested_address_type": str(matched_supplier_data.get('MATCH', {}).get('0', {}).get('ADDRESS_TYPE', 'N/A'))
            }

            # Checking for pre-existing BVD-ID
            logger.info("ENSID BEFORE------- %s", incoming_ens_id)
            processed_ens_id, duplicate = await check_and_update_unique_value(
                table_name="upload_supplier_master_data",
                column_name="bvd_id",
                bvd_id_to_check=matched_supplier_data.get('BVDID', 'N/A'),
                ens_id=incoming_ens_id,
                session=session
            )

            # Assign ens_id as the processed one (pre-existing if found)
            incoming_ens_id = processed_ens_id
            logger.info("ENS ID AFTER %s", incoming_ens_id)
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

            # Update Data To Table
            update_status = await update_dynamic_ens_data("upload_supplier_master_data", updated_data, ens_id=incoming_ens_id, session_id=session_id, session=session)
            api_response["status"] = "Updated in DB" if update_status["status"]=="success" else "Failed to update DB"
            
            results.append(api_response)

            logger.info(f"[SNV] Process completed for {incoming_ens_id}")

        return True, results

    except Exception as e:

        logger.error(f"Supplier Name Validation Failed - {str(e)}")
        raise


async def ensid_duplicate_in_session(session_id, session):

    logger.debug("---- STARTING DUPLICATION CHECK")

    data_for_sessionId = await get_dynamic_ens_data("upload_supplier_master_data", required_columns=["all"],ens_id=None, session_id=session_id, session=session)

    data_for_sessionId = [entry for entry in data_for_sessionId if (entry.get( "validation_status","")!=ValidationStatus.NOT_VALIDATED.value)]
    # Sort the data by the key(s) you want to group by
    data_for_sessionId.sort(key=itemgetter('ens_id'))

    grouped = groupby(data_for_sessionId, key=itemgetter('ens_id'))

    for ens_id, group in grouped:

        group_list = list(group)

        # TAKE ONLY CASES WHERE MORE THAN ONE ENTRY FOR SAME ENS ID
        if len(group_list) < 2:
            continue

        # Find all entries with national id match
        national_id_entries = [entry for entry in group_list if entry['national_id'] == entry['uploaded_national_id']]

        # Initialize a flag to track if 'maximum' has been assigned within this group
        max_assigned = False

        if national_id_entries:
            # Find the national id match with the maximum score
            national_id_top_match = max(national_id_entries, key=itemgetter('matched_percentage'))['matched_percentage']

            # Assign 'maximum' to the entry with the highest score, and 'other' to the others
            for entry in national_id_entries:
                if not max_assigned and entry['matched_percentage'] == national_id_top_match:
                    update_entry = {"duplicate_in_session": DUPINSESSION.RETAIN}
                    res = await update_for_ensid_svm_duplication(update_entry, entry["id"],session_id, session)
                    max_assigned = True  # Ensure only one maximum is assigned
                else:
                    update_entry = {"duplicate_in_session":  DUPINSESSION.REMOVE, "final_validation_status": FinalValidatedStatus.AUTO_REJECT}
                    res = await update_for_ensid_svm_duplication(update_entry, entry["id"], session_id, session)

            # For all other entries in the group, assign 'other'
            for entry in group_list:
                if entry['national_id'] != entry['uploaded_national_id']:
                    update_entry = {"duplicate_in_session":  DUPINSESSION.REMOVE, "final_validation_status": FinalValidatedStatus.AUTO_REJECT}
                    res = await update_for_ensid_svm_duplication(update_entry, entry["id"], session_id, session)

        else:
            # If no national id entries, just assign the maximum
            top_match = max(group_list, key=itemgetter('matched_percentage'))['matched_percentage']

            for entry in group_list:
                if entry['matched_percentage'] == top_match and not max_assigned:
                    update_entry = {"duplicate_in_session": DUPINSESSION.RETAIN}
                    res = await update_for_ensid_svm_duplication(update_entry, entry["id"],session_id, session)
                    max_assigned = True  # Ensure only one maximum is assigned
                else:
                    update_entry = {"duplicate_in_session":  DUPINSESSION.REMOVE, "final_validation_status": FinalValidatedStatus.AUTO_REJECT}
                    res = await update_for_ensid_svm_duplication(update_entry, entry["id"], session_id, session)

    return


async def truesight_l2_validation(incoming_ens_id, incoming_country, incoming_name, search_engine):

    try:
        logger.warning("[SNV]  == Performing L2 Validation == ")
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
                logger.warning(f"[SNV] TS analysis: {analysis}")

            # Aggregate results
            agg_output = aggregate_verified_flag(analysis)
            logger.warning(f"[SNV] Aggregated verified Flag: {agg_output}")
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

        # TODO: Truesight will make an api call back to orbis once it finds that entity's unique identifier on the web, but for now: we say [no match - no match]
        agg_verified = agg_output['verified']

        return agg_verified,agg_output,metric

    except Exception as e:

        logger.warning(str(e))

        agg_output = {
            "num_yes": 0,
            "num_analysed": 0,
            "ens_id": incoming_ens_id,
            "verified": "No",
            "token_usage": None
        }
        metric = 0.0
        agg_verified = agg_output['verified']

        return agg_verified, agg_output, metric