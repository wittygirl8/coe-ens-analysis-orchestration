import asyncio
import requests
from datetime import datetime
from app.core.utils.db_utils import *
import os
import json
from app.core.config import get_settings

async def newsscreening_main_company(data, session):
    print("Performing News Analysis...")

    kpi_template = {
        "kpi_area": "News Screening",
        "kpi_code": "",
        "kpi_definition": "",
        "kpi_flag": False,
        "kpi_value": None,
        "kpi_rating": "",
        "kpi_details": ""
    }

    NWS1A = kpi_template.copy()
    NWS1B = kpi_template.copy()
    NWS1C = kpi_template.copy()
    NWS1D = kpi_template.copy()
    NWS1E = kpi_template.copy()

    NWS1A["kpi_code"] = "NWS1A"
    NWS1A["kpi_definition"] = "Adverse Media - General"

    NWS1B["kpi_code"] = "NWS1B"
    NWS1B["kpi_definition"] = "Adverse Media - Business Ethics / Reputational Risk / Code of Conduct"

    NWS1C["kpi_code"] = "NWS1C"
    NWS1C["kpi_definition"] = "Bribery / Corruption / Fraud"

    NWS1D["kpi_code"] = "NWS1D"
    NWS1D["kpi_definition"] = "Regulatory Actions"

    NWS1E["kpi_code"] = "NWS1E"
    NWS1E["kpi_definition"] = "Adverse Media - Other Criminal Activity"

    ens_id = data.get("ens_id")
    session_id = data.get("session_id")

    required_columns = ["name", "country"]
    retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id, session_id, session)
    retrieved_data = retrieved_data[0]

    name = retrieved_data.get("name")
    country = retrieved_data.get("country")

    news_url = get_settings().urls.news_backend
    url = f"{news_url}/items/news_ens_data"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    data = {
        "name": name,
        "flag": "Entity",
        "company": "",
        "domain": [""],
        "start_date": "2020-01-01",
        "end_date": "2025-02-01",
        "country": country,
        "request_type": "single"
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        news_data = response.json().get("data", [])
        sentiment_aggregation = response.json().get("sentiment-data-agg", [])
    else:
        print("Error fetching news data:", response.text)
        news_data = []

    # Get current year for filtering
    current_year = datetime.now().year

    for record in news_data:
        sentiment = record.get("sentiment", "").lower()
        news_date = record.get("date", "")
        category = record.get("category", "").strip().lower()

        # Ensure sentiment is "Negative" and date is within the last 5 years
        if sentiment == "negative" and news_date:
            try:
                news_date_obj = datetime.strptime(news_date, "%Y-%m-%d")
                news_time_period = current_year - news_date_obj.year

                if news_time_period <= 5:
                    # Determine which KPI to update based on category
                    if category == "general":
                        kpi = NWS1A
                    elif category == "adverse media - business ethics / reputational risk / code of conduct":
                        kpi = NWS1B
                    elif category == "bribery / corruption / fraud":
                        kpi = NWS1C
                    elif category == "regulatory":
                        kpi = NWS1D
                    elif category == "adverse media - other criminal activity":
                        kpi = NWS1E
                    else:
                        continue

                    # Update KPI details
                    kpi["kpi_flag"] = True
                    if kpi["kpi_value"] is None:
                        kpi["kpi_value"] = record["title"]
                    else:
                        kpi["kpi_value"] += f"; {record['title']}"

                    kpi["kpi_rating"] = "High"
                    kpi["kpi_details"] = f"Negative news found in category '{kpi['kpi_definition']}' ({category}) dated {news_date}."

            except ValueError:
                continue

    kpi_list = [NWS1A, NWS1B, NWS1C, NWS1D, NWS1E]

    # TBD: Do we insert blank KPIs as well - currently using
    insert_status = await upsert_kpi("news", kpi_list, ens_id, session_id, session)
    print(insert_status)

    # Prepare data for database insertion
    columns_data =[{
        "sentiment_aggregation": sentiment_aggregation
    }]
    print(columns_data)
    # Update the database with the JSON data
    await insert_dynamic_ens_data("report_plot", columns_data, ens_id, session_id, session)

    print(f"Stored in the database")

    print("Performing News Screening Analysis for Company... Completed")

    return {"ens_id": ens_id, "module": "NEWS", "status": "completed"}


async def orbis_news_analysis(data, session):
    print("Performing Adverse Media Analysis - ONF...")

    kpi_area_module = "ONF"

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    try:

        kpi_template = {
            "kpi_area": kpi_area_module,
            "kpi_code": "",
            "kpi_definition": "",
            "kpi_flag": False,
            "kpi_value": None,
            "kpi_rating": "",
            "kpi_details": ""
        }

        ONF1A = kpi_template.copy()

        ONF1A["kpi_code"] = "ONF1A"
        ONF1A["kpi_definition"] = "Other News Findings"

        onf_kpis = []
        required_columns = ["orbis_news"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]
        print("no of data:", len(retrieved_data))
        onf = retrieved_data.get("orbis_news", None)


        if onf is None:
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}



        unique_onf=set()
        i = j = 0
        if len(onf) > 0:
            onf_events = []
            risk_rating_trigger = False
            onf_events_detail = "Other News Findings are as follows:\n"
            for event in onf:
                key = (event.get("DATE"),event.get("TITLE"))
                if key in unique_onf:
                    continue
                unique_onf.add(key)
                event_dict = {
                    "date": event.get("DATE", "Unavailable"),
                    "title": event.get("TITLE", ""),
                    "article": truncate_string(event.get("ARTICLE", "")),
                    "topic": event.get("TOPIC", ""),
                    "source": event.get("SOURCE", ""),
                    "publication": event.get("PUBLICATION", "")
                }
                current_year = datetime.now().year
                try:
                    event_date = datetime.strptime(event.get("DATE")[:10], "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                text = f"{i+1}. {event.get('TITLE')}: {event.get('TOPIC')} - {truncate_string(event.get("ARTICLE"))} (Date: {event.get("DATE")[:10]})\n"
                onf_events.append(event_dict)
                onf_events_detail += text
                i+=1
                if i+j>=5:
                    break
            kpi_value_overall_dict = {
                "count": len(onf_events) if len(onf_events) < 6 else "5 or more",
                "target": "organization",  # Since this is person level
                "findings": onf_events,
                "themes": [a.get("TOPIC") for a in onf_events]
            }
            ONF1A["kpi_flag"] = True
            ONF1A["kpi_value"] = json.dumps(kpi_value_overall_dict)
            ONF1A["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            ONF1A["kpi_details"] = onf_events_detail

            onf_kpis.append(ONF1A)
            print("onf_kpi:", ONF1A)

        insert_status = await upsert_kpi("news", onf_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure",
                    "info": "database_saving_error"}
    except Exception as e:
        print(f"Error in module: {kpi_area_module}:{str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}


def truncate_string(input_string, word_limit=40):
    try:
        words = input_string.split()  # Split the string into words
        truncated = " ".join(words[:word_limit])  # Get the first 'word_limit' words
        if len(words) > word_limit:
            truncated += " [...]"  # Add ellipsis if the string is longer than 'word_limit' words
        return truncated
    except:
        return input_string

# async def news_for_management(data, session):
#     print("Performing News Analysis for People...")
#
#     # Initialize KPI template
#     kpi_template = {
#         "kpi_area": "News Screening",
#         "kpi_code": "",
#         "kpi_definition": "",
#         "kpi_flag": False,
#         "kpi_value": None,
#         "kpi_rating": "",
#         "kpi_details": ""
#     }
#
#     # Initialize KPIs for management news screening
#     NWS2A = kpi_template.copy()
#     NWS2B = kpi_template.copy()
#     NWS2C = kpi_template.copy()
#     NWS2D = kpi_template.copy()
#     NWS2E = kpi_template.copy()
#
#     NWS2A["kpi_code"] = "NWS2A"
#     NWS2A["kpi_definition"] = "Adverse Media - General"
#
#     NWS2B["kpi_code"] = "NWS2B"
#     NWS2B["kpi_definition"] = "Adverse Media - Business Ethics / Reputational Risk / Code of Conduct"
#
#     NWS2C["kpi_code"] = "NWS2C"
#     NWS2C["kpi_definition"] = "Bribery / Corruption / Fraud"
#
#     NWS2D["kpi_code"] = "NWS2D"
#     NWS2D["kpi_definition"] = "Regulatory Actions"
#
#     NWS2E["kpi_code"] = "NWS2E"
#     NWS2E["kpi_definition"] = "Adverse Media - Other Criminal Activity"
#
#     ens_id_value = data.get("ens_id")
#     session_id_value = data.get("session_id")
#
#     required_columns = ["name", "country", "management", "controlling_shareholders"]
#     retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
#     retrieved_data = retrieved_data[0]
#
#     name = retrieved_data.get("name")
#     country = retrieved_data.get("country")
#     management = retrieved_data.get("management", [])
#     controlling_shareholders = retrieved_data.get("controlling_shareholders", [])
#
#     # Initialize overall lists for each category
#     overall_general_list = []
#     overall_amr_list = []
#     overall_bribery_list = []
#     overall_regulatory_list = []
#     overall_amo_list = []
#
#     current_year = datetime.now().year
#
#     # Function to process news data for a person
#     async def process_person_news(person_name):
#         url = "http://127.0.0.1:8001/items/news_ens_data"
#         headers = {
#             "accept": "application/json",
#             "Content-Type": "application/json"
#         }
#         data = {
#             "name": person_name,
#             "flag": "POI",
#             "company": name,
#             "domain": [""],
#             "start_date": "2020-01-01",
#             "end_date": "2025-02-01",
#             "country": country,
#             "request_type": "single"
#         }
#
#         response = requests.post(url, headers=headers, json=data)
#
#         if response.status_code == 200:
#             return response.json().get("data", [])
#         else:
#             print(f"Error fetching news data for {person_name}:", response.text)
#             return []
#
#     # Process management individuals with any indicator
#     for person in management:
#         if any(indicator == "Yes" for indicator in [person.get("pep_indicator"), person.get("media_indicator"), person.get("sanctions_indicator"), person.get("watchlist_indicator")]):
#             person_name = person.get("name")
#             news_data = await process_person_news(person_name)
#
#             # Initialize person-specific lists
#             persons_general = []
#             persons_amr = []
#             persons_bribery = []
#             persons_regulatory = []
#             persons_amo = []
#
#             for record in news_data:
#                 sentiment = record.get("sentiment", "").lower()
#                 news_date = record.get("date", "")
#                 category = record.get("category", "").strip().lower()
#
#                 # Ensure sentiment is "Negative" and date is within the last 5 years
#                 if sentiment == "negative" and news_date:
#                     try:
#                         news_date_obj = datetime.strptime(news_date, "%Y-%m-%d")
#                         news_time_period = current_year - news_date_obj.year
#
#                         if news_time_period <= 5:
#                             if category == "general":
#                                 persons_general.append(record)
#                             elif category == "adverse media - business ethics / reputational risk / code of conduct":
#                                 persons_amr.append(record)
#                             elif category == "bribery / corruption / fraud":
#                                 persons_bribery.append(record)
#                             elif category == "regulatory":
#                                 persons_regulatory.append(record)
#                             elif category == "adverse media - other criminal activity":
#                                 persons_amo.append(record)
#
#                     except ValueError:
#                         continue
#
#             # Append person-specific results to overall lists
#             overall_general_list.extend(persons_general)
#             overall_amr_list.extend(persons_amr)
#             overall_bribery_list.extend(persons_bribery)
#             overall_regulatory_list.extend(persons_regulatory)
#             overall_amo_list.extend(persons_amo)
#
#     # Process controlling shareholders with ownership > 50%
#     for csh in controlling_shareholders:
#         if csh.get("CSH_ENTITY_TYPE") == "One or more named individuals or families" and csh.get("total_ownership", 0) > 50:
#             person_name = csh.get("name")
#             news_data = await process_person_news(person_name)
#
#             # Initialize person-specific lists
#             persons_general = []
#             persons_amr = []
#             persons_bribery = []
#             persons_regulatory = []
#             persons_amo = []
#
#             for record in news_data:
#                 sentiment = record.get("sentiment", "").lower()
#                 news_date = record.get("date", "")
#                 category = record.get("category", "").strip().lower()
#
#                 # Ensure sentiment is "Negative" and date is within the last 5 years
#                 if sentiment == "negative" and news_date:
#                     try:
#                         news_date_obj = datetime.strptime(news_date, "%Y-%m-%d")
#                         news_time_period = current_year - news_date_obj.year
#
#                         if news_time_period <= 5:
#                             if category == "general":
#                                 persons_general.append(record)
#                             elif category == "adverse media - business ethics / reputational risk / code of conduct":
#                                 persons_amr.append(record)
#                             elif category == "bribery / corruption / fraud":
#                                 persons_bribery.append(record)
#                             elif category == "regulatory":
#                                 persons_regulatory.append(record)
#                             elif category == "adverse media - other criminal activity":
#                                 persons_amo.append(record)
#
#                     except ValueError:
#                         continue
#
#             # Append person-specific results to overall lists
#             overall_general_list.extend(persons_general)
#             overall_amr_list.extend(persons_amr)
#             overall_bribery_list.extend(persons_bribery)
#             overall_regulatory_list.extend(persons_regulatory)
#             overall_amo_list.extend(persons_amo)
#
#     # Update KPI
#     if overall_general_list:
#         NWS2A["kpi_flag"] = True
#         NWS2A["kpi_value"] = "; ".join([record["title"] for record in overall_general_list])
#         NWS2A["kpi_rating"] = "High"
#         NWS2A["kpi_details"] = f"Negative news found within last 5 years for {len(overall_general_list)} people"
#
#     if overall_amr_list:
#         NWS2B["kpi_flag"] = True
#         NWS2B["kpi_value"] = "; ".join([record["title"] for record in overall_amr_list])
#         NWS2B["kpi_rating"] = "High"
#         NWS2B["kpi_details"] = f"Negative news found within last 5 years for {len(overall_amr_list)} people"
#
#     if overall_bribery_list:
#         NWS2C["kpi_flag"] = True
#         NWS2C["kpi_value"] = "; ".join([record["title"] for record in overall_bribery_list])
#         NWS2C["kpi_rating"] = "High"
#         NWS2C["kpi_details"] = f"Negative news found within last 5 years for {len(overall_bribery_list)} people"
#
#     if overall_regulatory_list:
#         NWS2D["kpi_flag"] = True
#         NWS2D["kpi_value"] = "; ".join([record["title"] for record in overall_regulatory_list])
#         NWS2D["kpi_rating"] = "High"
#         NWS2D["kpi_details"] = f"Negative news found within last 5 years for {len(overall_regulatory_list)} people"
#
#     if overall_amo_list:
#         NWS2E["kpi_flag"] = True
#         NWS2E["kpi_value"] = "; ".join([record["title"] for record in overall_amo_list])
#         NWS2E["kpi_rating"] = "High"
#         NWS2E["kpi_details"] = f"Negative news found within last 5 years for {len(overall_amo_list)} people"
#
#     # Prepare KPI list for upsert
#     news_screening_kpis = [NWS2A, NWS2B, NWS2C, NWS2D, NWS2E]
#
#     # Upsert KPIs
#     insert_status = await upsert_kpi("news", news_screening_kpis, ens_id_value, session_id_value, session)
#     print(insert_status)
#
#     print("Performing News Screening Analysis for People... Completed")
#     return {"ens_id": ens_id_value, "module": "NEWS", "status": "completed"}