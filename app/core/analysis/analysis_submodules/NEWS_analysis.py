import asyncio
import requests
from datetime import datetime
from app.core.utils.db_utils import *
import os
import json
from app.core.config import get_settings

import requests
from datetime import datetime


async def newsscreening_main_company(data, session):
    print("Performing News Analysis...")
    kpi_area_module = "NWS"

    kpi_template = {
        "kpi_area": kpi_area_module,
        "kpi_code": "",
        "kpi_definition": "",
        "kpi_flag": False,
        "kpi_value": None,
        "kpi_rating": "",
        "kpi_details": ""
    }

    NWS1A = kpi_template.copy()
    NWS1A["kpi_code"] = "NWS1A"
    NWS1A["kpi_definition"] = "Adverse Media - Additional Screening"

    ens_id = data.get("ens_id")
    session_id = data.get("session_id")

    required_columns = ["name", "country"]
    retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id, session_id, session)
    retrieved_data = retrieved_data[0]

    name = retrieved_data.get("name")
    country = retrieved_data.get("country")
    print("checkpoint 1")

    news_url = get_settings().urls.news_backend
    url = f"{news_url}/items/news_ens_data"
    print("url:", url)

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }

    total_news = 0
    current_year = 2025
    min_year = 2020
    news_data = []
    response=[]
    while total_news < 5 and current_year >= min_year:
        start_date = f"{current_year}-01-01"
        end_date = f"{current_year}-12-31"

        data = {
            "name": name,
            "flag": "Entity",
            "company": "",
            "domain": [""],
            "start_date": start_date,
            "end_date": end_date,
            "country": country,
            "request_type": "single"
        }

        response = requests.post(url, headers=headers, json=data)
        print(f"Checking news for year {current_year}...")

        if response.status_code == 200:
            year_news = response.json().get("data", [])
            print(f"Found {len(year_news)} news articles for {current_year}")
            if isinstance(year_news, list):
                print("data is a list")
                if len(year_news)>0:
                    valid_or_not = year_news[0].get("link", 'N/A')
                else:
                    valid_or_not = 'N/A'
            else:
                valid_or_not = 'N/A'
            if valid_or_not == 'N/A':
                print("link is not present skipping")
                current_year -= 1
                continue
            news_data.extend(year_news)
            total_news += len(year_news)

            if total_news >= 5:
                break
        else:
            print(f"Error fetching news for {current_year}: {response.status_code}")

        current_year -= 1  # Move to the previous year

    print(f"Total news collected: {total_news}")

    if not news_data:
        print("No relevant news found.")
        return {"ens_id": ens_id, "module": "NEWS", "status": "completed"}

    # Process the collected news
    NWS1A["kpi_details"] = "Following Additional Screening:\n"
    NWS1A["kpi_value"] = ''

    current_year = datetime.now().year  # Get current year for filtering

    for i, record in enumerate(news_data):
        sentiment = record.get("sentiment", "").lower()
        news_date = record.get("date", "")
        category = record.get("category", "").strip().lower()
        summary = record.get("summary", "").strip()
        title = record.get("title", "").strip()
        link = record.get("link", "").strip()

        if sentiment == "negative" and news_date:
            try:
                news_date_obj = datetime.strptime(news_date, "%Y-%m-%d")
                news_time_period = current_year - news_date_obj.year

                if news_time_period <= 5:
                    # Categorize the news item
                    category_map = {
                        "general": "Adverse Media finding",
                        "adverse media - business ethics / reputational risk / code of conduct": "Adverse Media - Other Reputational Risk",
                        "bribery / corruption / fraud": "Bribery, Fraud or Corruption",
                        "regulatory": "Regulation",
                        "adverse media - other criminal activity": "Adverse Media - Other Criminal Activities"
                    }

                    cat = category_map.get(category, None)
                    if not cat:
                        continue

                    # Update KPI
                    NWS1A["kpi_flag"] = True
                    NWS1A["kpi_value"] += f"; {title}"
                    NWS1A["kpi_rating"] = "High"
                    NWS1A[
                        "kpi_details"] += f"{i + 1}. {cat}: {title} - {summary}\n Source: {link} (Date: {news_date_obj})\n"

            except ValueError:
                continue

    kpi_list = [NWS1A]
    print("kpi_list:", kpi_list)

    insert_status = await upsert_kpi("news", kpi_list, ens_id, session_id, session)

    columns_data = [{
        "sentiment_aggregation": response.json().get("sentiment-data-agg", [])
    }]
    print(columns_data)

    await insert_dynamic_ens_data("report_plot", columns_data, ens_id, session_id, session)

    print("Stored in the database")
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
                text = f"{i+1}. {event.get('TITLE')}: {event.get('TOPIC')} - {truncate_string(event.get('ARTICLE'))} \n Source:{event.get('SOURCE', 'N/A')} | Publication: {event.get('PUBLICATION', 'N/A')} | (Date: {event.get('DATE')[:10]})\n"
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