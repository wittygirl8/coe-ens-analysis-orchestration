import json
from datetime import datetime
from app.core.utils.db_utils import *

async def legal_analysis(data, session):

    print("Performing Legal Analysis...")

    kpi_area_module = "LEG"

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

        LEG1A = kpi_template.copy()

        LEG1A["kpi_code"] = "LEG1A"
        LEG1A["kpi_definition"] = "Legal Event - Organisation Level"

        # Data for Org-Level
        required_columns = ["legal", "grid_legal"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        legal = retrieved_data.get("legal", None)
        grid_legal = retrieved_data.get("grid_legal", None)

        # Data for Person-Level
        required_columns = ["grid_legal"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        person_retrieved_data = retrieved_data  # Multiple rows/people per ens_id and session_id

        # Check if all person data is blank
        person_info_none = all(person.get("grid_legal", None) is None for person in person_retrieved_data)

        if person_info_none and (legal is None) and (grid_legal is None):
            print(f"{kpi_area_module} Analysis... Completed With No Data")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        # ---------------- LEG1A - Legal Event - Organisation Level
        leg_data = (legal or []) + (grid_legal or [])

        LEG1A["kpi_value"] = json.dumps(leg_data)

        high_risk_rating_trigger = False
        all_events_detail = []
        for event in leg_data:  # TODO Sort this by date descending and take get the first 5
            LEG1A["kpi_flag"] = True  # True if any event found
            current_year = datetime.now().year
            try:
                event_date = datetime.strptime(event.get("eventDt"), "%Y-%m-%d")
                event_year = current_year - event_date.year
                if event_year <= 5:
                    high_risk_rating_trigger = True
            except:
                event_date = "Unavailable"
            text = f"""
                    {event.get("category")} - {event.get("categoryDesc")} (Date: {event.get("eventDt")})
                    \n
                    {truncate_string(event.get("eventDesc"))}
                    \n
                """
            all_events_detail.append(text)


        LEG1A["kpi_rating"] = "High" if high_risk_rating_trigger else "Low"
        LEG1A["kpi_details"] = json.dumps(all_events_detail)

        # ---------------- LEG1B - Legal Event - Person  # TODO PENDING

        legal_kpis = [LEG1A]

        insert_status = await upsert_kpi("lgrk", legal_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure","info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}: {str(e)}")
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