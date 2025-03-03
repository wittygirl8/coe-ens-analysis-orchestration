from datetime import datetime
import asyncio
from app.core.utils.db_utils import *
import json

async def adverse_media_analysis(data, session):

    print("Performing Adverse Media Analysis for Other Criminal Activities...")

    kpi_area_module = "AMO"

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

        amo_kpis = []

        AMO1A = kpi_template.copy()
        AMO1B = kpi_template.copy()

        AMO1A["kpi_code"] = "AMO1A"
        AMO1A["kpi_definition"] = "Adverse Media for Other Criminal Activities - Organization Level"

        AMO1B["kpi_code"] = "AMO1B"
        AMO1B["kpi_definition"] = "Adverse Media for Other Criminal Activities - Person Level"

        required_columns = ["event_adverse_media_other_crimes", "grid_event_adverse_media_other_crimes"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        adv = retrieved_data.get("event_adverse_media_other_crimes", None)
        grid_adv = retrieved_data.get("grid_event_adverse_media_other_crimes", None)

        # Data for Person-Level
        required_columns = ["grid_adverse_media_other_crimes"]
        retrieved_data = await get_dynamic_ens_data("grid_management", required_columns, ens_id_value, session_id_value, session)
        person_retrieved_data = retrieved_data  # Multiple rows/people per ens_id and session_id

        # Check if all person data is blank
        person_info_none = all(person.get("grid_adverse_media_other_crimes", None) is None for person in person_retrieved_data)

        if person_info_none and (adv is None) and (grid_adv is None):
            print(f"{kpi_area_module} Analysis... Completed With No Data")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        # AMO1A - Adverse Media for Other Criminal Activities - Organization Level
        adv = (adv or []) + (grid_adv or [])  # TODO: ANOTHER WAY TO COMBINE THIS INFO IF IT OVERLAPS

        if len(adv) > 0:
            current_year = datetime.now().year
            criminal_activities = []
            details = "Criminal activity discovered: \n"
            risk_rating_trigger = False
            for i, amo in enumerate(adv, start=1):
                event_desc = truncate_string(amo.get("eventDesc"))
                try:
                    event_date = datetime.strptime(amo.get("eventDt"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                category_desc = amo.get("categoryDesc")
                details += f"{i}. {category_desc} - {event_desc} (Date: {event_date})\n "

                criminal_activities.append(event_desc)

            AMO1A["kpi_flag"] = True
            AMO1A["kpi_value"] = json.dumps(criminal_activities)
            AMO1A["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            AMO1A["kpi_details"] = details

            amo_kpis.append(AMO1A)

        # --------- AMO1B - Adverse Media for Other Criminal Activities - Person Level
        all_person_amo_events = []
        if not person_info_none and len(person_retrieved_data) > 0:
            for person in person_retrieved_data:
                amo_events = person.get("grid_adverse_media_other_crimes",[])
                if amo_events is not None:
                    all_person_amo_events = all_person_amo_events + amo_events

            criminal_activities = []
            details = "Criminal activity discovered: \n"
            risk_rating_trigger = False
            current_year = datetime.now().year
            i = 0
            for amo in all_person_amo_events:
                i += 1
                event_desc = truncate_string(amo.get("eventDesc"))
                try:
                    event_date = datetime.strptime(amo.get("eventDt"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                category_desc = amo.get("categoryDesc")
                details += f"{i}. {category_desc} - {event_desc} (Date: {event_date})\n "

                criminal_activities.append(event_desc)

            AMO1B["kpi_flag"] = True
            AMO1B["kpi_value"] = json.dumps(criminal_activities)
            AMO1B["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            AMO1B["kpi_details"] = details

            amo_kpis.append(AMO1B)
        # ---------------------------------

        insert_status = await upsert_kpi("rfct", amo_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure","info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module} : {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}


async def adverse_media_reputation_risk(data, session):

    print("Performing Adverse Media Analysis for Business Ethics / Reputational Risk / Code of Conduct...")

    kpi_area_module = "AMR"

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

        AMR1A = kpi_template.copy()
        AMR1B = kpi_template.copy()

        AMR1A["kpi_code"] = "AMR1A"
        AMR1A["kpi_definition"] = "Adverse Media - Business Ethics / Reputational Risk / Code of Conduct - Organization Level"

        AMR1B["kpi_code"] = "AMR1B"
        AMR1B["kpi_definition"] = "Adverse Media - Business Ethics / Reputational Risk / Code of Conduct - Person Level"

        amr_kpis = []

        # Data for Organisation Level
        required_columns = ["event_adverse_media_reputational_risk", "grid_event_adverse_media_reputational_risk"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        adv = retrieved_data.get("event_adverse_media_reputational_risk", None)
        grid_adv = retrieved_data.get("grid_event_adverse_media_reputational_risk", None)

        # Data for Person-Level
        required_columns = ["grid_adverse_media_reputational_risk"]
        retrieved_data = await get_dynamic_ens_data("grid_management", required_columns, ens_id_value, session_id_value, session)
        person_retrieved_data = retrieved_data  # Multiple rows/people per ens_id and session_id

        # Check if all person data is blank
        person_info_none = all(person.get("grid_adverse_media_reputational_risk", None) is None for person in person_retrieved_data)

        if person_info_none and (adv is None) and (grid_adv is None):
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        # AMR1A - Adverse Media - Business Ethics / Reputational Risk / Code of Conduct - Organization Level
        adv = (adv or []) + (grid_adv or [])  # TODO: ANOTHER WAY TO COMBINE THIS INFO IF IT OVERLAPS

        if len(adv) > 0:
            current_year = datetime.now().year
            reputation_risks = []
            details = "Reputation risk due to the following events:\n"
            risk_rating_trigger = False
            for i, adv in enumerate(adv, start=1):
                event_desc = truncate_string(adv.get("eventDesc"))
                try:
                    event_date = datetime.strptime(adv.get("eventDt"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"

                category_desc = adv.get("categoryDesc")
                sub_category_desc = adv.get("subCategoryDesc")

                details += f"{category_desc}: {sub_category_desc} - {event_desc} (Date: {event_date})\n"
                reputation_risks.append(event_desc)

            AMR1A["kpi_flag"] = True
            AMR1A["kpi_value"] = json.dumps(reputation_risks)
            AMR1A["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            AMR1A["kpi_details"] = details

            amr_kpis.append(AMR1A)

        # ----- AMR1B - Adverse Media - Business Ethics / Reputational Risk / Code of Conduct - Person Level
        all_person_amr_events = []
        if not person_info_none and len(person_retrieved_data) > 0:
            for person in person_retrieved_data:
                amr_events = person.get("grid_adverse_media_reputational_risk",[])
                if amr_events is not None:
                    all_person_amr_events = all_person_amr_events + amr_events

            criminal_activities = []
            details = "Reputation risk due to the following events:\n"
            risk_rating_trigger = False
            current_year = datetime.now().year
            i = 0
            for amr in all_person_amr_events:
                i += 1
                event_desc = truncate_string(amr.get("eventDesc"))
                try:
                    event_date = datetime.strptime(amr.get("eventDt"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"

                category_desc = amr.get("categoryDesc")
                details += f"{i}. {category_desc} - {event_desc} (Date: {event_date})\n "

                criminal_activities.append(event_desc)

            AMR1B["kpi_flag"] = True
            AMR1B["kpi_value"] = json.dumps(criminal_activities)
            AMR1B["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            AMR1B["kpi_details"] = details

            amr_kpis.append(AMR1B)
        # ---------------------------------

        insert_status = await upsert_kpi("rfct", amr_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure",
                    "info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}


async def bribery_corruption_fraud_analysis(data, session):
    print("Performing Adverse Media Analysis - BCF...")

    kpi_area_module = "BCF"

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

        BCF1A = kpi_template.copy()
        BCF1B = kpi_template.copy()

        BCF1A["kpi_code"] = "BCF1A"
        BCF1A["kpi_definition"] = "Bribery, Corruption or Fraud - Organization Level"

        BCF1B["kpi_code"] = "BCF1B"
        BCF1B["kpi_definition"] = "Bribery, Corruption or Fraud - Person Level"

        bcf_kpis = []

        required_columns = ["event_bribery_fraud_corruption", "grid_event_bribery_fraud_corruption"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        bcf = retrieved_data.get("event_bribery_fraud_corruption", None)
        grid_bcf = retrieved_data.get("grid_event_bribery_fraud_corruption", None)

        # Data for Person-Level
        required_columns = ["grid_bribery_fraud_corruption"]
        retrieved_data = await get_dynamic_ens_data("grid_management", required_columns, ens_id_value, session_id_value, session)
        person_retrieved_data = retrieved_data  # Multiple rows/people per ens_id and session_id

        # Check if all person data is blank
        person_info_none = all(person.get("grid_bribery_fraud_corruption", None) is None for person in person_retrieved_data)

        if person_info_none and (bcf is None) and (grid_bcf is None):
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}


        # BCF1A - Bribery, Corruption or Fraud - Organization Level
        bcf = (bcf or []) + (grid_bcf or []) # TODO: ANOTHER WAY TO COMBINE THIS INFO IF IT OVERLAPS

        if len(bcf) > 0:
            bcf_events = []
            risk_rating_trigger = False
            bcf_events_detail = "Risk identified due to the following events:\n"
            for event in bcf:
                current_year = datetime.now().year
                try:
                    event_date = datetime.strptime(event.get("eventDt"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                text = f"""
                        {event.get("category")} - {event.get("categoryDesc")} ({event.get("eventDt")})
                        \n
                        {truncate_string(event.get("eventDesc"))}
                        \n
                    """
                bcf_events.append(truncate_string(event.get("eventDesc")))  # TODO CHANGE
                bcf_events_detail += text

            BCF1A["kpi_flag"] = True
            BCF1A["kpi_value"] = json.dumps(bcf_events)
            BCF1A["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            BCF1A["kpi_details"] = bcf_events_detail

            bcf_kpis.append(BCF1A)

        # --------- BCF1B - Bribery, Corruption or Fraud - Person Level
        all_person_bcf_events = []
        if not person_info_none and len(person_retrieved_data) > 0:
            for person in person_retrieved_data:
                bcf_events = person.get("grid_bribery_fraud_corruption",[])
                print(bcf_events)
                if bcf_events is not None:
                    all_person_bcf_events = all_person_bcf_events + bcf_events


            bcf_activities = []
            details = "Risk identified due to the following events:\n"
            risk_rating_trigger = False
            current_year = datetime.now().year
            i = 0
            for bcf in all_person_bcf_events:
                i += 1
                event_desc = truncate_string(bcf.get("eventDesc"))
                current_year = datetime.now().year
                try:
                    event_date = datetime.strptime(bcf.get("eventDt"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                category_desc = bcf.get("categoryDesc")
                details += f"{i}. {category_desc} - {event_desc} (Date: {event_date})\n "

                bcf_activities.append(event_desc)

            BCF1B["kpi_flag"] = True
            BCF1B["kpi_value"] = json.dumps(bcf_activities)
            BCF1B["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            BCF1B["kpi_details"] = details

        bcf_kpis.append(BCF1B)
        # ---------------------------------

        insert_status = await upsert_kpi("rfct", bcf_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure",
                    "info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module} : {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}


async def regulatory_analysis(data, session):

    print("Performing Regulatory Analysis...")

    kpi_area_module = "REG"

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

        REG1A = kpi_template.copy()
        REG1B = kpi_template.copy()

        REG1A["kpi_code"] = "REG1A"
        REG1A["kpi_definition"] = "Regulatory Actions - Organization"

        REG1B["kpi_code"] = "REG1B"
        REG1B["kpi_definition"] = "Regulatory Actions - People"

        reg_kpis = []

        required_columns = ["event_regulatory", "grid_event_regulatory"]
        intermediate_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        intermediate_data = intermediate_data[0]

        reg = intermediate_data.get("event_regulatory", None)
        grid_reg = intermediate_data.get("grid_event_regulatory", None)

        # Data for Person-Level
        required_columns = ["grid_regulatory"]
        retrieved_data = await get_dynamic_ens_data("grid_management", required_columns, ens_id_value, session_id_value, session)
        person_retrieved_data = retrieved_data  # Multiple rows/people per ens_id and session_id

        # Check if all person data is blank
        person_info_none = all(person.get("grid_regulatory", None) is None for person in person_retrieved_data)

        if person_info_none and (reg is None) and (grid_reg is None):
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        # ---------- REG1A - REGULATORY EVENTS - ORGANISATION
        reg = (reg or []) + (grid_reg or[] )  # TODO: ANOTHER WAY TO COMBINE THIS INFO IF IT OVERLAPS

        if len(reg) > 0:

            reg_events = []
            risk_rating_trigger = False
            reg_events_detail = "Risk identified due to the following events:\n"

            for event in reg:
                current_year = datetime.now().year
                try:
                    event_date = datetime.strptime(event.get("eventDt"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                text = f"""
                        {event.get("category")} - {event.get("categoryDesc")} ({event.get("eventDt")})
                        \n
                        {truncate_string(event.get("eventDesc"))}
                        \n
                    """
                reg_events.append(truncate_string(event.get("eventDesc")))
                reg_events_detail += text


            REG1A["kpi_flag"] = True
            REG1A["kpi_value"] = json.dumps(reg_events)
            REG1A["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            REG1A["kpi_details"] = reg_events_detail

            reg_kpis.append(REG1A)

        # --------- REG1B - Bribery, Corruption or Fraud - Person Level
        all_person_reg_events = []
        if not person_info_none and len(person_retrieved_data) > 0:
            for person in person_retrieved_data:
                reg_events = person.get("grid_regulatory",[])
                print(reg_events)
                if reg_events is not None:
                    all_person_reg_events = all_person_reg_events + reg_events

            reg_activities = []
            details = "Risk identified due to the following events:\n"
            risk_rating_trigger = False
            current_year = datetime.now().year
            i = 0
            for reg in all_person_reg_events:
                i += 1
                event_desc = truncate_string(reg.get("eventDesc"))
                try:
                    event_date = datetime.strptime(reg.get("eventDt"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                category_desc = reg.get("categoryDesc")
                details += f"{i}. {category_desc} - {event_desc} (Date: {event_date})\n "

                reg_activities.append(event_desc)

            REG1B["kpi_flag"] = True
            REG1B["kpi_value"] = json.dumps(reg_activities)
            REG1B["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            REG1B["kpi_details"] = details

        reg_kpis.append(REG1B)
        # ---------------------------------

        insert_status = await upsert_kpi("rfct", reg_kpis, ens_id_value, session_id_value, session)

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