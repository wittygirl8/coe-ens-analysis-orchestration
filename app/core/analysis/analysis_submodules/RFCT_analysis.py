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
        adv = sorted(adv, key=lambda x: x.get("eventDate", ""), reverse=True)
        unique_adv=set()
        i = j = 0
        if len(adv) > 0:
            current_year = datetime.now().year
            criminal_activities = []
            details = "Criminal activity discovered: \n"
            risk_rating_trigger = False
            for amo in adv:
                key=(amo.get("eventDate"),amo.get("eventCategory"),amo.get("eventSubCategory"),amo.get("eventDesc"))
                if key in unique_adv:
                    continue
                unique_adv.add(key)
                i += 1
                event_dict = {
                    "eventdt": amo.get("eventDate", "Unavailable"),
                    "eventcat": amo.get("eventCategory", ""),
                    "eventsub": amo.get("eventSubCategory", ""),
                    "categoryDesc": amo.get("eventCategoryDesc", ""),
                    "eventDesc": truncate_string(amo.get("eventDesc", ""))
                }
                event_desc = truncate_string(amo.get("eventDesc"))
                try:
                    event_date = datetime.strptime(amo.get("eventDate"), "%Y-%m-%d")
                    # print("event_date")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                category_desc = amo.get("eventCategoryDesc")
                sub_category_desc = amo.get("eventSubCategoryDesc")
                details += f"{i}. {category_desc}: {sub_category_desc} - {event_desc} (Date: {event_date})\n "

                criminal_activities.append(event_dict)
                if i + j >= 5:
                    break

            kpi_value_overall_dict = {
                "count": len(criminal_activities) if len(criminal_activities) < 6 else "5 or more",
                "target": "org",  # Since this is organization level
                "findings": criminal_activities,
                "themes": [a.get("eventsub") for a in criminal_activities]
            }

            AMO1A["kpi_flag"] = True
            AMO1A["kpi_value"] =json.dumps(kpi_value_overall_dict)
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
            all_person_amo_events = sorted(all_person_amo_events, key=lambda x:x.get("eventDate", ""), reverse=True)
            for amo in all_person_amo_events:
                j += 1
                event_dict = {
                    "eventdt": amo.get("eventDate", "Unavailable"),
                    "eventcat": amo.get("eventCategory", ""),
                    "eventsub": amo.get("eventSubCategory", ""),
                    "categoryDesc": amo.get("eventCategoryDesc", ""),
                    "eventDesc": truncate_string(amo.get("eventDesc", ""))
                }

                event_desc = truncate_string(amo.get("eventDesc"))
                try:
                    event_date = datetime.strptime(amo.get("eventDate"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                category_desc = amo.get("eventCategoryDesc")
                sub_category_desc = amo.get("eventSubCategoryDesc")
                details += f"{j}. {category_desc} : {sub_category_desc} - {event_desc} (Date: {event_date})\n "

                criminal_activities.append(event_dict)
                if i + j >= 5:
                    break
            kpi_value_overall_dict = {
                "count": len(criminal_activities) if len(criminal_activities) < 6 else "5 or more",
                "target": "person",  # Since this is person level
                "findings": criminal_activities,
                "themes": [a.get("eventsub") for a in criminal_activities]
            }
            AMO1B["kpi_flag"] = True
            AMO1B["kpi_value"] = json.dumps(kpi_value_overall_dict)
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
        adv = sorted(adv, key=lambda x: x.get("eventDate", ""), reverse=True)
        unique_adv=set()
        i = j =0
        if len(adv) > 0:
            current_year = datetime.now().year
            reputation_risks = []
            details = "Reputation risk due to the following events:\n"
            risk_rating_trigger = False
            for adv in adv:
                key = (adv.get("eventDate"), adv.get("eventCategory"), adv.get("eventSubCategory"), adv.get("eventDesc"))
                if key in unique_adv:
                    continue
                unique_adv.add(key)
                event_dict = {
                    "eventdt": adv.get("eventDate", "Unavailable"),
                    "eventcat": adv.get("eventCategory", ""),
                    "eventsub": adv.get("eventSubCategory", ""),
                    "categoryDesc": adv.get("eventCategoryDesc", ""),
                    "eventDesc": truncate_string(adv.get("eventDesc", ""))
                }
                event_desc = truncate_string(adv.get("eventDesc"))
                try:
                    event_date = datetime.strptime(adv.get("eventDate"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"

                category_desc = adv.get("eventCategory")
                sub_category_desc = adv.get("eventSubCategory")

                details += f"{i+1}. {category_desc}: {sub_category_desc} - {event_desc} (Date: {event_date})\n"
                reputation_risks.append(event_dict)
                i+=1
                if i + j >= 5:
                    break
            kpi_value_overall_dict = {
                "count": len(reputation_risks) if len(reputation_risks) < 6 else "5 or more",
                "target": "person",  # Since this is person level
                "findings": reputation_risks,
                "themes": [a.get("eventsub") for a in reputation_risks]
            }

            AMR1A["kpi_flag"] = True
            AMR1A["kpi_value"] = json.dumps(kpi_value_overall_dict)
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
            all_person_amr_events = sorted(all_person_amr_events, key=lambda x: x.get("eventDate", ""), reverse=True)
            for amr in all_person_amr_events:
                event_dict = {
                    "eventdt": amr.get("eventDate", "Unavailable"),
                    "eventcat": amr.get("eventCategory", ""),
                    "eventsub": amr.get("eventSubCategory", ""),
                    "categoryDesc": amr.get("eventCategoryDesc", ""),
                    "eventDesc": truncate_string(amr.get("eventDesc", ""))
                }
                event_desc = truncate_string(amr.get("eventDesc"))
                try:
                    event_date = datetime.strptime(amr.get("eventDate"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"

                category_desc = amr.get("eventCategoryDesc")
                details += f"{j+1}. {category_desc} - {event_desc} (Date: {event_date})\n "
                j+=1
                criminal_activities.append(event_dict)
                if i + j >=5:
                    break
            kpi_value_overall_dict = {
                "count": len(criminal_activities) if len(criminal_activities) < 6 else "5 or more",
                "target": "person",  # Since this is person level
                "findings": criminal_activities,
                "themes": [a.get("eventsub") for a in criminal_activities]
            }

            AMR1B["kpi_flag"] = True
            AMR1B["kpi_value"] = json.dumps(kpi_value_overall_dict)
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
        bcf = sorted(bcf, key=lambda x: x.get("eventDate", ""), reverse=True)
        unique_bcf=set()
        i = j = 0
        if len(bcf) > 0:
            bcf_events = []
            risk_rating_trigger = False
            bcf_events_detail = "Risk identified due to the following events:\n"
            for event in bcf:
                key = (event.get("eventDate"), event.get("eventCategory"), event.get("eventSubCategory"), event.get("eventDesc"))
                if key in unique_bcf:
                    continue
                unique_bcf.add(key)
                event_dict = {
                    "eventdt": event.get("eventDate", "Unavailable"),
                    "eventcat": event.get("eventCategory", ""),
                    "eventsub": event.get("eventSubCategory", ""),
                    "categoryDesc": event.get("eventCategoryDesc", ""),
                    "eventDesc": truncate_string(event.get("eventDesc", ""))
                }
                current_year = datetime.now().year
                try:
                    event_date = datetime.strptime(event.get("eventDate"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                text = f"{i+1}. {event.get('eventCategoryDesc')}: {event.get('eventSubCategoryDesc')} - {truncate_string(event.get("eventDesc"))} (Date: {event.get("eventDate")})\n"
                bcf_events.append(event_dict)
                bcf_events_detail += text
                i+=1
                if i+j>=5:
                    break
            kpi_value_overall_dict = {
                "count": len(bcf_events) if len(bcf_events) < 6 else "5 or more",
                "target": "person",  # Since this is person level
                "findings": bcf_events,
                "themes": [a.get("eventsub") for a in bcf_events]
            }
            BCF1A["kpi_flag"] = True
            BCF1A["kpi_value"] = json.dumps(kpi_value_overall_dict)
            BCF1A["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            BCF1A["kpi_details"] = bcf_events_detail

            bcf_kpis.append(BCF1A)

        # --------- BCF1B - Bribery, Corruption or Fraud - Person Level
        all_person_bcf_events = []
        if not person_info_none and len(person_retrieved_data) > 0:
            for person in person_retrieved_data:
                bcf_events = person.get("grid_bribery_fraud_corruption",[])
                if bcf_events is not None:
                    all_person_bcf_events = all_person_bcf_events + bcf_events


            bcf_activities = []
            details = "Risk identified due to the following events:\n"
            risk_rating_trigger = False
            current_year = datetime.now().year
            all_person_bcf_events = sorted(all_person_bcf_events, key=lambda x: x.get("eventDate", ""), reverse=True)

            for bcf in all_person_bcf_events:
                event_dict = {
                    "eventdt": bcf.get("eventDate", "Unavailable"),
                    "eventcat": bcf.get("eventCategory", ""),
                    "eventsub": bcf.get("eventSubCategory", ""),
                    "categoryDesc": bcf.get("eventCategoryDesc", ""),
                    "eventDesc": truncate_string(bcf.get("eventDesc", ""))
                }
                event_desc = truncate_string(bcf.get("eventDesc"))
                current_year = datetime.now().year
                try:
                    event_date = datetime.strptime(bcf.get("eventDate"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                category_desc = bcf.get("eventCategoryDesc")
                sub_category_desc = bcf.get("eventSubCategoryDesc")
                details += f"{j+1}. {category_desc}: {sub_category_desc}- {event_desc} (Date: {event_date})\n "

                bcf_activities.append(event_dict)
                j += 1
                if i + j >= 5:
                    break
            kpi_value_overall_dict = {
                "count": len(bcf_activities) if len(bcf_activities) < 6 else "5 or more",
                "target": "person",  # Since this is person level
                "findings": bcf_activities,
                "themes": [a.get("eventsub") for a in bcf_activities]
            }
            BCF1B["kpi_flag"] = True
            BCF1B["kpi_value"] = json.dumps(kpi_value_overall_dict)
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
        reg = sorted(reg, key=lambda x: x.get("eventDate", ""), reverse=True)
        unique_reg=set()
        i = j =0
        if len(reg) > 0:

            reg_events = []
            risk_rating_trigger = False
            reg_events_detail = "Risk identified due to the following events:\n"

            for event in reg:
                key = (event.get("eventDate"), event.get("eventCategory"), event.get("eventSubCategory"), event.get("eventDesc"))
                if key in unique_reg:
                    continue
                unique_reg.add(key)
                event_dict = {
                    "eventdt": event.get("eventDate", "Unavailable"),
                    "eventcat": event.get("eventCategory", ""),
                    "eventsub": event.get("eventSubCategory", ""),
                    "categoryDesc": event.get("eventCategoryDesc", ""),
                    "eventDesc": truncate_string(event.get("eventDesc", ""))
                }
                current_year = datetime.now().year
                try:
                    event_date = datetime.strptime(event.get("eventDate"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                text = f"{i+1}. {event.get('eventCategoryDesc')}: {event.get('eventSubCategoryDesc')} - {truncate_string(event.get("eventDesc"))}(Date: {event.get("eventDate")})\n"
                reg_events.append(event_dict)
                reg_events_detail += text
                i+=1
                if i+j>=5:
                    break
            kpi_value_overall_dict = {
                "count": len(reg_events) if len(reg_events) < 6 else "5 or more",
                "target": "person",  # Since this is person level
                "findings": reg_events,
                "themes": [a.get("eventsub") for a in reg_events]
            }

            REG1A["kpi_flag"] = True
            REG1A["kpi_value"] = json.dumps(kpi_value_overall_dict)
            REG1A["kpi_rating"] = "High" if risk_rating_trigger else "Medium"
            REG1A["kpi_details"] = reg_events_detail

            reg_kpis.append(REG1A)

        # --------- REG1B - Bribery, Corruption or Fraud - Person Level
        all_person_reg_events = []
        if not person_info_none and len(person_retrieved_data) > 0:
            for person in person_retrieved_data:
                reg_events = person.get("grid_regulatory",[])
                if reg_events is not None:
                    all_person_reg_events = all_person_reg_events + reg_events

            reg_activities = []
            details = "Risk identified due to the following events:\n"
            risk_rating_trigger = False
            current_year = datetime.now().year
            all_person_reg_events = sorted(all_person_reg_events, key=lambda x: x.get("eventDate", ""), reverse=True)
            for reg in all_person_reg_events:
                event_desc = truncate_string(reg.get("eventDesc"))
                try:
                    event_date = datetime.strptime(reg.get("eventDate"), "%Y-%m-%d")
                    event_year = current_year - event_date.year
                    if event_year <= 5:
                        risk_rating_trigger = True
                except:
                    event_date = "Unavailable"
                category_desc = reg.get("eventCategoryDesc")
                event_dict = {
                    "eventdt": reg.get("eventDate", "Unavailable"),
                    "eventcat": reg.get("eventCategory", ""),
                    "eventsub": reg.get("eventSubCategory", ""),
                    "categoryDesc": reg.get("eventCategoryDesc", ""),
                    "eventDesc": truncate_string(reg.get("eventDesc", ""))
                }
                details += f"{j+1}. {category_desc} - {event_desc} (Date: {event_date})\n "

                reg_activities.append(event_dict)
                j+=1
                if i+j>=5:
                    break
            kpi_value_overall_dict = {
                "count": len(reg_activities) if len(reg_activities) < 6 else "5 or more",
                "target": "person",  # Since this is person level
                "findings": reg_activities,
                "themes": [a.get("eventsub") for a in reg_activities]
            }
            REG1B["kpi_flag"] = True
            REG1B["kpi_value"] = json.dumps(kpi_value_overall_dict)
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