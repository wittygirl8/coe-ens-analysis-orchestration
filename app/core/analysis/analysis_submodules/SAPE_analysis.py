import datetime
import asyncio
from app.core.utils.db_utils import *
import json


async def sanctions_analysis(data, session):
    print("Performing Sanctions Analysis...")

    kpi_area_module = "SAN"

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

        SAN1A = kpi_template.copy()
        SAN1B = kpi_template.copy()
        SAN2A = kpi_template.copy()
        SAN2B = kpi_template.copy()

        SAN1A["kpi_code"] = "SAN1A"
        SAN1A["kpi_definition"] = "Sanctions for Direct Affiliations - Organisation Level"

        SAN1B["kpi_code"] = "SAN1B"
        SAN1B["kpi_definition"] = "Sanctions for Direct Affiliations - Person Level"

        SAN2A["kpi_code"] = "SAN2A"
        SAN2A["kpi_definition"] = "Sanctions for Indirect Affiliations - Organisation Level"

        SAN2B["kpi_code"] = "SAN2B"
        SAN2B["kpi_definition"] = "Sanctions for Indirect Affiliations - Person Level"

        san_kpis = []

        required_columns = ["event_sanctions", "grid_event_sanctions"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        sanctions_data = retrieved_data.get("event_sanctions", None)
        grid_event_sanctions = retrieved_data.get("grid_event_sanctions", None)

        # Data for Person-Level
        required_columns = ["grid_sanctions"]
        retrieved_data = await get_dynamic_ens_data("grid_management", required_columns, ens_id_value, session_id_value, session)
        person_retrieved_data = retrieved_data  # Multiple rows/people per ens_id and session_id

        # Check if all person data is blank
        person_info_none = all(person.get("grid_sanctions", None) is None for person in person_retrieved_data)

        if person_info_none and (sanctions_data is None) and (grid_event_sanctions is None):
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        # ---------------------- SAN1A, 2A: Direct and Indirect for Org Level from table "external_supplier_data"

        sanctions_data = (sanctions_data or []) + (grid_event_sanctions or []) # TODO: ANOTHER WAY TO COMBINE THIS INFO IF IT OVERLAPS
        sanctions_data = sorted(sanctions_data, key=lambda x: x.get("eventDate", ""), reverse=True)
        unique_sanctions_data=set()
        i = j = 0
        if len(sanctions_data) > 0:
            current_year = datetime.datetime.now().year
            indirect_eventCategory = ["SNX"]
            sanctions_lists_direct = sanctions_lists_indirect = []
            details_direct = details_indirect = "Following sanctions imposed :\n"
            risk_rating_trigger_direct = risk_rating_trigger_indirect = False
            for record in sanctions_data:
                key = (record.get("eventDate"), record.get("eventCategory"), record.get("eventSubCategory"), record.get("eventDesc"))
                if key in unique_sanctions_data:
                    continue
                unique_sanctions_data.add(key)
                try:
                    date = datetime.datetime.strptime(record.get("eventDate"), "%Y-%m-%d")
                    sanction_time_period = current_year - date.year
                    date_string = f"As of [{date}]: "
                except:
                    date = "Unavailable Date"
                    sanction_time_period = 999  # Workaround temp
                    date_string = ""

                event_category = record.get("eventCategory", None) # orbis grid version
                event_subcategory = record.get("eventSubCategory", None)  # orbis grid version
                event_categoryDesc = record.get("eventCategoryDesc", None)  # orbis grid version
                event_subcategoryDesc = record.get("eventSubCategoryDesc", None)  # orbis grid version


                sanctions_list_name = record.get("sourceName", "Sanctions Lists") # Give a generic string if we dont have the actual name
                sanction_details = truncate_string(record.get("eventDesc",""))
                sanction_entity_name = record.get("entityName", "")

                event_dict = {
                    "eventdt": record.get("eventDate", record.get("eventDt", "Unavailable")),
                    "eventcat": event_category,
                    "eventsub": event_subcategory,
                    "categoryDesc": record.get("eventCategoryDesc", record.get("categoryDesc", "")),
                    "entityName": sanction_entity_name,
                    "eventDesc": sanction_details
                }

                if event_category in indirect_eventCategory:
                    SAN2A["kpi_flag"] = True
                    date_string = f'{i + 1}. ' + date_string
                    details_indirect += f"\n{date_string} {event_categoryDesc} - {event_subcategoryDesc}: \n {sanction_details}"
                    sanctions_lists_indirect.append(event_dict)
                    i+=1
                    if sanction_time_period <= 5:
                        risk_rating_trigger_indirect = True
                else:
                    SAN1A["kpi_flag"] = True
                    date_string = f'{j + 1}. ' + date_string
                    details_direct +=f"\n{date_string} {event_categoryDesc} - {event_subcategoryDesc}: \n {sanction_details}"
                    sanctions_lists_direct.append(event_dict)
                    j+=1
                    if sanction_time_period <= 5:
                        risk_rating_trigger_direct = True
                if i+j >=5:
                    break  # TODO REMOVE - TEMP

            # For sanctions - client has requested to see "this entity is not sanctioned and none of the individuals are"
            if SAN1A["kpi_flag"]:
                kpi_value_direct_dict = {
                    "count": len(sanctions_lists_direct) if len(sanctions_lists_direct) < 6 else "5 or more",
                    "target": "org",  # Since this is organization level
                    "findings": sanctions_lists_direct,
                    "themes": [a.get("eventsub") for a in sanctions_lists_direct]
                }
                SAN1A["kpi_value"] = json.dumps(kpi_value_direct_dict)
                SAN1A["kpi_rating"] = "High" if risk_rating_trigger_direct else "Medium"
                SAN1A["kpi_details"] = details_direct
            else:
                SAN1A["kpi_value"] = ""
                SAN1A["kpi_rating"] = "INFO"
                SAN1A["kpi_details"] = "Screening Results Indicate No Direct Sanctions for This Entity"

            san_kpis.append(SAN1A)

            if SAN2A["kpi_flag"]:
                kpi_value_indirect_dict = {
                    "count": len(sanctions_lists_indirect) if len(sanctions_lists_indirect) < 6 else "5 or more",
                    "target": "org",  # Since this is organization level
                    "findings": sanctions_lists_indirect,
                    "themes": [a.get("eventsub") for a in sanctions_lists_indirect]
                }
                SAN2A["kpi_value"] = json.dumps(kpi_value_indirect_dict)
                SAN2A["kpi_rating"] = "High" if risk_rating_trigger_indirect else "Medium"
                SAN2A["kpi_details"] = details_indirect
            else:
                SAN2A["kpi_value"] = ""
                SAN2A["kpi_rating"] = "INFO"
                SAN2A["kpi_details"] = "Screening Results Indicate No Indirect Sanctions for This Entity"

            san_kpis.append(SAN2A)

        print("# ------------------------------------------------------------ # PERSONS")
        # ---------------------- SAN1B, 2B: Direct and Indirect for PERSON Level from table "grid_management"
        all_person_san_events = []
        if not person_info_none and len(person_retrieved_data) > 0:
            for person in person_retrieved_data:
                san_events = person.get("grid_sanctions",[])
                if san_events is not None:
                    all_person_san_events = all_person_san_events + san_events

            current_year = datetime.datetime.now().year
            indirect_eventCategory = ["SNX"]
            sanctions_lists_direct = sanctions_lists_indirect = []
            details_direct = details_indirect = "Following sanctions imposed :\n"
            risk_rating_trigger_direct = risk_rating_trigger_indirect = False
            all_person_san_events = sorted(all_person_san_events, key=lambda x: x.get("eventDate", ""), reverse=True)
            i = j =  0
            for record in all_person_san_events:

                try:
                    date = datetime.datetime.strptime(record.get("eventDate"), "%Y-%m-%d")
                    sanction_time_period = current_year - date.year
                    date_string = f" As of [{date}]: "
                except:
                    date = "Unavailable Date"
                    sanction_time_period = 999  # Workaround temp
                    date_string = ""

                event_category = record.get("eventCategory", None)
                event_subcategory = record.get("eventSubCategory", None)
                event_categoryDesc = record.get("eventCategoryDesc", None)
                event_subcategoryDesc = record.get("eventSubCategoryDesc", None)

                sanctions_list_name = record.get("sourceName", "Sanctions Lists") # Give a generic string if we dont have the actual name
                sanction_details = truncate_string(record.get("eventDesc"))
                sanction_entity_name = record.get("entityName","")

                event_dict = {
                    "eventdt": record.get("eventDate", "Unavailable"),
                    "eventcat": event_category,
                    "eventsub": event_subcategory,
                    "categoryDesc": record.get("eventCategoryDesc", ""),
                    "entityName": sanction_entity_name,
                    "eventDesc": sanction_details
                }

                if event_category in indirect_eventCategory:
                    SAN2B["kpi_flag"] = True
                    date_string =f'{i+1}. '+date_string
                    details_indirect += f"\n{date_string} {event_categoryDesc} - {event_subcategoryDesc}: \n {sanction_details}"
                    sanctions_lists_indirect.append(event_dict)
                    i+=1
                    if sanction_time_period <= 5:
                        risk_rating_trigger_indirect = True
                else:
                    SAN1B["kpi_flag"] = True
                    date_string = f'{j + 1}. ' + date_string
                    details_direct += f"\n{date_string} {event_categoryDesc} - {event_subcategoryDesc}: \n {sanction_details}"
                    sanctions_lists_direct.append(event_dict)
                    j+=1
                    if sanction_time_period <= 5:
                        risk_rating_trigger_direct = True

                if i+j >=5:
                    break  # TODO REMOVE - TEMP

            # For sanctions & pep, have no value options TODO - also add above
            if SAN1B["kpi_flag"]:
                kpi_value_direct_dict = {
                    "count": len(sanctions_lists_direct) if len(sanctions_lists_direct) < 6 else "5 or more",
                    "target": "person",  # Since this is person level
                    "findings": sanctions_lists_direct,
                    "themes": [a.get("eventsub") for a in sanctions_lists_direct]
                }
                SAN1B["kpi_value"] = json.dumps(kpi_value_direct_dict)
                SAN1B["kpi_rating"] = "High" if risk_rating_trigger_direct else "Medium"
                SAN1B["kpi_details"] = details_direct
            else:
                SAN1B["kpi_value"] = ""
                SAN1B["kpi_rating"] = "INFO"
                SAN1B["kpi_details"] = "Screening Results Indicate No Direct Sanctions for Individuals"

            san_kpis.append(SAN1B)

            if SAN2B["kpi_flag"]:
                kpi_value_indirect_dict = {
                    "count": len(sanctions_lists_indirect) if len(sanctions_lists_indirect) < 6 else "5 or more",
                    "target": "person",  # Since this is person level
                    "findings": sanctions_lists_indirect,
                    "themes": [a.get("eventsub") for a in sanctions_lists_indirect]
                }
                SAN2B["kpi_value"] = json.dumps(kpi_value_indirect_dict)
                SAN2B["kpi_rating"] = "High" if risk_rating_trigger_indirect else "Medium"
                SAN2B["kpi_details"] = details_indirect
            else:
                SAN2B["kpi_value"] = ""
                SAN2B["kpi_rating"] = "INFO"
                SAN2B["kpi_details"] = "Screening Results Indicate No Indirect Sanctions for Individuals"

            san_kpis.append(SAN2B)
        # ---------------------------------

        print(f"FOUND {len(san_kpis)} KPIS IN TOTAL -------------------")
        # Insert results into the database
        insert_status = await upsert_kpi("sape", san_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure","info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}


# async def pep_analysis(data, session):
#
#     print("Performing PEP Analysis...")
#
#     kpi_area_module = "PEP"
#
#     ens_id_value = data.get("ens_id")
#     session_id_value = data.get("session_id")
#
#     try:
#
#         kpi_template = {
#             "kpi_area": kpi_area_module,
#             "kpi_code": "",
#             "kpi_definition": "",
#             "kpi_flag": False,
#             "kpi_value": None,
#             "kpi_rating": "",
#             "kpi_details": ""
#         }
#
#         PEP1A = kpi_template.copy()
#         PEP1B = kpi_template.copy()
#         PEP2A = kpi_template.copy()
#         PEP2B = kpi_template.copy()
#
#         PEP1A["kpi_code"] = "PEP1A"
#         PEP1A["kpi_definition"] = "PeP for Direct Affiliations - Organisation Level"
#
#         PEP1B["kpi_code"] = "PEP1B"
#         PEP1B["kpi_definition"] = "PeP for Direct Affiliations -  Person Level"
#
#         PEP2A["kpi_code"] = "PEP2A"
#         PEP2A["kpi_definition"] = "PeP for Indirect Affiliations - Organisation Level"
#
#         PEP2B["kpi_code"] = "PEP2B"
#         PEP2B["kpi_definition"] = "PeP for Indirect Affiliations - Person Level"
#
#         pep_kpis = []
#
#         required_columns = ["event_pep", "grid_event_pep", "management"]
#         retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
#         retrieved_data = retrieved_data[0]
#         pep_management_list = [x for x in retrieved_data.get("management", []) if x.get("pep_indicator").lower() == 'yes']
#         event_pep = retrieved_data.get("event_pep", [])
#         grid_event_pep = retrieved_data.get("grid_event_pep", [])
#
#         # Data for Person-Level
#         required_columns = ["grid_pep"]
#         retrieved_data = await get_dynamic_ens_data("grid_management", required_columns, ens_id_value, session_id_value, session)
#         person_retrieved_data = retrieved_data  # Multiple rows/people per ens_id and session_id
#
#         # Check if all person data is blank
#         person_info_none = all(person.get("grid_pep", None) is None for person in person_retrieved_data)
#
#         if person_info_none and (event_pep is None) and (grid_event_pep is None) and (len(pep_management_list)==0):
#             return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}
#
#
#         # ---------------------- PEP1A, 2A: Direct and Indirect for Org Level from table "external_supplier_data"
#         pep_data = (event_pep or []) + (grid_event_pep or []) # TODO: ANOTHER WAY TO COMBINE THIS INFO IF IT OVERLAPS
#         pep_data = sorted(pep_data, key=lambda x: x.get("eventDate", ""), reverse=True)
#         unique_pep_data=set()
#         if len(pep_data) > 0:
#             current_year = datetime.datetime.now().year
#             indirect_eventCategory = [""]
#             pep_lists_direct = pep_lists_indirect = []
#             details_direct = details_indirect = "Following PeP findings :\n"
#             risk_rating_trigger_direct = risk_rating_trigger_indirect = False
#
#             i = j = 0
#             for record in pep_data:
#                 key = (record.get("eventDate"), record.get("eventCategory"), record.get("eventSubCategory"), record.get("eventDesc"))
#                 if key in unique_pep_data:
#                     continue
#                 unique_pep_data.add(key)
#                 try:
#                     date = datetime.datetime.strptime(record.get("eventDate"), "%Y-%m-%d")
#                     time_period = current_year - date.year
#                     date_string = f"As of [{date}]: "
#                 except:
#                     date = "Unavailable Date"
#                     # When date not available, The KPI Rating is high
#                     risk_rating_trigger_direct=True
#                     time_period = 999  # Workaround temp
#                     date_string = ""
#
#
#                 event_category = record.get("eventCategory", None) # orbis grid version
#                 event_subcategory = record.get("eventSubCategory", None)  # orbis grid version
#                 pep_details = truncate_string(record.get("eventDesc",""))
#                 pep_entity_name = record.get("entityName", "")
#
#                 if event_category in indirect_eventCategory:
#                     PEP2A["kpi_flag"] = True
#                     details_indirect += f"\n{i+1}.{date_string}{pep_entity_name}: {pep_details}"
#                     pep_lists_indirect.append(pep_details)
#                     i+=1
#                     if time_period <= 5:
#                         risk_rating_trigger_indirect = True
#                 else:
#                     PEP1A["kpi_flag"] = True
#                     details_direct += f"\n{j+1}.{date_string}{pep_entity_name}: {pep_details}"
#                     pep_lists_direct.append(pep_details)
#                     j+=1
#                     if time_period <= 5:
#                         risk_rating_trigger_direct = True
#                 if i+j >=5:
#                     break  # TODO REMOVE - TEMP
#
#             # For sanctions - client has requested to see "this entity is not sanctioned and none of the individuals are"
#             if PEP1A["kpi_flag"]:
#                 PEP1A["kpi_value"] = json.dumps(pep_lists_direct)
#                 PEP1A["kpi_rating"] = "High" if risk_rating_trigger_direct else "Medium"
#                 PEP1A["kpi_details"] = details_direct
#             else:
#                 PEP1A["kpi_value"] = ""
#                 PEP1A["kpi_rating"] = "INFO"
#                 PEP1A["kpi_details"] = "Screening Results Indicate No PeP Findings"
#
#             pep_kpis.append(PEP1A)
#
#             if PEP2A["kpi_flag"]:
#                 PEP2A["kpi_value"] = json.dumps(pep_lists_indirect)
#                 PEP2A["kpi_rating"] = "High" if risk_rating_trigger_indirect else "Medium"
#                 PEP2A["kpi_details"] = details_indirect
#             else:
#                 PEP2A["kpi_value"] = ""
#                 PEP2A["kpi_rating"] = "INFO"
#                 PEP2A["kpi_details"] = "Screening Results Indicate No PeP Findings"
#
#             pep_kpis.append(PEP2A)
#
#         print("# ------------------------------------------------------------ # PERSONS")
#         # ---------------------- SAN1B, 2B: Direct and Indirect for PERSON Level from table "grid_management"
#         all_person_pep_events = []
#         unique_pep_entity_name = set()
#         pep_lists_direct = pep_lists_indirect = []
#         i = j = 0
#         if not person_info_none and len(person_retrieved_data) > 0:
#             for person in person_retrieved_data:
#                 pep_events = person.get("grid_pep",[])
#                 if pep_events is not None:
#                     all_person_pep_events = all_person_pep_events + pep_events
#
#             current_year = datetime.datetime.now().year
#             indirect_eventCategory = [""]
#             details_direct = details_indirect = "Following PeP findings :\n"
#             risk_rating_trigger_direct = risk_rating_trigger_indirect = False
#             all_person_pep_events = sorted(all_person_pep_events, key=lambda x: x.get("eventDate", ""), reverse=True)
#             for record in all_person_pep_events:
#                 try:
#                     date = datetime.datetime.strptime(record.get("eventDate"), "%Y-%m-%d")
#                     time_period = current_year - date.year
#                     date_string = f"As of [{date}]: "
#                 except:
#                     date = "Unavailable Date"
#                     risk_rating_trigger_direct=True
#                     time_period = 999  # Workaround temp
#                     date_string = ""
#
#
#                 event_category = record.get("eventCategory", None)
#                 event_subcategory = record.get("eventSubCategory", None)
#
#                 pep_details = truncate_string(record.get("eventDesc",""))
#                 pep_entity_name = record.get("entityName", "")
#
#                 if event_category in indirect_eventCategory:
#                     PEP2B["kpi_flag"] = True
#                     details_indirect += f"\n{i+1}.{date_string}{pep_entity_name}: {pep_details}"
#                     pep_lists_indirect.append(pep_details)
#                     i+=1
#                     if time_period <= 5:
#                         risk_rating_trigger_indirect = True
#                 else:
#                     PEP1B["kpi_flag"] = True
#                     details_direct += f"\n{j+1}.{date_string}{pep_entity_name}: {pep_details}"
#                     unique_pep_entity_name.add(pep_entity_name)
#                     pep_lists_direct.append(pep_details)
#                     j+=1
#                     if time_period <= 5:
#                         risk_rating_trigger_direct = True
#                 if i+j >= 5:
#                     break  # TODO REMOVE - TEMP
#
#
#             # For sanctions & pep, have no value options TODO - also add above
#             if PEP1B["kpi_flag"]:
#                 PEP1B["kpi_value"] = str(pep_lists_direct)
#                 PEP1B["kpi_rating"] = "High" if risk_rating_trigger_direct else "Medium"
#                 PEP1B["kpi_details"] = details_direct
#             else:
#                 PEP1B["kpi_value"] = ""
#                 PEP1B["kpi_rating"] = "INFO"
#                 PEP1B["kpi_details"] = "Screening Results Indicate No PeP Findings"
#
#             if PEP2B["kpi_flag"]:
#                 PEP2B["kpi_value"] = str(pep_lists_indirect)
#                 PEP2B["kpi_rating"] = "High" if risk_rating_trigger_indirect else "Medium"
#                 PEP2B["kpi_details"] = details_indirect
#             else:
#                 PEP2B["kpi_value"] = ""
#                 PEP2B["kpi_rating"] = "INFO"
#                 PEP2B["kpi_details"] = "Screening Results Indicate No PeP Findings"
#
#             pep_kpis.append(PEP2B)
#         elif len(pep_management_list)>0:
#             j=0
#             details_direct = "Following PeP findings :\n"
#             for person in pep_management_list:
#                 if i >= 5:
#                     break  # TODO REMOVE - TEMP
#                 else:
#                     if person.get("name") not in unique_pep_entity_name:
#                         PEP1B["kpi_flag"] = True
#                         details_direct += f"\n{j+1}.As of [Unavailable Date]:{person.get("name")}: On BVD StateList"
#                         unique_pep_entity_name.add(person.get("name"))
#                         pep_lists_direct.append("")
#                         j += 1
#             if j:
#                 PEP1B["kpi_flag"]= True
#                 PEP1B["kpi_value"] = str(pep_lists_direct)
#                 PEP1B["kpi_rating"] = "High"
#                 PEP1B["kpi_details"] = details_direct
#         pep_kpis.append(PEP1B)
#
#         # ---------------------------------
#
#         print(f"FOUND {len(pep_kpis)} KPIS IN TOTAL -------------------")
#         # Insert results into the database
#         insert_status = await upsert_kpi("sape", pep_kpis, ens_id_value, session_id_value, session)
#
#         if insert_status["status"] == "success":
#             print(f"{kpi_area_module} Analysis... Completed Successfully")
#             return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
#         else:
#             return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure","info": "database_saving_error"}
#
#     except Exception as e:
#         print(f"Error in module: {kpi_area_module}: {str(e)}")
#         return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}
#
#
def truncate_string(input_string, word_limit=40):
    try:
        words = input_string.split()  # Split the string into words
        truncated = " ".join(words[:word_limit])  # Get the first 'word_limit' words
        if len(words) > word_limit:
            truncated += " [...]"  # Add ellipsis if the string is longer than 'word_limit' words
        return truncated
    except:
        return input_string