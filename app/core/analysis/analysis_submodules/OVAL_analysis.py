import asyncio
from app.core.utils.db_utils import *
import json
import ast

async def ownership_analysis(data, session):

    print("Performing Ownership Structure Analysis... Started")

    kpi_area_module = "OWN"

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

        OWN1A = kpi_template.copy()

        OWN1A["kpi_code"] = "OWN1A"
        OWN1A["kpi_definition"] = "Direct Shareholder With > 50% Ownership" # TODO SET THRESHOLD

        required_columns = ["shareholders", "controlling_shareholders","controlling_shareholders_type","beneficial_owners", "beneficial_owners_intermediatory", "global_ultimate_owner","global_ultimate_owner_type", "other_ultimate_beneficiary", "ultimately_owned_subsidiaries"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        shareholders = retrieved_data.get("shareholders", None)
        controlling_shareholders = retrieved_data.get("controlling_shareholders", None)
        controlling_shareholders_type = retrieved_data.get("controlling_shareholders_type", None)
        beneficial_owners = retrieved_data.get("beneficial_owners", None)
        beneficial_owners_intermediatory = retrieved_data.get("beneficial_owners_intermediatory", None)
        global_ultimate_owner = retrieved_data.get("global_ultimate_owner", None)
        global_ultimate_owner_type = retrieved_data.get("global_ultimate_owner_type", None)
        other_ultimate_beneficiary = retrieved_data.get("other_ultimate_beneficiary", None)
        ultimately_owned_subsidiaries = retrieved_data.get("ultimately_owned_subsidiaries", None)

        # Check if all/any mandatory required data is None - (if so then add one general?) and return
        if all(var is None for var in [shareholders, controlling_shareholders]):
            print(f"{kpi_area_module} Analysis... Completed With No Data")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        # ---- PERFORM ANALYSIS LOGIC HERE
        if controlling_shareholders is not None:
            for csh in controlling_shareholders:
                total = csh.get("total_ownership", "n.a.")
                direct = csh.get("direct_ownership", "n.a.")
                if (total != "n.a.") and ast.literal_eval(total):
                    OWN1A["kpi_value"] = json.dumps(csh)
                    OWN1A["kpi_details"] = f"Controlling shareholder {csh.get("name")} has total ownership of {total}%"
                    OWN1A["kpi_rating"] = "HIGH"
                    break  # Since only one person can have > 50% control
                elif (direct != "n.a.") and ast.literal_eval(direct):
                    OWN1A["kpi_value"] = json.dumps(csh)
                    OWN1A["kpi_details"] = f"Controlling shareholder {csh.get("name")} has direct ownership of {direct}%"
                    OWN1A["kpi_rating"] = "HIGH"
                    break  # Since only one person can have > 50% control
        elif shareholders is not None:
            for sh in shareholders:
                total = sh.get("total_ownership", "n.a.")
                direct = sh.get("direct_ownership", "n.a.")
                if (total != "n.a.") and ast.literal_eval(total):
                    OWN1A["kpi_value"] = json.dumps(sh)
                    OWN1A["kpi_details"] = f"Shareholder {sh.get("name")} has total ownership of {total}%"
                    OWN1A["kpi_rating"] = "HIGH"
                    break  # Since only one person can have > 50% control
                elif (direct != "n.a.") and ast.literal_eval(direct):
                    OWN1A["kpi_value"] = json.dumps(sh)
                    OWN1A["kpi_details"] = f"Shareholder {sh.get("name")} has direct ownership of {direct}%"
                    OWN1A["kpi_rating"] = "HIGH"
                    break  # Since only one person can have > 50% control

        own_kpis = [OWN1A]

        insert_status = await upsert_kpi("oval", own_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure","info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}, {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}