import asyncio
from app.core.utils.db_utils import *
import json

async def sown_analysis(data, session):

    print("Performing State Ownership Structure Analysis... Started")

    kpi_area_module = "SCO"

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

        SCO1A = kpi_template.copy()

        SCO1A["kpi_code"] = "SCO1A"
        SCO1A["kpi_definition"] = "Global Ultimate Owner Is Public Authority/State/Government"

        required_columns = ["shareholders", "global_ultimate_owner", "global_ultimate_owner_type"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        shareholders = retrieved_data.get("shareholders", None)
        global_ultimate_owner = retrieved_data.get("global_ultimate_owner", None)
        global_ultimate_owner_type = retrieved_data.get("global_ultimate_owner_type", None)

        # ---- PERFORM ANALYSIS LOGIC HERE
        if "state" in global_ultimate_owner_type:  # TODO INSERT LOGIC
            SCO1A["kpi_flag"] = True
            SCO1A["kpi_value"] = json.dumps(global_ultimate_owner_type)
            SCO1A["kpi_rating"] = "High"
            SCO1A["kpi_details"] = f"Global Ultimate Owner: {global_ultimate_owner}, is {global_ultimate_owner_type}"
        else:
            SCO1A["kpi_value"] = json.dumps(global_ultimate_owner_type)
            SCO1A["kpi_rating"] = "LOW"
            SCO1A["kpi_details"] = f"Group / Global Ultimate Owner not found to be state-controlled"

        sco_kpis = [SCO1A]

        insert_status = await upsert_kpi("sown", sco_kpis, ens_id_value, session_id_value, session) # --- SHOULD WE CHANGE THIS TO BE PART OF PEP

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure","info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}
