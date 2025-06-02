import asyncio
from app.core.utils.db_utils import *
import json
import datetime
from app.schemas.logger import logger

async def country_risk_analysis(data, session):

    module_activation = False

    logger.info("Performing Country Risk Analysis... Started")

    kpi_area_module = "CR"

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    if not module_activation:
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "module_deactivated"}

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

        CR1A = kpi_template.copy()

        CR1A["kpi_code"] = "CR1A"
        CR1A["kpi_definition"] = "Country Risk"

        required_columns = ["country"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        country = retrieved_data.get("country", None).lower()

        # High-Risk Countries
        high_risk_countries = [
            "russia", "belarus", "crimea", "cuba", "iran", "north korea",
            "syria", "sudan", "china", "venezuela", "yemen"
        ]

        # Medium-Risk Countries
        medium_risk_countries = [
            "afghanistan", "bosnia & herzegovina", "burundi", "central african republic",
            "domestic republic of congo", "guinea", "guinea bissau", "iraq", "lebanon",
            "libya", "maldives", "mali", "myanmar", "nicaragua", "somalia", "south sudan",
            "syria", "tunisia", "ukraine", "zimbabwe"
        ]

        # Low-Risk Countries
        low_risk_countries = [
            "egypt", "mainland", "india", "south korea", "mexico",
            "saudi arabia", "turkey", "uae"
        ]

        # ---- PERFORM ANALYSIS LOGIC HERE
        if country in high_risk_countries:  # TODO INSERT LOGIC
            CR1A["kpi_flag"] = True
            CR1A["kpi_value"] = country
            CR1A["kpi_rating"] = "High"
            CR1A["kpi_details"] = f"The organization in established in {country.upper()}"
        elif country in medium_risk_countries:
            CR1A["kpi_flag"] = True
            CR1A["kpi_value"] = country
            CR1A["kpi_rating"] = "Medium"
            CR1A["kpi_details"] = f"The organization in established in {country.upper()}"
        elif country in low_risk_countries:
            CR1A["kpi_flag"] = True
            CR1A["kpi_value"] = country
            CR1A["kpi_rating"] = "Low"
            CR1A["kpi_details"] = f"The organization in established in {country.upper()}"
        else:
            CR1A["kpi_flag"] = False
            CR1A["kpi_value"] = country
            CR1A["kpi_rating"] = "Info"
            CR1A["kpi_details"] = f"The organization in established in {country.upper()}"


        cr_kpis = [CR1A]
        # TODO - have to decide the table
        insert_status = await upsert_kpi("sown",cr_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            logger.info(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            logger.error(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure","info": "database_saving_error"}

    except Exception as e:
        logger.error(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}
