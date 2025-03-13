import asyncio
from app.core.utils.db_utils import *


async def esg_analysis(data, session):

    print("Performing ESG Analysis...")

    kpi_area_module = "ESG"

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

        ESG1A = kpi_template.copy()
        ESG2A = kpi_template.copy()
        ESG2B = kpi_template.copy()
        ESG2C = kpi_template.copy()

        ESG1A["kpi_code"] = "ESG1A"
        ESG1A["kpi_definition"] = "ESG Overall Score"

        ESG2A["kpi_code"] = "ESG2A"
        ESG2A["kpi_definition"] = "ESG - Environmental Score"

        ESG2B["kpi_code"] = "ESG2B"
        ESG2B["kpi_definition"] = "ESG - Social Score"

        ESG2C["kpi_code"] = "ESG2C"
        ESG2C["kpi_definition"] = "ESG - Governance Score"


        required_columns = ["esg_environmental_rating", "esg_social_rating", "esg_governance_rating", "esg_overall_rating", "esg_date"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        esg_score = retrieved_data.get("esg_overall_rating", None)
        env_score = retrieved_data.get("esg_environmental_rating", None)
        gov_score = retrieved_data.get("esg_governance_rating", None)
        soc_score = retrieved_data.get("esg_social_rating", None)
        esg_date = retrieved_data.get("esg_date", None)

        # Check if all/any mandatory required data is None - and return
        if all(var is None for var in [esg_score, env_score, gov_score, soc_score, soc_score]):
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        # ESG1A - ESG Rating Based on Overall ESG Score # TODO ADD A FALLBACK IF OTHER env, soc, gov SCORES AVAILABLE AND THIS ISNT

        if esg_score is None:
            ESG1A["kpi_flag"] = False
            ESG1A["kpi_value"] = ""
            ESG1A["kpi_rating"] = "INFO"
            ESG1A["kpi_details"] = f"No rating available"
        elif esg_score <= 29:
            ESG1A["kpi_flag"] = True
            ESG1A["kpi_value"] = str(esg_score)
            ESG1A["kpi_rating"] = "High"
            ESG1A["kpi_details"] = f"Weak Overall ESG Score: {esg_score}"
        elif 30 <= esg_score <= 49:
            ESG1A["kpi_flag"] = True
            ESG1A["kpi_value"] = str(esg_score)
            ESG1A["kpi_rating"] = "Medium"
            ESG1A["kpi_details"] = f"Medium Overall ESG Score: {esg_score}"
        elif esg_score >= 50:
            ESG1A["kpi_flag"] = False
            ESG1A["kpi_value"] = str(esg_score)
            ESG1A["kpi_rating"] = "Low"
            ESG1A["kpi_details"] = f"Strong Overall ESG Score: {esg_score}"


        # ESG2A - Environment Rating Based on Env Score
        if env_score is None:
            ESG2A["kpi_flag"] = False
            ESG2A["kpi_value"] = ""
            ESG2A["kpi_rating"] = "INFO"
            ESG2A["kpi_details"] = f"No rating available"
        elif env_score <= 29:
            ESG2A["kpi_flag"] = True
            ESG2A["kpi_value"] = str(env_score)
            ESG2A["kpi_rating"] = "High"
            ESG2A["kpi_details"] = f"Weak Environmental Score: {env_score}"
        elif 30 <= env_score <= 49:
            ESG2A["kpi_flag"] = True
            ESG2A["kpi_value"] = str(env_score)
            ESG2A["kpi_rating"] = "Medium"
            ESG2A["kpi_details"] = f"Medium Environmental Score: {env_score}"
        elif env_score >= 50:
            ESG2A["kpi_flag"] = False
            ESG2A["kpi_value"] = str(env_score)
            ESG2A["kpi_rating"] = "Low"
            ESG2A["kpi_details"] = f"Strong Environmental Score: {env_score}"

        # ESG2B - Social Rating Based on Soc Score
        if soc_score is None:
            ESG2B["kpi_flag"] = False
            ESG2B["kpi_value"] = ""
            ESG2B["kpi_rating"] = "INFO"
            ESG2B["kpi_details"] = f"No rating available"
        elif soc_score <= 29:
            ESG2B["kpi_flag"] = True
            ESG2B["kpi_value"] = str(soc_score)
            ESG2B["kpi_rating"] = "High"
            ESG2B["kpi_details"] = f"Weak Social Score: {soc_score}"
        elif 30 <= soc_score <= 49:
            ESG2B["kpi_flag"] = True
            ESG2B["kpi_value"] = str(soc_score)
            ESG2B["kpi_rating"] = "Medium"
            ESG2B["kpi_details"] = f"Medium Social Score: {soc_score}"
        elif soc_score >= 50:
            ESG2B["kpi_flag"] = False
            ESG2B["kpi_value"] = str(soc_score)
            ESG2B["kpi_rating"] = "Low"
            ESG2B["kpi_details"] = f"Strong Social Score: {soc_score}"

        # ESG2C - Governance Rating Based on Gov Score
        if gov_score is None:
            ESG2C["kpi_flag"] = False
            ESG2C["kpi_value"] = ""
            ESG2C["kpi_rating"] = "INFO"
            ESG2C["kpi_details"] = f"No rating available"
        elif gov_score <= 29:
            ESG2C["kpi_flag"] = True
            ESG2C["kpi_value"] = str(gov_score)
            ESG2C["kpi_rating"] = "High"
            ESG2C["kpi_details"] = f"Weak Governance Score: {gov_score}"
        elif 30 <= gov_score <= 49:
            ESG2C["kpi_flag"] = True
            ESG2C["kpi_value"] = str(gov_score)
            ESG2C["kpi_rating"] = "Medium"
            ESG2C["kpi_details"] = f"Medium Governance Score: {gov_score}"
        elif gov_score >= 50:
            ESG2C["kpi_flag"] = False
            ESG2C["kpi_value"] = str(gov_score)
            ESG2C["kpi_rating"] = "Low"
            ESG2C["kpi_details"] = f"Strong Governance Score: {gov_score}"

        esg_kpis = [ESG1A, ESG2A, ESG2B, ESG2C]

        print(f"Completed processing {len(esg_kpis)} kpis")

        # TBD: Do we insert blank KPIs as well - currently using
        insert_status = await upsert_kpi("cyes", esg_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}

async def cyber_analysis(data, session):

    print("Performing Cyber Risk Analysis...")
    kpi_area_module = "CYB"

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

        CYB1A = kpi_template.copy()
        CYB1B = kpi_template.copy()
        CYB2A = kpi_template.copy()

        CYB1A["kpi_code"] = "CYB1A"
        CYB1A["kpi_definition"] = "Cyber Risk Rating Score"

        CYB1B["kpi_code"] = "CYB1B"
        CYB1B["kpi_definition"] = "Implied Cyber Risk Rating Score"

        CYB2A["kpi_code"] = "CYB2A"
        CYB2A["kpi_definition"] = "Overall Cyber Risk Rating"

        required_columns = ["cyber_risk_score", "cyber_date", "implied_cyber_risk_score", "implied_cyber_risk_score_date", "cyber_bonet_infection", "cyber_malware_servers", "cyber_ssl_certificate", "cyber_webpage_headers"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        cyber_rating = retrieved_data.get("cyber_risk_score")
        cyber_date = retrieved_data.get("cyber_date")
        implied_cyber_risk = retrieved_data.get("implied_cyber_risk_score")
        implied_cyber_risk_score_date = retrieved_data.get("implied_cyber_risk_score_date")
        cyber_bonet_infection = retrieved_data.get("cyber_bonet_infection")  # TODO minor: this is botnet
        cyber_malware_servers = retrieved_data.get("cyber_malware_servers")
        cyber_ssl_certificate = retrieved_data.get("cyber_ssl_certificate")
        cyber_webpage_headers = retrieved_data.get("cyber_webpage_headers")

        # Check if all/any mandatory required data is None - (if so then add one general?) and return
        if all(var is None for var in [cyber_rating, implied_cyber_risk]):
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        # CYB1A - Cyber Rating Based on Overall Cyber Rating (CYBER)
        if cyber_rating is not None:
            if cyber_rating <= 650:
                CYB1A["kpi_flag"] = True
                CYB1A["kpi_value"] = str(cyber_rating)
                CYB1A["kpi_rating"] = "High"
                CYB1A["kpi_details"] = f"High Cyber Risk Rating: {cyber_rating}"
            elif 650 < cyber_rating <= 750:
                CYB1A["kpi_flag"] = True
                CYB1A["kpi_value"] = str(cyber_rating)
                CYB1A["kpi_rating"] = "Medium"
                CYB1A["kpi_details"] = f"Medium Cyber Risk Rating: {cyber_rating}"
            elif 750 < cyber_rating <= 900:
                CYB1A["kpi_flag"] = False
                CYB1A["kpi_value"] = str(cyber_rating)
                CYB1A["kpi_rating"] = "Low"
                CYB1A["kpi_details"] = f"Low Cyber Risk Rating: {cyber_rating}"
        else:
            CYB1A["kpi_flag"] = False
            CYB1A["kpi_value"] = ""
            CYB1A["kpi_rating"] = "INFO"
            CYB1A["kpi_details"] = "No Cyber Risk Rating available"

        # CYB1B - Implied Cyber Risk Rating Based on Implied Risk (IMPLIED)
        if implied_cyber_risk is not None:
            if "very high" in implied_cyber_risk.lower():
                CYB1B["kpi_flag"] = True
                CYB1B["kpi_value"] = str(implied_cyber_risk)
                CYB1B["kpi_rating"] = "High"
                CYB1B["kpi_details"] = f"High Implied Cyber Risk"
            elif "high" in implied_cyber_risk.lower():
                CYB1B["kpi_flag"] = True
                CYB1B["kpi_value"] = str(implied_cyber_risk)
                CYB1B["kpi_rating"] = "High"
                CYB1B["kpi_details"] = f"High Implied Cyber Risk"
            elif "medium" in implied_cyber_risk.lower():
                CYB1B["kpi_flag"] = True
                CYB1B["kpi_value"] = str(implied_cyber_risk)
                CYB1B["kpi_rating"] = "Medium"
                CYB1B["kpi_details"] = f"Medium Implied Cyber Risk"
            elif "low" in implied_cyber_risk.lower():
                CYB1B["kpi_flag"] = False
                CYB1B["kpi_value"] = str(implied_cyber_risk)
                CYB1B["kpi_rating"] = "Low"
                CYB1B["kpi_details"] = f"Low Implied Cyber Risk"
        else:
            CYB1B["kpi_flag"] = False
            CYB1B["kpi_value"] = ""
            CYB1B["kpi_rating"] = "INFO"
            CYB1B["kpi_details"] = "No Implied Cyber Risk Available"

        # CYB2A - Final Cyber Risk Rating Based on CYB1A (Cyber Rating) and CYB1B (Implied Risk)
        if CYB1A["kpi_rating"] == "High":
            CYB2A["kpi_flag"] = True
            CYB2A["kpi_value"] = ""
            CYB2A["kpi_rating"] = "High"
            CYB2A["kpi_details"] = "Final Cyber Risk Rating: High"
        elif CYB1A["kpi_rating"] == "Medium":
            CYB2A["kpi_flag"] = True
            CYB2A["kpi_value"] = ""
            CYB2A["kpi_rating"] = "Medium"
            CYB2A["kpi_details"] = "Final Cyber Risk Rating: Medium"
        elif CYB1A["kpi_rating"] == "Low":
            if CYB1B["kpi_rating"] == "High" or CYB1B["kpi_rating"] == "Medium":
                CYB2A["kpi_flag"] = True
                CYB2A["kpi_value"] = ""
                CYB2A["kpi_rating"] = "Medium"
                CYB2A["kpi_details"] = "Final Cyber Risk Rating: Medium"
            elif CYB1B["kpi_rating"] == "Low":
                CYB2A["kpi_flag"] = False
                CYB2A["kpi_value"] = ""
                CYB2A["kpi_rating"] = "Low"
                CYB2A["kpi_details"] = "Final Cyber Risk Rating: Low"
        else:
            # If CYB1A (Cyber Rating) is not available, use CYB1B (Implied Risk) for the final rating
            if CYB1B["kpi_rating"] == "High":
                CYB2A["kpi_flag"] = True
                CYB2A["kpi_value"] = ""
                CYB2A["kpi_rating"] = "High"
                CYB2A["kpi_details"] = "Final Cyber Risk Rating: High (Based on Implied Risk)"
            elif CYB1B["kpi_rating"] == "Medium":
                CYB2A["kpi_flag"] = True
                CYB2A["kpi_value"] = ""
                CYB2A["kpi_rating"] = "Medium"
                CYB2A["kpi_details"] = "Final Cyber Risk Rating: Medium (Based on Implied Risk)"
            elif CYB1B["kpi_rating"] == "Low":
                CYB2A["kpi_flag"] = False
                CYB2A["kpi_value"] = ""
                CYB2A["kpi_rating"] = "Low"
                CYB2A["kpi_details"] = "Final Cyber Risk Rating: Low (Based on Implied Risk)"
            else:
                CYB2A["kpi_flag"] = False
                CYB2A["kpi_value"] = ""
                CYB2A["kpi_rating"] = "INFO"
                CYB2A["kpi_details"] = "No Final Cyber Risk Rating Available"

        cyber_kpis = [CYB1A, CYB1B, CYB2A]
        print(f"Completed processing {len(cyber_kpis)} kpis")

        # Insert KPI into the database
        insert_status = await upsert_kpi("cyes", cyber_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}
