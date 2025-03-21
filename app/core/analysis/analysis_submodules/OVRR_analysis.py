import asyncio
import requests
from datetime import datetime
from app.core.utils.db_utils import *
import os
import json
from app.core.config import get_settings
async def ovrr(data, session):

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    kpi_area_module = "OVR"
    try:
        # ------------------------- SANCTIONS
        required_columns = ["kpi_area", "kpi_code", "kpi_rating", "kpi_flag"]

        sape = await get_dynamic_ens_data("sape", required_columns, ens_id_value, session_id_value, session)
        sape_high_trigger = False
        sape_medium_trigger = False
        sape_low_trigger = False
        for row in sape:
            print(row)
            if row.get("kpi_flag"):
                if "High" in row.get('kpi_rating'):
                    print("high")
                    sape_high_trigger = True
                    break
                elif "Medium" in row.get('kpi_rating'):
                    print("medium")
                    sape_medium_trigger = True
                elif "Low" in row.get('kpi_rating'):
                    print("low")
                    sape_low_trigger = True
        sanctions_rating = "High" if sape_high_trigger else "Medium" if sape_medium_trigger else "Low" if sape_low_trigger else "No Alerts"

        sanctions_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "sanctions",
            "kpi_rating": sanctions_rating
        }

        # ------------------------- ESG & CYBER

        esg_cyber = await get_dynamic_ens_data("cyes", required_columns, ens_id_value, session_id_value, session)

        esg = [d for d in esg_cyber if d['kpi_area'] == "ESG"]
        cyb = [d for d in esg_cyber if d['kpi_area'] == "CYB"]
        web = [d for d in esg_cyber if d['kpi_area'] == "WEB"]

        # ------------------------- ESG
        esg_medium_trigger = False
        esg_low_trigger = False
        for row in esg:
            if "High" in row.get('kpi_rating'):
                esg_medium_trigger = True
            elif "Medium" in row.get('kpi_rating'):
                esg_medium_trigger = True
            elif "Low" in row.get('kpi_rating'):
                esg_low_trigger = True
        esg_rating = "Medium" if esg_medium_trigger else "Low" if esg_low_trigger else "No Alerts"

        esg_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "esg",
            "kpi_rating": esg_rating
        }

        # ------------------------- CYBER
        cyb_medium_trigger = False
        cyb_low_trigger = False
        for row in cyb:
            if "High" in row.get('kpi_rating'):
                cyb_medium_trigger = True
            elif "Medium" in row.get('kpi_rating'):
                cyb_medium_trigger = True
            elif "Low" in row.get('kpi_rating'):
                cyb_low_trigger = True
        cyb_rating = "Medium" if cyb_medium_trigger else "Low" if cyb_low_trigger else "No Alerts"

        cyber_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "cyber",
            "kpi_rating": cyb_rating
        }

        web_medium_trigger = False
        web_high_trigger = False
        for row in web:
            if row.get("kpi_flag"):
                if "High" in row.get('kpi_rating'):
                    web_high_trigger = True
                    break
                elif "Medium" in row.get('kpi_rating'):
                    web_medium_trigger = True
        web_rating = "High" if web_high_trigger else "Medium" if web_medium_trigger else "No Alerts"

        web_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "web",
            "kpi_rating": web_rating
        }

        cyes_kpis = [esg_overall, cyber_overall, web_overall]

        compile_section_ratings = [d.get("kpi_rating", "") for d in cyes_kpis]

        if "High" in compile_section_ratings:  # Any presence of High
            additional_indicator_rating = "High"
        elif "Medium" in compile_section_ratings:  # Any presence of Medium (No High)
            additional_indicator_rating = "Medium"
        elif "Low" in compile_section_ratings:  # Any presence of Medium (No High)
            additional_indicator_rating = "Low"
        else:
            additional_indicator_rating = "No Alerts"

        additional_indicator_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "additional indicator",
            "kpi_rating": additional_indicator_rating
        }

        # ------------------------- FINANCIALS

        fin = await get_dynamic_ens_data("fstb", required_columns, ens_id_value, session_id_value, session)
        fin_high_trigger = False
        fin_medium_trigger = False
        fin_low_trigger = False
        for row in fin:
            if "High" in row.get('kpi_rating'):
                fin_high_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                fin_medium_trigger = True
            elif "Low" in row.get('kpi_rating'):
                fin_low_trigger = True
        fin_rating = "High" if fin_high_trigger else "Medium" if fin_medium_trigger else "Low" if fin_low_trigger else "No Alerts"

        fin_rating = "No Alerts" if (len(fin) == 0) else fin_rating

        financials_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "financials",
            "kpi_rating": fin_rating
        }

        print("financial rating:",fin_rating)

        # ---------------------------------------------------------------- RFCT & LGRK

        rfct = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)
        news = await get_dynamic_ens_data("news",required_columns, ens_id_value, session_id_value, session)
        amo_amr = [d for d in rfct if ((d['kpi_area'] == "AMO") or (d['kpi_area'] == "AMR" ))]
        bcf = [d for d in rfct if d['kpi_area'] == "BCF"]
        reg = [d for d in rfct if d['kpi_area'] == "REG"]
        amo_amr_onf_2cents = amo_amr + news


        lgrk = await get_dynamic_ens_data("lgrk", required_columns, ens_id_value, session_id_value, session)


        # ------------------------- RFCT: ADVERSE MEDIA

        am_high_trigger = False
        am_medium_trigger = False
        am_low_trigger = False
        for row in amo_amr_onf_2cents:
            if row.get("kpi_flag"):
                if "High" in row.get('kpi_rating'):
                    am_high_trigger = True
                    break
                elif "Medium" in row.get('kpi_rating'):
                    am_medium_trigger = True
                else:
                    am_low_trigger = True
        am_rating = "High" if am_high_trigger else "Medium" if am_medium_trigger else "Low" if am_low_trigger else "No Alerts"

        other_adverse_media_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "other_adverse_media",
            "kpi_rating": am_rating
        }

        # ------------------------- RFCT: BCF

        bcf_high_trigger = False
        bcf_medium_trigger = False
        bcf_low_trigger = False
        for row in bcf:
            if row.get("kpi_flag"):
                if "High" in row.get('kpi_rating'):
                    bcf_high_trigger = True
                    break
                elif "Medium" in row.get('kpi_rating'):
                    bcf_medium_trigger = True
                else:
                    bcf_low_trigger = True
        bcf_rating = "High" if bcf_high_trigger else "Medium" if bcf_medium_trigger else "Low" if bcf_low_trigger else "No Alerts"

        bribery_corruption_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "bribery_corruption_overall",
            "kpi_rating": bcf_rating
        }

        # ------------------------- RFCT: REG
        reg_leg = reg+lgrk
        reg_high_trigger = False
        reg_medium_trigger = False
        reg_low_trigger = False
        for row in reg_leg:
            if row.get("kpi_flag"):
                if "High" in row.get('kpi_rating'):
                    reg_high_trigger = True
                    break
                elif "Medium" in row.get('kpi_rating'):
                    reg_medium_trigger = True
                else:
                    reg_low_trigger = True
        reg_rating = "High" if reg_high_trigger else "Medium" if reg_medium_trigger else "Low" if reg_low_trigger else "No Alerts"

        regulatory_legal_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "regulatory_legal",
            "kpi_rating": "Deactivated"
        }

        # ------------------------- GOVERNMENT & POLITICAL
        sown = await get_dynamic_ens_data("sown", required_columns, ens_id_value, session_id_value, session)

        gov_high_trigger = False
        gov_medium_trigger = False
        gov_low_trigger = False
        for row in sown:
            if row.get("kpi_flag"):
                if "High" in row.get('kpi_rating'):
                    gov_high_trigger = True
                    break
                elif "Medium" in row.get('kpi_rating'):
                    gov_medium_trigger = True
                else:
                    gov_low_trigger = True
        gov_rating = "High" if gov_high_trigger else "Medium" if gov_medium_trigger else "Low" if gov_low_trigger else "No Alerts"

        government_political_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "government_political",
            "kpi_rating": gov_rating
        }

        #-----------------------------

        oval = await get_dynamic_ens_data("oval", required_columns, ens_id_value, session_id_value, session)
        oval_high_trigger = False
        oval_medium_trigger = False
        oval_low_trigger = False
        for row in oval:
            if row.get("kpi_flag"):
                if "High" in row.get('kpi_rating'):
                    oval_high_trigger = True
                    break
                elif "Medium" in row.get('kpi_rating'):
                    oval_medium_trigger = True
                else:
                    oval_low_trigger = True
        ownership_rating = "High" if oval_high_trigger else "Medium" if oval_medium_trigger else "Low" if oval_low_trigger else "No Alerts"

        ownership_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "ownership",
            "kpi_rating": "Deactivated"
        }


        ovr_kpis = [government_political_overall, bribery_corruption_overall, regulatory_legal_overall,
                      other_adverse_media_overall, financials_overall, additional_indicator_overall,esg_overall,web_overall, cyber_overall, sanctions_overall, ownership_overall]

        compile_section_ratings = [d.get("kpi_rating","") for d in ovr_kpis]

        if "High" in compile_section_ratings: # Any presence of High
            supplier_rating = "High"
        elif "Medium" in compile_section_ratings:  # Any presence of Medium (No High)
            supplier_rating = "Medium"
        else:
            supplier_rating = "Low"

        supplier_overall = {
            "kpi_area": "overall_rating",
            "kpi_code": "supplier",
            "kpi_rating": supplier_rating
        }

        ovr_kpis.append(supplier_overall)

        insert_status = await upsert_kpi("ovar", ovr_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure","info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}, {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}