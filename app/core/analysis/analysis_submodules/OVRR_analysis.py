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
        # ------------------------- SANCTIONS & PEP
        required_columns = ["kpi_area", "kpi_code", "kpi_rating"]

        sape = await get_dynamic_ens_data("sape", required_columns, ens_id_value, session_id_value, session)
        sape_high_trigger = False
        sape_medium_trigger = False
        for row in sape:
            if "High" in row.get('kpi_rating'):
                sape_high_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                sape_medium_trigger = True
                break
        sanctions_rating = "High" if sape_high_trigger else "Medium" if sape_medium_trigger else "Low"

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
        for row in esg:
            if "High" in row.get('kpi_rating'):
                esg_medium_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                esg_medium_trigger = True
                break
        esg_rating = "Medium" if esg_medium_trigger else "Low"

        esg_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "esg",
            "kpi_rating": esg_rating
        }

        # ------------------------- CYBER
        cyb_medium_trigger = False
        for row in cyb:
            if "High" in row.get('kpi_rating'):
                cyb_medium_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                cyb_medium_trigger = True
                break
        cyb_rating = "Medium" if cyb_medium_trigger else "Low"

        cyber_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "cyber",
            "kpi_rating": cyb_rating
        }

        web_medium_trigger = False
        web_high_trigger = False
        for row in web:
            if "High" in row.get('kpi_rating'):
                web_high_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                web_medium_trigger = True
                break
        web_rating = "High" if web_high_trigger else "Medium" if web_medium_trigger else "Low"

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
        else:
            additional_indicator_rating = "Low"

        additional_indicator_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "additional indicator",
            "kpi_rating": additional_indicator_rating
        }

        # ------------------------- FINANCIALS

        fin = await get_dynamic_ens_data("fstb", required_columns, ens_id_value, session_id_value, session)
        fin_high_trigger = False
        fin_medium_trigger = False
        for row in fin:
            if "High" in row.get('kpi_rating'):
                fin_high_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                fin_medium_trigger = True
                break
        fin_rating = "High" if fin_high_trigger else "Medium" if fin_medium_trigger else "Low"

        financials_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "financials",
            "kpi_rating": fin_rating
        }

        # ---------------------------------------------------------------- RFCT & LGRK

        rfct = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)
        news = await get_dynamic_ens_data("news",required_columns, ens_id_value, session_id_value, session)
        amo_amr = [d for d in rfct if ((d['kpi_area'] == "AMO") or (d['kpi_area'] == "AMR" ))]
        bcf = [d for d in rfct if d['kpi_area'] == "BCF"]
        reg = [d for d in rfct if d['kpi_area'] == "REG"]
        onf = [d for d in news if d['kpi_area'] == "ONF"]
        amo_amr_onf = amo_amr + onf


        lgrk = await get_dynamic_ens_data("lgrk", required_columns, ens_id_value, session_id_value, session)


        # ------------------------- RFCT: ADVERSE MEDIA

        am_high_trigger = False
        am_medium_trigger = False
        for row in amo_amr_onf:
            if "High" in row.get('kpi_rating'):
                am_high_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                am_medium_trigger = True
                break
        am_rating = "High" if am_high_trigger else "Medium" if am_medium_trigger else "Low"

        other_adverse_media_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "other_adverse_media",
            "kpi_rating": am_rating
        }

        # ------------------------- RFCT: BCF

        bcf_high_trigger = False
        bcf_medium_trigger = False
        for row in bcf:
            if "High" in row.get('kpi_rating'):
                bcf_high_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                bcf_medium_trigger = True
                break
        bcf_rating = "High" if bcf_high_trigger else "Medium" if bcf_medium_trigger else "Low"

        bribery_corruption_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "bribery_corruption_overall",
            "kpi_rating": bcf_rating
        }

        # ------------------------- RFCT: REG
        reg_leg = reg+lgrk
        reg_high_trigger = False
        reg_medium_trigger = False
        for row in reg_leg:
            if "High" in row.get('kpi_rating'):
                reg_high_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                reg_medium_trigger = True
                break
        reg_rating = "High" if reg_high_trigger else "Medium" if reg_medium_trigger else "Low"

        regulatory_legal_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "regulatory_legal",
            "kpi_rating": reg_rating
        }

        # ------------------------- GOVERNMENT & POLITICAL
        sown = await get_dynamic_ens_data("sown", required_columns, ens_id_value, session_id_value, session)

        gov_high_trigger = False
        gov_medium_trigger = False
        for row in sown:
            if "High" in row.get('kpi_rating'):
                gov_high_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                gov_medium_trigger = True
                break
        gov_rating = "High" if gov_high_trigger else "Medium" if gov_medium_trigger else "Low"

        government_political_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "government_political",
            "kpi_rating": gov_rating
        }

        #-----------------------------

        oval = await get_dynamic_ens_data("oval", required_columns, ens_id_value, session_id_value, session)
        oval_high_trigger = False
        oval_medium_trigger = False
        for row in oval:
            if "High" in row.get('kpi_rating'):
                oval_high_trigger = True
                break
            elif "Medium" in row.get('kpi_rating'):
                oval_medium_trigger = True
                break
        ownership_rating = "High" if oval_high_trigger else "Medium" if oval_medium_trigger else "Low"

        ownership_overall = {
            "kpi_area": "theme_rating",
            "kpi_code": "ownership",
            "kpi_rating": ownership_rating
        }


        ovr_kpis = [government_political_overall, regulatory_legal_overall, bribery_corruption_overall,
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