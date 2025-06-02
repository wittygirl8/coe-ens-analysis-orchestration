import asyncio
import requests
from datetime import datetime
from app.core.utils.db_utils import *
import os
import json
from app.core.config import get_settings
from collections import defaultdict
import io
from app.schemas.logger import logger


async def format_json_report(data, session):
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")
    main_report_json = {}

    initial_info = {
        "ens_id": ens_id_value,
        "session_id": session_id_value
    }
    logger.debug("checkpoint 1")
    main_report_json.update(initial_info)
    # GET COMPANY PROFILE
    copr_required_cols = ["employee", "name", "location", "address", 'website', 'active_status', 'operation_type',
                          'legal_status', 'national_identifier', 'alias', 'incorporation_date', 'revenue',
                          'subsidiaries', 'corporate_group', 'shareholders', 'key_executives', 'employee',
                          "create_time"]
    copr = await get_dynamic_ens_data('company_profile', copr_required_cols, ens_id_value, session_id_value, session)
    copr = copr[0]
    main_report_json.update(copr)
    logger.debug("checkpoint 2")
    # GET UPLOAD METADATA
    upload_meta_cols = ["unmodified_name", "unmodified_city", "unmodified_country", "unmodified_address",
                        "unmodified_national_id"]
    meta_cols = await get_dynamic_ens_data("upload_supplier_master_data", upload_meta_cols, ens_id_value,
                                           session_id_value, session)
    meta_cols = meta_cols[0]
    logger.debug(meta_cols)
    upload_metadata = {
        "upload_metadata": meta_cols
    }
    main_report_json.update(upload_metadata)
    logger.debug("checkpoint 3")

    # GET KPIS
    required_columns = ["kpi_area", "kpi_code", "kpi_definition", "kpi_rating", "kpi_flag", "kpi_details"]
    kpi_table_name = ['cyes', 'fstb', 'lgrk', 'oval', 'rfct', 'sape', 'sown', 'news']  # news
    logger.debug("checkpoint 4")
    gather_all_kpis = []
    for table_name in kpi_table_name:
        res_kpis = await get_dynamic_ens_data(table_name, required_columns, ens_id_value, session_id_value, session)
        gather_all_kpis.extend(res_kpis)
    # print(gather_all_kpis)
    # Create a defaultdict to group by kpi_area
    grouped_data = defaultdict(list)
    # Loop through each dictionary and group by 'kpi_area'
    for item in gather_all_kpis:
        grouped_data[item['kpi_area']].append(item)

    # Convert defaultdict to a regular dict (optional) for the final output
    screening_kpis_dict = dict(grouped_data)
    screening_kpis = {
        "screening_kpis": screening_kpis_dict
    }
    main_report_json.update(screening_kpis)
    logger.debug("checkpoint 5")
    aggregated_ratings = {}
    for area, kpis_list in screening_kpis_dict.items():  # this is the compiled all kpis
        # key = kpi_area, value = list of kpis for area
        all_area_ratings = [d.get("kpi_rating") for d in kpis_list]
        if "High" in all_area_ratings:
            aggregated_ratings[area] = "High"
        elif "Medium" in all_area_ratings:
            aggregated_ratings[area] = "Medium"
        else:
            aggregated_ratings[area] = "Low"

    # logger.info(json.dumps(aggregated_ratings, indent=4))
    aggregated_ratings = {
        "aggregated_ratings": aggregated_ratings
    }
    main_report_json.update(aggregated_ratings)

    # Add Overall Rating:
    required_columns = ["kpi_area", "kpi_code", "kpi_definition", "kpi_rating", "update_time"]
    res_ratings = await get_dynamic_ens_data("ovar", required_columns, ens_id_value, session_id_value, session)
    overall_rating = ""
    theme_ratings = {}
    update_time = ""
    for rating_row in res_ratings:
        if rating_row.get("kpi_area", "") == "overall_rating" and rating_row.get("kpi_code", "") == "supplier":
            overall_rating = rating_row.get("kpi_rating", "")
            update_time = rating_row.get("update_time")
        elif rating_row.get("kpi_area", "") == "theme_rating":
            if rating_row.get("kpi_rating", "").lower() != "deactivated":
                if not (rating_row.get("kpi_code", "") in ["esg", "cyber", "web"]):
                    theme_ratings.update({
                        rating_row.get("kpi_code", "").replace(" ", "_"): rating_row.get("kpi_rating", "")
                    })

    # Add Supplier Overall Rating
    main_report_json.update({
        "overall_rating": overall_rating,
        "update_time": update_time
    })

    try:
        # Add Theme Ratings - Additional
        theme_ratings_dict = {
            "theme_ratings": theme_ratings
        }
        main_report_json.update(theme_ratings_dict)

        # Add Theme Mappings and Static Info - Additional
        static_info = {
            "theme_mapping": {
                "sanctions": ["SAN"],
                "government_political": ["PEP", "SCO"],
                "bribery_corruption_overall": ["BCF"],
                "financials": ["FIN", "BKR"],
                "other_adverse_media": ["NWS", "AMR", "AMO", "ONF"],
                "additional_indicator": ["CYB", "ESG", "WEB"]
            },
            "static_source": ["Source: EY Network Alliance Databases"],
            "summary": {
                "overall": "",
                "sanctions": "",
                "government_political": "",
                "bribery_corruption_overall": "",
                "financials": "",
                "other_adverse_media": "",
                "additional_indicator": ""
            },
        }
        main_report_json.update(static_info)
    except Exception as e:
        logger.error(f"ERROR IN TPRP FIELDS -> REPORT.JSON {str(e)}")

    buffer = io.BytesIO()
    json_bytes = json.dumps(main_report_json, default=str).encode('utf-8')
    buffer.write(json_bytes)
    buffer.seek(0)

    return buffer
