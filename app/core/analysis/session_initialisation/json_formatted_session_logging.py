import asyncio
import requests
from datetime import datetime
from app.core.utils.db_utils import *
import os
import json
from app.core.config import get_settings
from collections import defaultdict
from app.models import STATUS
import io
import csv


async def format_json_log(session_id_value, session):

    session_status = await get_session_screening_status_static(session_id_value, SessionFactory())
    session_status = session_status[0] #get main dict

    ens_id_all_status = await get_all_ensid_screening_status_static(session_id_value, SessionFactory())  # list

    session_status.update({
        "ens_id_status":ens_id_all_status
    })

    failed_ens_ids = [d for d in ens_id_all_status if d.get("overall_status") == STATUS.FAILED]

    error_messages = {
        "overall_status": "Error - failure in pipeline",
        "orbis_retrieval_status": "ERROR: Data retrieval failed.",
        "screening_modules_status": "ERROR: Report generation failed.",
        "report_generation_status": "ERROR: Report generation failed.",

    }

    all_error_logs = []
    for ens in failed_ens_ids:
        error_logs = []
        if ens["overall_status"] == "FAILED":
            error_logs.append(error_messages["overall_status"])
        if ens["report_generation_status"] == "FAILED":
            error_logs.append(error_messages["report_generation_status"])

        if error_logs:  # Add to result only if there are errors
            all_error_logs.append({
                "ens_id": ens["ens_id"],
                "error_logs": error_logs
            })

    session_status.update({
        "error_logs" : all_error_logs
    })
    # print(json.dumps(session_status, indent=4, default=str))
    #
    # with open("error_logs.json", 'w') as fp:
    #     json.dump(session_status,fp, default=str)

    #Changes by prakruthi
    buffer = io.BytesIO()
    json_bytes = json.dumps(session_status, default=str).encode('utf-8')
    buffer.write(json_bytes)
    buffer.seek(0)

    return buffer


async def format_csv_report(data, session):
    session_id_value = data
    main_report_json = {}
    upload_meta_cols = ["ens_id"]
    ens_id = await get_dynamic_ens_data("upload_supplier_master_data", upload_meta_cols,None,
                                           session_id_value, session)
    # print(f"data type{ens_id}, \n {type(ens_id)}")
    meta_cols=[]
    for ens in ens_id:
        meta_cols.extend(await get_join_dynamic_ens_data("upload_supplier_master_data", "supplier_master_data",ens["ens_id"], session_id_value, session))
    meta_cols = [
        {clean_key(key): value for key, value in row.items()}
        for row in meta_cols
    ]

    # print(f"meta_cols-----{meta_cols}")
    fieldnames = list(meta_cols[0].keys()) if meta_cols else []
    buffer = io.StringIO()
    csv_writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction='ignore')

    csv_writer.writeheader()
    for row in meta_cols:
        csv_writer.writerow(row)

    # Convert StringIO to BytesIO to return as binary
    byte_buffer = io.BytesIO(buffer.getvalue().encode('utf-8-sig'))
    byte_buffer.seek(0)

    return byte_buffer

def clean_key(key: str) -> str:
    # Remove prefix like "left." or "right."
    key = key.split('.')[-1]
    # Replace underscores with spaces and capitalize each word
    return key.replace('_', ' ').title()