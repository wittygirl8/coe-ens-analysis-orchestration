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




