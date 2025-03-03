# app/core/phase1_analysis.py

from app.core.analysis.session_initialisation.session import *
from app.core.analysis.session_initialisation.json_formatted_session_logging import *

from app.core.analysis.analysis_submodules.CYES_analysis import *
from app.core.analysis.analysis_submodules.FSTB_analysis import *
from app.core.analysis.analysis_submodules.LGRK_analysis import *
from app.core.analysis.analysis_submodules.NEWS_analysis import *
from app.core.analysis.analysis_submodules.OVRR_analysis import *
from app.core.analysis.analysis_submodules.OVAL_analysis import *
from app.core.analysis.analysis_submodules.RFCT_analysis import *
from app.core.analysis.analysis_submodules.SOWN_analysis import *
from app.core.analysis.analysis_submodules.SAPE_analysis import *
from app.core.analysis.analysis_submodules.COPR_analysis import *
from app.core.analysis.orbis_submodules.COMPANY_orbis import *
from app.core.analysis.orbis_submodules.GRID_orbis import *
from app.core.analysis.orbis_submodules.GRID_byID import *
from app.core.analysis.orbis_submodules.GRID_byNAME import *
from app.core.analysis.orbis_submodules.MATCH_orbis import *
from app.core.analysis.report_generation_submodules.report import *
from app.core.analysis.report_generation_submodules.json_formatted_report import *
from app.core.analysis.supplier_validation_submodules.supplier_name_validation import *
import logging
from app.core.database_session import _ASYNC_ENGINE, SessionFactory

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG if you want more details
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Ensures logs are printed to the terminal
    ]
)

# Create a logger instance
log = logging.getLogger(__name__)

from app.core.utils.db_utils import *


def batch_generator(all_items_list, batch_size):
    for i in range(0, len(all_items_list), batch_size):
        yield all_items_list[i: i + batch_size]


async def run_supplier_name_validation(data, session):
    log.info(f"<<< RUNNING SVP FOR NEW SESSION >>>")
    log.info(f"API Data: {data} ")
    # 1. session initialisation run
    session_id_value = data.get("session_id")
    log.info(f"SESSION = {session_id_value}")
    # data_for_sessionId = await get_ens_ids_for_session_id("upload_supplier_master_data", session_id=session_id_value, session=session)
    data_for_sessionId = await get_dynamic_ens_data("upload_supplier_master_data", required_columns=["all"],
                                                    ens_id=None, session_id=session_id_value, session=session)
    # log.info("Data for session ID:", data_for_sessionId)

    # Updating the Status value in session_screening_status
    validation = {"supplier_name_validation_status": STATUS.IN_PROGRESS.value}
    update_status = await update_dynamic_ens_data("session_screening_status", validation, ens_id=None,
                                                  session_id=session_id_value, session=session)
    if update_status["status"] == "success":
        log.info("UPDATED status for session = IN_PROGRESS")
    else:
        log.info("Failed to UPDATE status for session = IN_PROGRESS, check db_util parameters")

    # 2. Run Name Validatio Pipeline Concurrently (In Batches of N Concurrent Suppliers)
    batch_size = 2
    batch_data = batch_generator(data_for_sessionId, batch_size)
    # log.info(f"batch data: {batch_data}")
    api_response = []
    count = 0
    for nameval_batch in batch_data:
        for element in nameval_batch:
            # log.info(f"Running for ens ID: {element["ens_id"]}")
            nameval_tasks = [supplier_name_validation(element, session, search_engine="bing")]  # update func params
            nameval_batch_result = await asyncio.gather(*nameval_tasks)
            for runs in range(len(nameval_batch_result)):
                process_status, result = nameval_batch_result[runs]
                api_response.append(result)
                # if process_status:
                #     # TODO: the enum for this needs to be STATUS instead of TruesightStatus
                #     temp = {"process_status":STATUS.COMPLETED.value}
                #     update_status = await update_dynamic_ens_data("upload_supplier_master_data", temp, session=session, ens_id=element["ens_id"], session_id=session_id_value)
                # else:
                #     # TODO: the enum for this needs to be STATUS instead of TruesightStatus
                #     temp = {"process_status":STATUS.FAILED.value}
                #     update_status = await update_dynamic_ens_data("upload_supplier_master_data", temp, session=session, ens_id=element["ens_id"], session_id=session_id_value)

    # Updating the Status value in session_screening_status
    validation = {"supplier_name_validation_status": STATUS.COMPLETED.value}
    update_status = await update_dynamic_ens_data("session_screening_status", validation, ens_id=None,
                                                  session_id=session_id_value, session=session)
    if update_status["status"] == "success":
        log.info("UPDATED status for session = COMPLETED")
    else:
        log.info("Failed to UPDATE status for session = COMPLETED, check db_util parameters")

    return {"overall_process_status": STATUS.COMPLETED.value, "supplier_data": api_response}


async def run_report_generation_standalone(data, session):

    """
    This is the standalone report generation function to be used in its own test endpoint
    Not to be used in deployed analysis pipeline
    :param data:
    :param session: db session
    :return:
    """
    log.info(f"<<< RUNNING RGC FOR NEW SESSION - STANDALONE CALL FOR ONLY RGC>>>")
    log.info(f"API Data: {data} ")
    # 1. session initialisation run
    session_id_value = data.get("session_id")
    log.info(f"SESSION = {session_id_value}")

    # data_for_sessionId = await get_ens_ids_for_session_id("upload_supplier_master_data", session_id=session_id_value, session=session)
    data_for_sessionId = await get_dynamic_ens_data("supplier_master_data", required_columns=["all"],
                                                    ens_id=None, session_id=session_id_value, session=session)  # TODO IDEALLY DONT PULL ALL COLS?
    print("\n\nData for session ID", data_for_sessionId)

    # TODO: Confirm if a col exists for this status
    # Update the status message for report generation
    # validation = {"supplier_name_validation_status": STATUS.IN_PROGRESS.value}
    # update_status = await update_dynamic_ens_data("session_screening_status", validation, ens_id=None, session_id=session_id_value, session=session)
    # if update_status["status"]=="success":
    #     log.info("UPDATED status for session = IN_PROGRESS")
    # else:
    #     log.info("Failed to UPDATE status for session = IN_PROGRESS, check db_util parameters")

    batch_size = 3
    batch_data = batch_generator(data_for_sessionId, batch_size)

    api_response = []
    count = 0
    for nameval_batch in batch_data:
        for element in nameval_batch:
            log.info(f"\n\nRunning for ens ID: {element}")
            nameval_tasks = [report_generation(element, session, ts_data=None, upload_to_blob=True, save_locally=True)]
            nameval_batch_result = await asyncio.gather(*nameval_tasks)
            for runs in range(len(nameval_batch_result)):
                process_status, result = nameval_batch_result[runs]
                api_response.append(result)
                # if process_status:
                #     # TODO: the enum for this needs to be STATUS instead of TruesightStatus
                #     temp = {"process_status":STATUS.COMPLETED.value}
                #     update_status = await update_dynamic_ens_data("upload_supplier_master_data", temp, session=session, ens_id=element["ens_id"], session_id=session_id_value)
                # else:
                #     # TODO: the enum for this needs to be STATUS instead of TruesightStatus
                #     temp = {"process_status":STATUS.FAILED.value}
                #     update_status = await update_dynamic_ens_data("upload_supplier_master_data", temp, session=session, ens_id=element["ens_id"], session_id=session_id_value)

    # TODO: again, check if a col exists for this
    # validation = {"supplier_name_validation_status": STATUS.COMPLETED.value}
    # update_status = await update_dynamic_ens_data("session_screening_status", validation, ens_id=None, session_id=session_id_value, session=session)
    # if update_status["status"]=="success":
    #     log.info("UPDATED status for session = COMPLETED")
    # else:
    #     log.info("Failed to UPDATE status for session = COMPLETED, check db_util parameters")

    return {"overall_process_status": STATUS.COMPLETED.value, "report_data": api_response}

async def run_report_generation_single(data, session):
    log.info(f"<<< RUNNING RGC FOR NEW SESSION >>>")
    log.info(f"API Data: {data} ")
    # 1. session initialisation run
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")
    log.info(f"SESSION = {session_id_value}")

    # data_for_sessionId = await get_ens_ids_for_session_id("upload_supplier_master_data", session_id=session_id_value, session=session)
    data_for_sessionId = await get_dynamic_ens_data("supplier_master_data", required_columns=["all"],
                                                    ens_id=ens_id_value, session_id=session_id_value, session=session)
    print("\n\nData for session ID", data_for_sessionId)

    # TODO: Confirm if a col exists for this status
    # Update the status message for report generation
    # validation = {"supplier_name_validation_status": STATUS.IN_PROGRESS.value}
    # update_status = await update_dynamic_ens_data("session_screening_status", validation, ens_id=None, session_id=session_id_value, session=session)
    # if update_status["status"]=="success":
    #     log.info("UPDATED status for session = IN_PROGRESS")
    # else:
    #     log.info("Failed to UPDATE status for session = IN_PROGRESS, check db_util parameters")

    api_response = []
    process_status, result = await report_generation(data_for_sessionId[0], session, ts_data=None, upload_to_blob=True, save_locally=True)
    api_response.append(result)

    # if process_status:
    #     # TODO: the enum for this needs to be STATUS instead of TruesightStatus
    #     temp = {"process_status":STATUS.COMPLETED.value}
    #     update_status = await update_dynamic_ens_data("upload_supplier_master_data", temp, session=session, ens_id=element["ens_id"], session_id=session_id_value)
    # else:
    #     # TODO: the enum for this needs to be STATUS instead of TruesightStatus
    #     temp = {"process_status":STATUS.FAILED.value}
    #     update_status = await update_dynamic_ens_data("upload_supplier_master_data", temp, session=session, ens_id=element["ens_id"], session_id=session_id_value)

    # TODO: again, check if a col exists for this
    # validation = {"supplier_name_validation_status": STATUS.COMPLETED.value}
    # update_status = await update_dynamic_ens_data("session_screening_status", validation, ens_id=None, session_id=session_id_value, session=session)
    # if update_status["status"]=="success":
    #     log.info("UPDATED status for session = COMPLETED")
    # else:
    #     log.info("Failed to UPDATE status for session = COMPLETED, check db_util parameters")

    return {"overall_process_status": STATUS.COMPLETED.value, "report_data": api_response}


async def run_analysis_tasks(data, session):
    """
     Execute all analysis functions concurrently, then run report generation.
    """

    print("BEGINNING ANALYSIS")
    print(data)

    session_id = data.get("session_id")
    ens_id = data.get("ens_id")
    # List of analysis functions to run concurrently
    analysis_tasks = [
        esg_analysis(data, SessionFactory()),
        cyber_analysis(data, SessionFactory()),
        financials_analysis(data,SessionFactory()),
        bankruptcy_and_financial_risk_analysis(data,SessionFactory()),
        legal_analysis(data,SessionFactory()),
        ownership_analysis(data, SessionFactory()),
        sanctions_analysis(data,SessionFactory()),
        pep_analysis(data,SessionFactory()),
        adverse_media_analysis(data,SessionFactory()),
        adverse_media_reputation_risk(data, SessionFactory()),
        bribery_corruption_fraud_analysis(data,SessionFactory()),
        regulatory_analysis(data, SessionFactory()),
        sown_analysis(data, SessionFactory()),
        company_profile(data, SessionFactory())
    ]

    try:
        # Run all analysis tasks concurrently
        analysis_results = await asyncio.gather(*analysis_tasks)

        # Add logic to switch news screening on/off
        # Look for grid events, if not, take orbis news events, if not, go for 2 sents screening
        # news_screening(data, SessionFactory()) # 2 sents

        ovrr_result = await ovrr(data, SessionFactory())
        print(ovrr_result)

        # / --- UPDATE ENSID STATUS
        ens_ids_rows = [{"ens_id": ens_id, "screening_modules_status": STATUS.COMPLETED}]
        insert_status = await upsert_ensid_screening_status(ens_ids_rows, session_id, SessionFactory())
        # print(insert_status)

    except Exception as e:
        ens_ids_rows = [{"ens_id": ens_id, "screening_modules_status": STATUS.FAILED}]
        insert_status = await upsert_ensid_screening_status(ens_ids_rows, session_id, SessionFactory())
        # print(insert_status)
        return []

    try:

        ens_ids_rows = [{"ens_id": ens_id, "report_generation_status": STATUS.IN_PROGRESS}]
        insert_status = await upsert_ensid_screening_status(ens_ids_rows, session_id, SessionFactory())
        # print(insert_status)

        # Run report generation after all analyses are complete
        report_result = await report_generation(data, session, ts_data=None, upload_to_blob=True, save_locally=False)

        report_json=await format_json_report(data, SessionFactory())
        json_file_name=f"{ens_id}/report.json"
        upload_to_azure_blob(report_json, json_file_name,session_id)

        ens_ids_rows = [{"ens_id": ens_id, "report_generation_status": STATUS.COMPLETED}]
        insert_status = await upsert_ensid_screening_status(ens_ids_rows, session_id, SessionFactory())
        # print(insert_status)

        ens_ids_rows = [{"ens_id": ens_id, "overall_status": STATUS.COMPLETED}]
        insert_status = await upsert_ensid_screening_status(ens_ids_rows, session_id, SessionFactory())
        # print(insert_status)

        # Combine results
        return analysis_results + [report_result]  # TODO Neaten
    except Exception as e:
        ens_ids_rows = [{"ens_id": ens_id, "overall_status": STATUS.FAILED}]
        insert_status = await upsert_ensid_screening_status(ens_ids_rows, session_id, SessionFactory())
        # print(insert_status)

        return []


async def run_orbis(ens_id, session_id, bvd_id, session):
    """
    :param ens_id:
    :param session_id:
    :param bvd_id:
    :param session:
    :return:
    """

    try:

        data = {
            "session_id": session_id,
            "ens_id": ens_id,
            "bvd_id": bvd_id
        }

        print(f"PERFORMING ORBIS RETRIEVAL FOR {ens_id}")

        # 1. ORBIS COMPANY - most of the main fields in external_supplier_data (FOR COMPANY LEVEL)
        company_result = await orbis_company(data, session)
        print(company_result.get("success",""))

        # 2. ORBIS GRID - fields which are "event_" in external_supplier_data (FOR COMPANY LEVEL)
        orbis_grid_result = await orbis_grid_search(data, session)
        print(orbis_grid_result.get("success",""))

        # 3A. GRID-GRID by ID <may not work> (FOR COMPANY LEVEL)
        grid_grid_result = await gridbyid_organisation(data, session)
        print(grid_grid_result.get("success",""))

        grid_msg = grid_grid_result.get("message", "")
        grid_success = grid_grid_result.get("success", False)
        # 3B. (FALLBACK) GRID-GRID by NAME (FOR COMPANY LEVEL)
        if grid_msg == "No event for the particular entity" or not grid_success:
            print("No Initial GRID")
            grid_grid_result = await gridbyname_organisation(data, session)
            print(grid_grid_result.get("success",""))

        # 4. (FALLBACK) GRID-GRID by NAME (FOR PERSON LEVEL)
        grid_grid_result_person = await gridbyname_person(data, session)
        print(grid_grid_result_person.get("success",""))

        # Check combined result
        orbis_result = {"company_result": company_result["status"], "orbis_grid_result": orbis_grid_result["status"]} #update more here if needed

        # / --- UPDATE ENSID STATUS
        status_ens_id = [{"ens_id":ens_id, "orbis_retrieval_status": STATUS.COMPLETED}] #must be list even though just 1 row
        insert_status = await upsert_ensid_screening_status(status_ens_id, session_id, SessionFactory())
        # print(insert_status)
        return orbis_result

    except Exception as e:
        print(f"ERROR IN ORBIS RETRIEVAL FOR {ens_id}: {str(e)}")
        # / --- UPDATE ENSID STATUS
        status_ens_id = [{"ens_id":ens_id, "orbis_retrieval_status": STATUS.FAILED}] #must be list even though just 1 row
        insert_status = await upsert_ensid_screening_status(status_ens_id, session_id, SessionFactory())
        # print(insert_status)
        return {"company_result": STATUS.FAILED , "orbis_grid_result": STATUS.FAILED}



async def run_analysis(data, session):

    session_id_value = data.get("session_id")

    print(f"STARTING ANALYSIS FOR SESSION ID: {session_id_value}")
    initialisation_result = await ensid_screening_status_initialisation(session_id_value, SessionFactory())
    all_ens_ids = await get_ens_ids_for_session_id("supplier_master_data",["ens_id", "session_id", "bvd_id", "name", "country", "national_id"], session_id_value, session)

    # / --- UPDATE SESSIONID STATUS TO STARTED
    session_status_cols = [{"screening_analysis_status": STATUS.IN_PROGRESS}]
    insert_status = await upsert_session_screening_status(session_status_cols, session_id_value, SessionFactory())
    # print(insert_status)  # TODO CHECK

    # 2. Orbis Data Fetch For All Concurrently (In Batches of 20 Concurrent Suppliers)
    orbis_batch_size = 1
    orbis_batches = batch_generator(all_ens_ids, orbis_batch_size)
    orbis_retrieval_status = []
    for orbis_batch in orbis_batches:
        # / --- UPDATE ENSID STATUS
        ens_ids_rows = [{**{"ens_id": entry["ens_id"]}, "orbis_retrieval_status": STATUS.IN_PROGRESS} for entry in orbis_batch]
        insert_status = await upsert_ensid_screening_status(ens_ids_rows, session_id_value, SessionFactory())
        # print(insert_status)

        # / --- RUN ORBIS RETRIEVAL FOR BATCH
        orbis_tasks = [run_orbis(entry["ens_id"], entry["session_id"], entry["bvd_id"], SessionFactory()) for entry in orbis_batch]
        orbis_batch_result = await asyncio.gather(*orbis_tasks)
        orbis_retrieval_status.extend(orbis_batch_result)


    screening_batch_size = 1
    screening_batches = batch_generator(all_ens_ids, screening_batch_size) # TODO Change input here based on success/fail
    screening_retrieval_status = []
    for screening_batch in screening_batches:
        # / --- UPDATE ENSID STATUS
        ens_ids_rows = [{**{"ens_id": entry["ens_id"]}, "screening_modules_status": STATUS.IN_PROGRESS} for entry in screening_batch]
        insert_status = await upsert_ensid_screening_status(ens_ids_rows, session_id_value, SessionFactory())
        # print(insert_status)

        # / --- RUN SCREENING TASKS FOR BATCH
        screening_tasks = [run_analysis_tasks(entry, SessionFactory()) for entry in screening_batch]
        screening_batch_result = await asyncio.gather(*screening_tasks)
        screening_retrieval_status.extend(screening_batch_result)


    # PERFORM ADDITIONAL VALIDATION / SUMMARIES HERE (?)
    log_json=await format_json_log(session_id_value, SessionFactory())
    log_json_file_name="error_logs.json"
    upload_to_azure_blob(log_json,log_json_file_name,session_id_value)

    session_status_cols = [{"overall_status": STATUS.COMPLETED}]
    insert_status = await upsert_session_screening_status(session_status_cols, session_id_value, SessionFactory())
    # print(insert_status)

    return []

