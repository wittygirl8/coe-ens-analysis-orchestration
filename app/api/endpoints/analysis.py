from fastapi import APIRouter, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.requests import AnalysisRequest, BulkAnalysisRequest, AnalysisRequestSingle
from app.schemas.responses import AnalysisResponse, BulkAnalysisResponse, AnalysisResult, TriggerTaskResponse
from app.core.analysis.analysis import *
from app.core.utils.db_utils import *
from app.models import *

router = APIRouter()


@router.post(
    "/run-analysis", response_model=AnalysisResponse, description="Run Phase 1 Analysis (Synchronous)"
)
async def run_analysis_pipeline(request: AnalysisRequest, session: AsyncSession = Depends(deps.get_session), current_user: User = Depends(deps.get_current_user)):
    """
    API endpoint to run the analysis pipeline for a session id and wait for the process to finish before returning results.
    Not to be used in production.

    Args:
        request (AnalysisRequest): Input data contains session id to be run for

    Returns:
        AnalysisResponse: Await the results and return # TODO UPDATE
    """
    try:
        # Pass the validated request data to the analysis function
        results = await run_analysis(
            request.dict(),
            session
        )
        return {
            "success": True,
            "message": f"Analysis Pipeline Completed for {request.dict().get('session_id', '')}",
            "results": []
        }
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error submitting analysis: {str(e)}")

@router.post(
    "/trigger-analysis", response_model=TriggerTaskResponse, description="Run Phase 1 Analysis"
)
async def trigger_analysis_pipeline(request: AnalysisRequest, background_tasks:BackgroundTasks, session: AsyncSession = Depends(deps.get_session), current_user: User = Depends(deps.get_current_user)):
    """
    API endpoint to submit trigger for screening analysis

    Args:
        request (AnalysisRequest): Input data for the analysis.

    Returns:
        AnalysisResponse: status of whether request was submitted successfully
    """
    try:
        # Pass the validated request data to the analysis function
        background_tasks.add_task(run_analysis, request.dict(), session)
        trigger_response = TriggerTaskResponse(
            status=True,
            message=f"Screening Analysis Pipeline Triggered For {request.dict().get("session_id", "")}"
        )
        return trigger_response

    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error submitting analysis: {str(e)}")


@router.post(
    "/orbis-api", response_model=AnalysisResponse, description="Run Orbis API Analysis"
)
async def run_orbisapi(request: AnalysisRequest, session: AsyncSession = Depends(deps.get_session), current_user: User = Depends(deps.get_current_user)):
    """
    API endpoint to execute Orbis API analysis.

    Args:
        request (AnalysisRequest): Input data for the analysis.

    Returns:
        AnalysisResponse: Results of the analysis.
    """
    try:
        # Pass the validated request data to the analysis function
        results = await run_orbis(
            request.dict(),
            session
        )  # Convert the request object to a dictionary
        return {
            "success": True,
            "message": "Orbis-API completed successfully",
            "results": results,
        }
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")


@router.post(
    "/batch-analysis",
    response_model=BulkAnalysisResponse,
    description="Run Batch Phase 1 Analysis",
)
async def batch_analysis(request: BulkAnalysisRequest, session: AsyncSession = Depends(deps.get_session), current_user: User = Depends(deps.get_current_user)):
    """
    API endpoint to execute Phase 1 analysis in batch.

    Args:
        request (BulkAnalysisRequest): Input data for the batch analysis.

    Returns:
        BulkAnalysisResponse: Results of the batch analysis.
    """
    try:
        # Pass the validated request data to the analysis function
        results = await run_analysis(
            request.dict(),
            session
        )  # Convert the request object to a dictionary
        return {
            "success": True,
            "message": "Analysis completed successfully",
            "results": results,
        }
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")


@router.post(
    "/batch-orbis-api",
    response_model=BulkAnalysisResponse,
    description="Run Batch Orbis API Analysis",
)
async def batch_orbis_api(request: BulkAnalysisRequest, session: AsyncSession = Depends(deps.get_session), current_user: User = Depends(deps.get_current_user)):
    """
    API endpoint to execute Orbis API analysis in batch.

    Args:
        request (BulkAnalysisRequest): Input data for the batch Orbis API analysis.

    Returns:
        BulkAnalysisResponse: Results of the batch Orbis API analysis.
    """
    try:
        # Pass the validated request data to the analysis function
        results = await run_orbis(
            request.dict(),
            session
        )  # Convert the request object to a dictionary
        return {
            "success": True,
            "message": "Orbis-API completed successfully",
            "results": results,
        }
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")


@router.post(
    "/run-supplier-validation", response_model=AnalysisResponse, description="Run Supplier Name Validation (Synchronous)"
)
async def supplier_validation(request: AnalysisRequest, session: AsyncSession = Depends(deps.get_session), current_user: User = Depends(deps.get_current_user)):
    """
    API endpoint to execute supplier name validation.
    Not to be used in deployment
    Args:
        request (AnalysisRequest): Input data for the analysis.

    Returns:
        AnalysisResponse: Results of the analysis.
    """
    try:
        # Pass the validated request data to the function which runs the analysis
        response = await run_supplier_name_validation(
            request.dict(),
            session
        ) 
        analysis_result = AnalysisResult(
            module="Supplier Name Validation",
            status="success",
            result=response
        )
        return AnalysisResponse(results=analysis_result)
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")


@router.post(
    "/trigger-supplier-validation", response_model=TriggerTaskResponse, description="Trigger Supplier Name Validation as Background Task"
)
async def trigger_supplier_validation(request: AnalysisRequest, background_tasks: BackgroundTasks, session: AsyncSession = Depends(deps.get_session), current_user: User = Depends(deps.get_current_user)):
    """
    API endpoint to trigger supplier name validation as a background task

    Args:
        request (AnalysisRequest): session id

    Returns:
        TriggerTaskResponse: Results of the analysis.
    """
    try:
        background_tasks.add_task(run_supplier_name_validation, request.dict(), session)
        trigger_response = TriggerTaskResponse(
            status=True,
            message=f"Supplier Name Validation Pipeline Triggered For {request.dict().get("session_id")}"
        )
        return trigger_response
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")


# TODO: Will mostly remove this, but for now, using it for testing 
@router.post(
    "/generate-supplier-reports", response_model=AnalysisResponse, description="Generate all reports for session id"
)
async def generate_report(request: AnalysisRequest, session: AsyncSession = Depends(deps.get_session), current_user: User = Depends(deps.get_current_user)):
    """
    API endpoint to Report Generation for all suppliers in session_id - standalone
    Not to be used in deployment

    Args:
        request (AnalysisRequest): Input data of session_id

    Returns:
        AnalysisResponse: Results of the report generation activity
    """
    try:
        # Pass the validated request data to the function which runs the analysis
        response = await run_report_generation_standalone(
            request.dict(),
            session
        )
        analysis_result = AnalysisResult(
            module="Report Generation",
            status="success",
            result=response
        )
        return AnalysisResponse(results=analysis_result)
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")


# This is not for the main workflow, just for testing 
@router.post(
    "/generate-single-supplier-report", response_model=AnalysisResponse, description="Generate a report for a supplier"
)
async def generate_report(request: AnalysisRequestSingle, session: AsyncSession = Depends(deps.get_session)):
    """
    API endpoint to Report Generation for all suppliers in session_id - standalone
    Not to be used in deployment

    Args:
        request (AnalysisRequest): Input data of session_id

    Returns:
        AnalysisResponse: Results of the report generation activity
    """
    try:
        # Pass the validated request data to the function which runs the analysis
        response = await run_report_generation_single(
            request.dict(),
            session
        )
        analysis_result = AnalysisResult(
            module="Report Generation - Single",
            status="success",
            result=response
        )
        return AnalysisResponse(results=analysis_result)
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")

# @router.get("/dummy-table")
# async def get_dummy_table_data(session: AsyncSession = Depends(deps.get_session)):
#     try:
#         # table_name= 'kpi_master_data'
#         # required_columns= ['id','ens_id']
#         # ens_id= 'ENS12345'
#         # # Call the function to get data dynamically from the table
#         # res = await get_dynamic_ens_data(table_name, required_columns, ens_id, session)

#         # kpi_data = {
#         #     "kpi_area": "+44",
#         #     "kpi_code": "ESG1CGSDFSXC"
#         # }

#         # ens_id = "ENS12345"
#         # table_name = "kpi_master_data"

#         # # Call the function
#         # res = await update_dynamic_ens_data(table_name, kpi_data, ens_id, session)

#         # kpi_data = [
#         #     {"kpi_area": "ESG", "kpi_code": "ESG1C", "kpi_flag": False, "kpi_value": '10.5', "kpi_details": "Initial details"},
#         #     {"kpi_area": "Sustainability", "kpi_code": "SUST1B", "kpi_flag": True, "kpi_value": '20.8', "kpi_details": "Secondary details"}
#         # ]
#         # ens_id = "ENS12345"
#         # session_id = "SESSION67890"
#         # table_name = "kpi_master_data"

#         # # Call the function
#         # res = await insert_dynamic_ens_data(table_name, kpi_data, ens_id, session_id, session)

#         # kpi_data = [{
#         #     "kpi_area": "+41",
#         #     "kpi_value": "56765",
#         #     "kpi_code": "ESG1CGSDFSXC"
#         # },
#         # {
#         #     "kpi_area": "+42",
#         #     "kpi_value": "_",
#         #     "kpi_code": "ESG1CGbghceSDFSXC"
#         # }]

#         # ens_id = "ENS12345"
#         # table_name = "cyes"
#         # session_id = "7yer837r3876548974978s"

#         # # Call the function
#         # res = await upsert_kpi(table_name, kpi_data, ens_id, session_id,session)

#         # kpi_data = [{
#         #     "overall_status": STATUS.COMPLETED,
#         #     "orbis_retrieval_status": STATUS.IN_PROGRESS,
#         #     "ens_id": "6768"
#         # },
#         # {
#         #     "overall_status": STATUS.COMPLETED,
#         #     "orbis_retrieval_status": STATUS.COMPLETED,
#         #     "ens_id": "jghgj"
#         # },
#         # {
#         #     "overall_status": STATUS.COMPLETED,
#         #     "orbis_retrieval_status": STATUS.IN_PROGRESS,
#         #     "ens_id": "8687"
#         # }]

#         # table_name = "ensid_screening_status"
#         # session_id = "7yer837r3876548974978s"

#         # # Call the function
#         # res = await upsert_ensid_screening_status( kpi_data, session_id,session)

#         kpi_data = [{
#             "overall_status": STATUS.COMPLETED,
#             "list_upload_status": STATUS.IN_PROGRESS
#         }]

#         table_name = "ensid_screening_status"
#         session_id = "7yer837r3876548974978s"

#         # Call the function
#         res = await upsert_session_screening_status( kpi_data, session_id,session)

#         print("res___", res)
#         return res

#     except ValueError as ve:
#         # Handle the case where the table does not exist or other value errors
#         print(f"Error: {ve}")
#         return {"error": str(ve), "data": []}

#     except SQLAlchemyError as sa_err:
#         # Handle SQLAlchemy-specific errors
#         print(f"Database error: {sa_err}")
#         return {"error": "Database error", "data": []}

#     except Exception as e:
#         # Catch any other exceptions
#         print(f"An unexpected error occurred: {e}")
#         return {"error": "An unexpected error occurred", "data": []}