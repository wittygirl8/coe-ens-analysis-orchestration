from fastapi import APIRouter, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.requests import AnalysisRequest, BulkAnalysisRequest, AnalysisRequestSingle
from app.schemas.responses import AnalysisResponse, BulkAnalysisResponse, AnalysisResult, TriggerTaskResponse
from app.core.analysis.analysis import *
from app.core.utils.db_utils import *
from app.models import *
import traceback
from app.schemas.logger import logger

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
        return {"success": True, "message": f"Analysis Pipeline Completed for {request.dict().get('session_id','')}", results: []}

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
            message=f"Screening Analysis Pipeline Triggered For {request.dict().get('session_id', '')}"
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
            message=f"Supplier Name Validation Pipeline Triggered For {request.dict().get('session_id')}"
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
        # Capture the detailed traceback
        tb_str = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        
        # Log or print the traceback for debugging
        logger.error(tb_str)  # You can replace this with logging (e.g., logger.error(tb_str))

        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(tb_str)}")
    
@router.post(
    "/graph-default-graph", 
    description="Generate a default graph with all data"
)
async def generate_default_graph(session: AsyncSession = Depends(deps.get_session)):
    try:
        
        response = await get_default_graph(session)
        analysis_result = AnalysisResult(
            module="Graph",
            status="Completed",
            result=response
        )
        return AnalysisResponse(results=analysis_result)

    except Exception as e:
        tb_str = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.info(tb_str)  
        raise HTTPException(status_code=500, detail=f"Error generating default graph: {str(tb_str)}")