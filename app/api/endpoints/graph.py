from fastapi import APIRouter, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.analysis.graph_database_module.graph_utils import _q1_1, _q1_2
from app.schemas.responses import AnalysisResponse, AnalysisResult
from app.core.analysis.graph_database_module.graph_utils import *
from app.core.utils.db_utils import *
from app.models import *
from app.schemas.requests import GraphRequest
import traceback
from app.schemas.logger import logger
router = APIRouter()

@router.post(
    "/graph-root-node", 
    description="Generate a default graph with all data"
)
async def _q1(sess: AsyncSession = Depends(deps.get_session)):
    try:
        _resp = await _q1_1(sess)
        _res = AnalysisResult(
            module="Graph",
            status="Completed",
            result=_resp
        )
        return AnalysisResponse(results=_res)

    except Exception as e:
        _tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.info(_tb)
        raise HTTPException(status_code=500, detail=f"Error generating default graph: {_tb}")

@router.post(
    "/generate-graph", 
    description="Generate a default graph with all data"
)
async def _q2(req: GraphRequest, sess: AsyncSession = Depends(deps.get_session)):
    try:
        _resp = await _q1_2(sess, req.dict().get('client_id', ''), req.dict().get('session_id', ''))
        _res = AnalysisResult(
            module="Graph",
            status="Completed",
            result=_resp
        )
        return AnalysisResponse(results=_res)

    except Exception as e:
        _tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        logger.info(_tb)
        raise HTTPException(status_code=500, detail=f"Error generating default graph: {_tb}")
