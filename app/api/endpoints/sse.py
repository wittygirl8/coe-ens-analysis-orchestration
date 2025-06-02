from urllib import request
from fastapi import APIRouter,  HTTPException, Query
import urllib
from app.schemas.requests import StreamingENSIdRequest, StreamingSessionIdRequest
from app.schemas.responses import ResponseMessage
from app.core.analysis.analysis import *
from app.core.utils.db_utils import *
from app.models import *
from app.core.config import get_settings
from app.schemas.logger import logger
import asyncpg
from fastapi.responses import StreamingResponse
from app.core.sse.streaming import *

router = APIRouter()
DATABASE_URL = (
    f"postgresql://{get_settings().database.username}:{get_settings().database.password.get_secret_value()}@"
    f"{get_settings().database.hostname}:{get_settings().database.port}/{get_settings().database.db}"
)

@router.get(
    "/ensid-status-bulk",
    response_model=ResponseMessage,
    description="Stream Status SSE for ENS_ID Changes - For List of ENS IDS"
)
async def get_ensid_status(
    session_id: str = Query(..., description="Session ID"),
    ens_id_list: str = Query(..., description="Double-encoded JSON-encoded list of ENS_IDs"),
    db_session: AsyncSession = Depends(deps.get_session), 
    current_user: User = Depends(deps.get_current_user)
):
    """
    GET Endpoint for Stream Status SSE
    """
    try:
        # Decode twice
        decoded_ids = urllib.parse.unquote(ens_id_list)
        decoded_ids = urllib.parse.unquote(decoded_ids)
        ens_id_list = json.loads(decoded_ids)  # Decode JSON string to list
        

        logger.debug("ens_id_list",ens_id_list, "\n session_id", session_id)

        conn = await asyncpg.connect(DATABASE_URL)  # single connection for duration of request ???
        logger.info("CONNECTION ESTABLISHED")

        # try:
        asyncio.create_task(listen_for_ensid_notifications(conn))

        return StreamingResponse(ensid_event_stream(session_id, ens_id_list, db_session), media_type="text/event-stream")
        # finally:
        #     await conn.close()
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")


@router.get(
    "/ensid-status",
    response_model=ResponseMessage,
    description="Stream Status SSE for ENS_ID Changes"
)
async def get_ensid_status(
        session_id: str = Query(..., description="Session ID"),
            ens_id: str = Query(..., description="ENS ID (single, string)"),
        db_session: AsyncSession = Depends(deps.get_session), 
        current_user: User = Depends(deps.get_current_user)
):
    """
    GET Endpoint for Stream Status SSE
    """
    try:
        # ENS ID, Session ID comes directly from Query Param

        logger.debug("ens_id_list", ens_id, "\n session_id", session_id)
        ens_id_list = [ens_id]

        conn = await asyncpg.connect(DATABASE_URL)  # single connection for duration of request ???
        logger.info("CONNECTION ESTABLISHED")

        # try:
        asyncio.create_task(listen_for_ensid_notifications(conn))

        return StreamingResponse(ensid_event_stream(session_id, ens_id_list, db_session),
                                 media_type="text/event-stream")
        # finally:
        #     await conn.close()
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")

@router.get(
    "/session-status", response_model=ResponseMessage, description="Stream Status SSE for Session ID Changes"
)
async def get_sessionid_status(
    session_id: str = Query(..., description="Session ID"),
    db_session: AsyncSession = Depends(deps.get_session), 
    current_user: User = Depends(deps.get_current_user)
):
    """

    :param request:
    :return:
    """
    try:
        logger.debug("session_id", session_id)
        conn = await asyncpg.connect(DATABASE_URL)
        logger.info("CONNECTION ESTABLISHED")

        # try:
        asyncio.create_task(listen_for_session_notifications(conn))

        return StreamingResponse(session_event_stream(session_id, db_session), media_type="text/event-stream")
        # finally:
        #     await conn.close()
    except Exception as e:
        # Handle errors gracefully
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")


@router.get(
    "/poll-session-status", response_model=ResponseMessage, description="Stream Status SSE for Session ID Changes"
)
async def get_sessionid_status_poll(
    session_id: str = Query(..., description="Session ID"),
    db_session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)
):
    """

    :param request:
    :return:
    """
    try:
        logger.debug("session_id", session_id)

        initial_state = await get_session_screening_status_static(session_id, db_session)

        logger.debug(initial_state)

        return {"status": "", "data": initial_state[0], "message": ""}

    except Exception as e:
        # Handle errors gracefully
        logger.error(f"get_sessionid_status_poll --> {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")

@router.get(
    "/poll-ensid-status", response_model=ResponseMessage, description="Stream Status SSE for Session ID Changes"
)
async def get_sessionid_status_poll(
        session_id: str = Query(..., description="Session ID"),
        ens_id: str = Query(..., description="ENS ID (single, string)"),
        db_session: AsyncSession = Depends(deps.get_session),
        current_user: User = Depends(deps.get_current_user)
):
    """

    :param request:
    :return:
    """
    try:
        logger.debug("session_id", session_id)
        ens_id_list = [ens_id]

        initial_state = await get_ensid_screening_status_static(ens_id_list, session_id, db_session)

        logger.debug(initial_state)

        return {"status": "", "data": initial_state[0], "message": ""}

    except Exception as e:
        # Handle errors gracefully
        logger.error(f"poll-ensid-status --> {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error running analysis: {str(e)}")