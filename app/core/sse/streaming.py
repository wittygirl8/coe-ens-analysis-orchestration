import asyncio
import json
from typing import List
from app.core.utils.db_utils import get_ensid_screening_status_static, get_session_screening_status_static

# ------------ ENSID STATUS
ensid_notification_queue = asyncio.Queue()
ensid_streaming_queue = asyncio.Queue()

async def listen_for_ensid_notifications(conn):
    await conn.add_listener('ens_id_status_channel', handle_ensid_notification)
    while True:
        payload = await ensid_notification_queue.get()
        await ensid_streaming_queue.put(payload)

async def handle_ensid_notification(connection, pid, channel, payload):
    data = json.loads(payload)
    await ensid_notification_queue.put(data)


async def ensid_event_stream(session_id: str, ens_id_list: List[str], db_session):
    initial_state = await get_ensid_screening_status_static(ens_id_list, session_id, db_session)
    for each_ensid in initial_state:
        yield f"data: {json.dumps(each_ensid, default=str)}\n\n"
    while True:
        data = await ensid_streaming_queue.get()
        if data['ens_id'].strip() in ens_id_list:
            resp = data
            yield f"data: {json.dumps(resp)}\n\n"


# ------------ SESSION STATUS
session_notification_queue = asyncio.Queue()
session_streaming_queue = asyncio.Queue()


async def listen_for_session_notifications(conn):
    await conn.add_listener('session_id_status_channel', handle_session_notification)
    while True:
        payload = await session_notification_queue.get()
        await session_streaming_queue.put(payload)


async def handle_session_notification(connection, pid, channel, payload):
    data = json.loads(payload)
    await session_notification_queue.put(data)


async def session_event_stream(session_id: str, db_session):
    initial_state = await get_session_screening_status_static(session_id, db_session)
    yield f"data: {json.dumps(initial_state[0], default=str)}\n\n"
    while True:
        data = await session_streaming_queue.get()
        if data['session_id'].strip() == session_id:
            resp = data
            yield f"data: {json.dumps(resp)}\n\n"