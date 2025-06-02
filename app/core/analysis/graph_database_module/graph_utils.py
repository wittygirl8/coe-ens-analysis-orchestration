# app/core/phase1_analysis.py

from app.core.analysis.graph_database_module.configure import _q1, _q3
from app.core.analysis.session_initialisation.session import *

from app.core.analysis.graph_database_module.configure import *
import logging
from app.core.database_session import _ASYNC_ENGINE, SessionFactory

async def _q1_1(sess):
    _s = await _q1(sess)
    if _s["status"] == 'pass':
        return {"graph": STATUS.COMPLETED, "id": _s["client_id"]}
    elif _s["status"] == 'fail':
        return {"graph": STATUS.FAILED, "id": None}

async def _q1_2(sess, cid, sid):
    try:
        _s = await _q3(sess, cid, sid)

        if not isinstance(_s, dict):
            return {"graph": STATUS.FAILED, "neo4j_result": None}

        if _s.get("status") == "pass":
            return {
                "graph": STATUS.COMPLETED,
                "neo4j_result": _s.get("supplier_statuses", [])
            }

        elif _s.get("status") == "fail":
            return {
                "graph": STATUS.FAILED,
                "neo4j_result": _s.get("supplier_statuses", [])
            }

        return {
            "graph": STATUS.FAILED,
            "neo4j_result": _s
        }

    except HTTPException as e:
        raise e

    except Exception as e:
        return {
            "graph": STATUS.FAILED,
            "neo4j_result": {"error": str(e)}
        }
