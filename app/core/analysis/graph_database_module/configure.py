from datetime import datetime
import re
import textwrap
from fastapi import HTTPException
from app.core.analysis.report_generation_submodules.utilities import *
from app.core.security.jwt import create_jwt_token
from app.core.config import get_settings
from app.core.utils.db_utils import *
import os
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase
import uuid
import asyncio
from app.schemas.logger import logger
load_dotenv()
URI = os.environ.get("GRAPHDB__URI")
USER = os.environ.get("GRAPHDB__USER")
PASSWORD = os.environ.get("GRAPHDB__PASSWORD")


async def default_graph(session):
    supplier_name, country, nat_id, contact_id = "Not Found", "Not Found", "Not Found", "Not Found"

    data = await get_dynamic_ens_data("ovar", required_columns=["all"], ens_id=None, session_id="913f5280-1d8c-4c94-bfe6-5b0f3d9789ac", session=session)

    filtered_suppliers = [row for row in data if row.get("kpi_area") == "overall_rating" and row.get("kpi_rating") in ["High", "Medium"]]
    if not filtered_suppliers:
        return {"status": "fail", "message": "No suppliers with high/medium/low rating found."}

    cypher_query = 'CREATE (a:Company {name: "Aramco", id: "0000A01"})'
    relationships = []
    i = 1
    for supplier in filtered_suppliers:
        try:
            misc = await get_dynamic_ens_data("supplier_master_data", required_columns=["all"], ens_id=supplier["ens_id"], session_id=None, session=session)
            if misc:
                supplier_name = misc[0]["name"]
                country = misc[0]["country"]
                nat_id = misc[0]["national_id"]

        except Exception as e:            
            logger.error(f"Supplier name not matched from supplier_master_data table. ENS ID = {supplier['ens_id']}, error {e}")

        try:
            grid = await get_dynamic_ens_data("grid_management", required_columns=["all"], ens_id=supplier["ens_id"], session_id=None, session=session)
            if grid:
                contact_id = grid[0]["contact_id"]
        except Exception as e:
            contact_id = "Not Found"
            logger.error(f"Supplier name not matched from supplier_master_data table. ENS ID = {supplier['ens_id']}, error {e}")

        node_var = f"direct_supplier_{i}"  
        cypher_query += f', ({node_var}:`Supplier` {{id: "{supplier["id"]}", type: "Supplier", name: "{supplier_name}", country:"{country}", national_id:"{nat_id}", contact_id:"{contact_id}", overall_rating: "{supplier["kpi_rating"]}"}})'
        relationships.append(f'({node_var})-[:SUPPLIER_OF]->(a)')
        i += 1

    cypher_query += ', ' + ', '.join(relationships) + ';'
    
    with open("app/core/analysis/graph_database_module/direct_suppliers.txt", "w", encoding="utf-8") as file:
        formatted_query = cypher_query.replace(",", ",\n    ")  
        formatted_query = formatted_query.replace("CREATE", "\nCREATE") 
        file.write(formatted_query)


    return await _q0(cypher_query)

async def _q0(cq: str) -> dict:
    try:
        async with AsyncGraphDatabase.driver(URI, auth=(USER, PASSWORD)) as d:
            async with d.session() as s:
                r = await s.run(cq)
                try:
                    d_ = await r.data()
                    return {"status": "pass", "message": "Query executed successfully.", "records": d_}
                except Exception:
                    return {"status": "pass", "message": "Query executed successfully. No return values."}
    except Exception as e:
        return {"status": "fail", "message": f"Error executing query: {str(e)}"}

async def _q1(s):
    cid = str(uuid.uuid4())
    cq = f'CREATE (a:Company {{name: "Aramco", id: "{cid}"}})'
    return {"status": "pass", "message": "Aramco node created successfully.", "client_id": cid, "neo4j_result": r}

async def _q2(m1, cm, om, sp, nv, cid):
    n, c, ni, bi = "Not Found", "Not Found", "Not Found", "Not Found"
    if m1:
        n = m1[0].get("name", n)
        c = m1[0].get("country", c)
        ni = m1[0].get("national_id", ni)
        bi = m1[0].get("bvd_id", bi)
    l, nid = "Not Found", "Not Found"
    if cm:
        l = cm[0].get("location", l)
        nid = cm[0].get("national_identifier", nid)

    s1 = gpr = bcr = fr = oamr = air = orr = "Not Found"

    if om:
        for rw in om:
            kc = rw.get("kpi_code", "").lower()
            ka = rw.get("kpi_area", "").lower()
            kr = rw.get("kpi_rating", "")
            if kc == "other_adverse_media":
                oamr = kr
            elif kc == "sanctions":
                s1 = kr
            elif kc == "government_political":
                gpr = kr
            elif kc == "bribery_corruption_overall":
                bcr = kr
            elif kc == "financials":
                fr = kr
            elif kc == "additional indicator":
                air = kr
            elif ka == "overall_rating":
                orr = kr

    sid = sp["ens_id"]
    q = textwrap.dedent(f'''
    MERGE ({nv}:Supplier {{ bvd_id: "{bi}" }})
    ON MATCH SET
        {nv}.id = "{sid}",
        {nv}.update_time = "{datetime.utcnow().isoformat()}",
        {nv}.type = COALESCE({nv}.type, "Organization"),
        {nv}.name = COALESCE({nv}.name, "{n}"),
        {nv}.location = COALESCE({nv}.location, "{l}"),
        {nv}.country = COALESCE({nv}.country, "{c}"),
        {nv}.national_id = COALESCE({nv}.national_id, "{ni}"),
        {nv}.national_identifier = COALESCE({nv}.national_identifier, "{nid}"),
        {nv}.sanctions_rating = COALESCE({nv}.sanctions_rating, "{s1}"),
        {nv}.government_political_rating = COALESCE({nv}.government_political_rating, "{gpr}"),
        {nv}.bribery_corruption_overall_rating = COALESCE({nv}.bribery_corruption_overall_rating, "{bcr}"),
        {nv}.financials_rating = COALESCE({nv}.financials_rating, "{fr}"),
        {nv}.other_adverse_media_rating = COALESCE({nv}.other_adverse_media_rating, "{oamr}"),
        {nv}.additional_indicator_rating = COALESCE({nv}.additional_indicator_rating, "{air}"),
        {nv}.overall_rating = COALESCE({nv}.overall_rating, "{orr}"),
        {nv}.beneficial_owners_full_node = COALESCE({nv}.beneficial_owners_full_node, ""),
        {nv}.other_ultimate_beneficiary_full_node = COALESCE({nv}.other_ultimate_beneficiary_full_node, ""),
        {nv}.shareholders_full_node = COALESCE({nv}.shareholders_full_node, ""),
        {nv}.global_ultimate_owner_full_node = COALESCE({nv}.global_ultimate_owner_full_node, ""),
        {nv}.ultimately_owned_subsidiaries_full_node = COALESCE({nv}.ultimately_owned_subsidiaries_full_node, "")
    ON CREATE SET
        {nv}.id = "{sid}",
        {nv}.update_time = "{datetime.utcnow().isoformat()}",
        {nv}.type = "Organization",
        {nv}.name = "{n}",
        {nv}.location = "{l}",
        {nv}.country = "{c}",
        {nv}.national_id = "{ni}",
        {nv}.national_identifier = "{nid}",
        {nv}.sanctions_rating = "{s1}",
        {nv}.government_political_rating = "{gpr}",
        {nv}.bribery_corruption_overall_rating = "{bcr}",
        {nv}.financials_rating = "{fr}",
        {nv}.other_adverse_media_rating = "{oamr}",
        {nv}.additional_indicator_rating = "{air}",
        {nv}.overall_rating = "{orr}",
        {nv}.beneficial_owners_full_node = "",
        {nv}.other_ultimate_beneficiary_full_node = "",
        {nv}.shareholders_full_node = "",
        {nv}.global_ultimate_owner_full_node = "",
        {nv}.ultimately_owned_subsidiaries_full_node = ""
    MERGE ({nv})-[:SUPPLIER_OF]->(a)
    ''')
    q = f'MATCH (a:Company {{id: "{cid}"}})\n' + q
    try:
        r = await _q0(q)
    except Exception as e:
        raise RuntimeError(f"Neo4j query failed for supplier {sp['ens_id']}: {e}")

    return r, sid

async def _q9(ext_misc, nv, cid, sid, sess):
    try:
        _misc = ext_misc[0].get("beneficial_owners", []) if ext_misc and len(ext_misc) else []
        _flag = False
        _cq = ''
        _nvn = ''
        if (_misc == None) or not len(_misc):
            logger.info("No beneficial_owner found.")
            return ""

        async def _bq(sub, idx, sess, pre=""):
            try:
                _nt = "Organization" if sub.get("bvd_id") else "Individual"
                _nl = "Supplier" if _nt == "Organization" else "Individual"
                _nid = str(uuid.uuid4())
                if _nt == "Organization":
                    try:
                        _d = await get_main_supplier_bvdid_data(
                            required_columns=["ens_id", "session_id", "bvd_id"],
                            bvd_id=sub["bvd_id"],
                            session=sess
                        )
                        _eid = _d[0].get("ens_id") if _d else None
                        _nid = _eid or sub["bvd_id"]
                    except Exception as e:
                        logger.warning(f"[Warning] Error fetching BVD data: {e}")
                        _nid = sub["bvd_id"]
                else:
                    _nid = sub.get("contact_id")

                _vn = f"{nv}_{pre}beneficial_owner_{'supplier' if _nt == 'Organization' else 'individual'}_{idx}"

                _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                _mi = sub.get("media_indicator", "").lower() != "no"
                _pi = sub.get("pep_indicator", "").lower() != "no"
                _ri = _si or _mi or _pi
                _pc = sub.get('possible percentage change', '0')

                if str(_nid) == 'None':
                    #logger.warning(f"here ---> _q9")
                    _nid = str(uuid.uuid4())

                return textwrap.dedent(f'''
                    MERGE ({_vn}:{_nl} {{
                        id: "{_nid}"
                    }})
                    SET 
                        {_vn}.type = "{_nt}",
                        {_vn}.update_time = "{datetime.utcnow().isoformat()}",
                        {_vn}.name = "{sub.get('name')}",
                        {_vn}.pep_indicator = "{str(_pi).lower()}",
                        {_vn}.media_indicator = "{str(_mi).lower()}",
                        {_vn}.sanctions_indicator = "{str(_si).lower()}",
                        {_vn}.risk_indicator = "{str(_ri).lower()}",
                        {_vn}.possible_percentage_change = "{str(_pc).lower()}"
                    MERGE ({_vn})-[:BENEFICIAL_OWNER_OF]->({nv})
                ''')
            except Exception as e:
                logger.error(f"[Error] Failed to build Cypher: {e}")
                return ""

        if _misc and len(_misc) < 50:
            _flag = True
            for idx, sub in enumerate(_misc, start=1):
                _cq += await _bq(sub, idx, sess=sess)
        else:
            _hr = []
            for sub in _misc:
                try:
                    _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                    _mi = sub.get("media_indicator", "").lower() != "no"
                    _pi = sub.get("pep_indicator", "").lower() != "no"
                    _ri = _si or _mi or _pi
                    _pc = sub.get('possible percentage change', '0')

                    if _ri:
                        _of = 0.0
                        _hr.append((_of, sub))
                except Exception as e:
                    logger.error(f"[Warning] Skipped beneficial_owner due to error: {e}")
                    continue

            _top = sorted(_hr, key=lambda x: x[0], reverse=True)[:20]
            for idx, (_, sub) in enumerate(_top, start=1):
                _cq += await _bq(sub, idx, sess=sess, pre="top")

        _cq = f'''
            MATCH (a:Company {{id: "{cid}"}})
            MATCH (s:Supplier {{id: "{sid}"}})-[:SUPPLIER_OF]->(a)
            WITH s as {nv}
            ''' + _cq + f'''SET {nv}.beneficial_owners_full_node = "{str(_flag).lower()}"'''
        try:
            _res = await _q0(_cq)
        except Exception as e:
            raise RuntimeError(f"Neo4j query failed for supplier {sid}: {e}")

        return _cq
    except Exception as e:
        logger.error(f"[Error] _q9 failed: {e}")
        raise RuntimeError(f"_q9 failed: {e}")

async def _q10(ext_misc, nv, cid, sid, sess):
    try:
        _misc = ext_misc[0].get("shareholders", []) if ext_misc and len(ext_misc) else []
        _flag = False
        _cq = ''
        _nvn = ''
        if (_misc == None) or not len(_misc):
            return ''

        async def _bq(sub, idx, sess, pre=""):
            try:
                _nt = "Organization" if sub.get("bvd_id") else "Individual"
                _nl = "Supplier" if _nt == "Organization" else "Individual"
                _nid = str(uuid.uuid4())
                if _nt == "Organization":
                    try:
                        _d = await get_main_supplier_bvdid_data(
                            required_columns=["ens_id", "session_id", "bvd_id"],
                            bvd_id=sub["bvd_id"],
                            session=sess
                        )
                        _eid = _d[0].get("ens_id") if _d else None
                        _nid = _eid or sub["bvd_id"]
                    except Exception as e:
                        logger.warning(f"[Warning] Error fetching BVD data: {e}")
                        _nid = sub["bvd_id"]
                else:
                    _nid = sub.get("contact_id")

                _vn = f"{nv}_{pre}shareholders_{'supplier' if _nt == 'Organization' else 'individual'}_{idx}"

                _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                _mi = sub.get("media_indicator", "").lower() != "no"
                _pi = sub.get("pep_indicator", "").lower() != "no"
                _ri = _si or _mi or _pi
                _to = sub.get('total_ownership', '0')
                _do = sub.get('direct_ownership', '0')

                if str(_nid) == 'None':
                    #logger.info(f"here ---> _q10")
                    _nid = str(uuid.uuid4())

                return textwrap.dedent(f'''
                    MERGE ({_vn}:{_nl} {{
                        id: "{_nid}"
                    }})
                    SET 
                        {_vn}.type = "{_nt}",
                        {_vn}.update_time = "{datetime.utcnow().isoformat()}",
                        {_vn}.name = "{sub.get('name')}",
                        {_vn}.pep_indicator = "{str(_pi).lower()}",
                        {_vn}.media_indicator = "{str(_mi).lower()}",
                        {_vn}.sanctions_indicator = "{str(_si).lower()}",
                        {_vn}.risk_indicator = "{str(_ri).lower()}",
                        {_vn}.total_ownership = "{str(_to).lower()}",
                        {_vn}.direct_ownership = "{str(_do).lower()}"
                    MERGE ({_vn})-[:SHAREHOLDER_OF]->({nv})
                ''')
            except Exception as e:
                logger.error(f"[Error] Failed to build Cypher: {e}")
                return ""

        if _misc and len(_misc) < 50:
            _flag = True
            for idx, sub in enumerate(_misc, start=1):
                _cq += await _bq(sub, idx, sess=sess)
        else:
            _hr = []
            for sub in _misc:
                try:
                    _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                    _mi = sub.get("media_indicator", "").lower() != "no"
                    _pi = sub.get("pep_indicator", "").lower() != "no"
                    _ri = _si or _mi or _pi
                    _do = sub.get('direct_ownership', '0')

                    if _ri:
                        try:
                            _of = float(_do)
                        except:
                            _of = 0.0
                        _hr.append((_of, sub))
                except Exception as e:
                    logger.warning(f"[Warning] Skipped shareholder due to error: {e}")
                    continue

            _top = sorted(_hr, key=lambda x: x[0], reverse=True)[:20]
            for idx, (_, sub) in enumerate(_top, start=1):
                _cq += await _bq(sub, idx, sess=sess, pre="top")

        _cq = f'''
            MATCH (a:Company {{id: "{cid}"}})
            MATCH (s:Supplier {{id: "{sid}"}})-[:SUPPLIER_OF]->(a)
            WITH s as {nv}
            ''' + _cq + f'''SET {nv}.shareholders_full_node = "{str(_flag).lower()}"'''
        try:
            _res = await _q0(_cq)
        except Exception as e:
            raise RuntimeError(f"Neo4j query failed for supplier {sid}: {e}")

        return _cq
    except Exception as e:
        logger.error(f"[Error] _q10 failed: {e}")
        raise RuntimeError(f"_q10 failed: {e}")

async def _q11(ext_misc, nv, cid, sid, sess):
    try:
        _misc = ext_misc[0].get("global_ultimate_owner", []) if ext_misc and len(ext_misc) else []
        _flag = False
        _cq = ''
        _nvn = ''
        if (_misc == None) or not len(_misc):
            return _cq

        async def _bq(sub, idx, sess, pre=""):
            try:
                _nt = "Organization" if sub.get("bvd_id") else "Individual"
                _nl = "Supplier" if _nt == "Organization" else "Individual"
                _nid = str(uuid.uuid4())
                if _nt == "Organization":
                    try:
                        _d = await get_main_supplier_bvdid_data(
                            required_columns=["ens_id", "session_id", "bvd_id"],
                            bvd_id=sub["bvd_id"],
                            session=sess
                        )
                        _eid = _d[0].get("ens_id") if _d else None
                        _nid = _eid or sub["bvd_id"]
                    except Exception as e:
                        logger.warning(f"[Warning] Error fetching BVD data: {e}")
                        _nid = sub["bvd_id"]
                else:
                    _nid = sub.get("contact_id")

                _vn = f"{nv}_{pre}global_ultimate_owner_{'supplier' if _nt == 'Organization' else 'individual'}_{idx}"

                _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                _mi = sub.get("media_indicator", "").lower() != "no"
                _pi = sub.get("pep_indicator", "").lower() != "no"
                _ri = _si or _mi or _pi
                _to = sub.get('total_ownership', '0')
                _do = sub.get('direct_ownership', '0')

                if str(_nid) == 'None':
                    #logger.info(f"here ---> _q11")
                    _nid = str(uuid.uuid4())

                return textwrap.dedent(f'''
                    MERGE ({_vn}:{_nl} {{
                        id: "{_nid}"
                    }})
                    SET 
                        {_vn}.type = "{_nt}",
                        {_vn}.update_time = "{datetime.utcnow().isoformat()}",
                        {_vn}.name = "{sub.get('name')}",
                        {_vn}.pep_indicator = "{str(_pi).lower()}",
                        {_vn}.media_indicator = "{str(_mi).lower()}",
                        {_vn}.sanctions_indicator = "{str(_si).lower()}",
                        {_vn}.risk_indicator = "{str(_ri).lower()}",
                        {_vn}.total_ownership = "{str(_to).lower()}",
                        {_vn}.direct_ownership = "{str(_do).lower()}"
                    MERGE ({_vn})-[:GLOBAL_ULTIMATE_OWNER_OF]->({nv})
                ''')
            except Exception as e:
                logger.error(f"[Error] Failed to build Cypher: {e}")
                return ""

        if _misc and len(_misc) < 50:
            _flag = True
            for idx, sub in enumerate(_misc, start=1):
                _cq += await _bq(sub, idx, sess=sess)
        else:
            _hr = []
            for sub in _misc:
                try:
                    _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                    _mi = sub.get("media_indicator", "").lower() != "no"
                    _pi = sub.get("pep_indicator", "").lower() != "no"
                    _ri = _si or _mi or _pi
                    _do = sub.get('direct_ownership', '0')

                    if _ri:
                        try:
                            _of = float(_do)
                        except:
                            _of = 0.0
                        _hr.append((_of, sub))
                except Exception as e:
                    logger.warning(f"[Warning] Skipped ultimate owner due to error: {e}")
                    continue

            _top = sorted(_hr, key=lambda x: x[0], reverse=True)[:20]
            for idx, (_, sub) in enumerate(_top, start=1):
                _cq += await _bq(sub, idx, sess=sess, pre="top")

        _cq = f'''
            MATCH (a:Company {{id: "{cid}"}})
            MATCH (s:Supplier {{id: "{sid}"}})-[:SUPPLIER_OF]->(a)
            WITH s as {nv}
            ''' + _cq + f'''SET {nv}.global_ultimate_owner_full_node = "{str(_flag).lower()}"'''
        try:
            _res = await _q0(_cq)
        except Exception as e:
            raise RuntimeError(f"Neo4j query failed for supplier {sid}: {e}")

        return _res
    except Exception as e:
        logger.error(f"[Error] _q11 failed: {e}")
        raise RuntimeError(f"_q11 failed: {e}")

async def _q7(ext_misc, nv, cid, sid, sess):
    try:
        _flag = False
        _cq = ''
        _nvn = ''
        _misc = ext_misc[0].get("ultimately_owned_subsidiaries", []) if ext_misc and len(ext_misc) else []
        if (_misc == None) or not len(_misc):
            return _cq

        async def _bq(sub, idx, sess, pre=""):
            try:
                _nt = "Organization" if sub.get("bvd_id") else "Individual"
                _nl = "Supplier" if _nt == "Organization" else "Individual"
                _nid = str(uuid.uuid4())
                if _nt == "Organization":
                    try:
                        _d = await get_main_supplier_bvdid_data(
                            required_columns=["ens_id", "session_id", "bvd_id"],
                            bvd_id=sub["bvd_id"],
                            session=sess
                        )
                        _eid = _d[0].get("ens_id") if _d else None
                        _nid = _eid or sub["bvd_id"]
                    except Exception as e:
                        logger.warning(f"[Warning] Error fetching BVD data: {e}")
                        _nid = sub["bvd_id"]
                else:
                    _nid = sub.get("contact_id")

                _vn = f"{nv}_{pre}ultimately_owned_SUBSIDIARIES_{'supplier' if _nt == 'Organization' else 'individual'}_{idx}"

                _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                _mi = sub.get("media_indicator", "").lower() != "no"
                _pi = sub.get("pep_indicator", "").lower() != "no"
                _ri = _si or _mi or _pi
                _to = sub.get('total_ownership', '0')
                _do = sub.get('direct_ownership', '0')

                if str(_nid) == 'None':
                    #logger.info(f"here ---> _q7")
                    _nid = str(uuid.uuid4())

                return textwrap.dedent(f'''
                    MERGE ({_vn}:{_nl} {{
                        id: "{_nid}"
                    }})
                    SET 
                        {_vn}.type = "{_nt}",
                        {_vn}.update_time = "{datetime.utcnow().isoformat()}",
                        {_vn}.name = "{sub.get('name')}",
                        {_vn}.pep_indicator = "{str(_pi).lower()}",
                        {_vn}.media_indicator = "{str(_mi).lower()}",
                        {_vn}.sanctions_indicator = "{str(_si).lower()}",
                        {_vn}.risk_indicator = "{str(_ri).lower()}",
                        {_vn}.total_ownership = "{str(_to).lower()}",
                        {_vn}.direct_ownership = "{str(_do).lower()}"
                    MERGE ({_vn})-[:ULTIMATELY_OWNED_SUBSIDIARY_OF]->({nv})
                ''')
            except Exception as e:
                logger.error(f"[Error] Failed to build Cypher: {e}")
                return ""

        if _misc and len(_misc) < 50:
            _flag = True
            for idx, sub in enumerate(_misc, start=1):
                _cq += await _bq(sub, idx, sess=sess)
        else:
            _hr = []
            for sub in _misc:
                try:
                    _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                    _mi = sub.get("media_indicator", "").lower() != "no"
                    _pi = sub.get("pep_indicator", "").lower() != "no"
                    _ri = _si or _mi or _pi
                    _do = sub.get('direct_ownership', '0')

                    if _ri:
                        try:
                            _of = float(_do)
                        except:
                            _of = 0.0
                        _hr.append((_of, sub))
                except Exception as e:
                    logger.warning(f"[Warning] Skipped subsidiary due to error: {e}")
                    continue

            _top = sorted(_hr, key=lambda x: x[0], reverse=True)[:20]
            for idx, (_, sub) in enumerate(_top, start=1):
                _cq += await _bq(sub, idx, sess=sess, pre="top")

        _cq = f'''
            MATCH (a:Company {{id: "{cid}"}})
            MATCH (s:Supplier {{id: "{sid}"}})-[:SUPPLIER_OF]->(a)
            WITH s as {nv}
            ''' + _cq + f'''SET {nv}.ultimately_owned_subsidiaries_full_node = "{str(_flag).lower()}"'''
        try:
            _res = await _q0(_cq)
        except Exception as e:
            raise RuntimeError(f"Neo4j query failed for supplier {sid}: {e}")

        return _res
    except Exception as e:
        logger.error(f"[Error] _q7 failed: {e}")
        raise RuntimeError(f"_q7 failed: {e}")

async def _q8(ext_misc, nv, cid, sid, sess):
    try:
        _flag = False
        _cq = ''
        _nvn = ''
        _misc = ext_misc[0].get("other_ultimate_beneficiary", []) if ext_misc and len(ext_misc) else []
        if (_misc == None) or not len(_misc):
            return _cq

        async def _bq(sub, idx, sess, pre=""):
            try:
                _nt = "Organization" if sub.get("bvd_id") else "Individual"
                if _nt != "Organization":
                    logger.info("Empty BVD ID")
                    return _cq
                _nl = "Supplier" if _nt == "Organization" else "Individual"
                _nid = str(uuid.uuid4())
                if _nt == "Organization":
                    try:
                        _d = await get_main_supplier_bvdid_data(
                            required_columns=["ens_id", "session_id", "bvd_id"],
                            bvd_id=sub["bvd_id"],
                            session=sess
                        )
                        _eid = _d[0].get("ens_id") if _d else None
                        _nid = _eid or sub["bvd_id"]
                    except Exception as e:
                        logger.warning(f"[Warning] Error fetching BVD data: {e}")
                        _nid = sub["bvd_id"]

                _vn = f"{nv}_{pre}ultimately_owned_SUBSIDIARIES_{'supplier' if _nt == 'Organization' else 'individual'}_{idx}"

                _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                _mi = sub.get("media_indicator", "").lower() != "no"
                _pi = sub.get("pep_indicator", "").lower() != "no"
                _ri = _si or _mi or _pi
                _pc = sub.get('possible percentage change', '0')

                return textwrap.dedent(f'''
                    MERGE ({_vn}:{_nl} {{
                        id: "{_nid}"
                    }})
                    SET 
                        {_vn}.type = "{_nt}",
                        {_vn}.update_time = "{datetime.utcnow().isoformat()}",
                        {_vn}.name = "{sub.get('name')}",
                        {_vn}.pep_indicator = "{str(_pi).lower()}",
                        {_vn}.media_indicator = "{str(_mi).lower()}",
                        {_vn}.sanctions_indicator = "{str(_si).lower()}",
                        {_vn}.risk_indicator = "{str(_ri).lower()}",
                        {_vn}.possible_percentage_change = "{str(_pc).lower()}"
                    MERGE ({_vn})-[:OTHER_ULTIMATELY_OWNED_SUBSIDIARY_OF]->({nv})
                ''')
            except Exception as e:
                logger.error(f"[Error] Failed to build Cypher: {e}")
                return ""

        if _misc and len(_misc) < 50:
            _flag = True
            for idx, sub in enumerate(_misc, start=1):
                _cq += await _bq(sub, idx, sess=sess)
        else:
            _hr = []
            for sub in _misc:
                try:
                    _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                    _mi = sub.get("media_indicator", "").lower() != "no"
                    _pi = sub.get("pep_indicator", "").lower() != "no"
                    _ri = _si or _mi or _pi

                    if _ri:
                        _of = 0.0
                        _hr.append((_of, sub))
                except Exception as e:
                    logger.warning(f"[Warning] Skipped sub due to error: {e}")
                    continue

            _top = sorted(_hr, key=lambda x: x[0], reverse=True)[:20]
            for idx, (_, sub) in enumerate(_top, start=1):
                _cq += await _bq(sub, idx, sess=sess, pre="top")

        _cq = f'''
            MATCH (a:Company {{id: "{cid}"}})
            MATCH (s:Supplier {{id: "{sid}"}})-[:SUPPLIER_OF]->(a)
            WITH s as {nv}
            ''' + _cq + f'''SET {nv}.other_ultimate_beneficiary_full_node = "{str(_flag).lower()}"'''
        try:
            _res = await _q0(_cq)
        except Exception as e:
            raise RuntimeError(f"Neo4j query failed for supplier {sid}: {e}")

        return _res
    except Exception as e:
        logger.error(f"[Error] _q8 failed: {e}")
        raise RuntimeError(f"_q8 failed: {e}")


async def _q4(ext_misc, nv, cid, sid, sess):
    try:
        _flag = False
        _cq = ''
        _nvn = ''
        _misc = ext_misc[0].get("other_ultimate_beneficiary", []) if ext_misc and len(ext_misc) else []
        if (_misc == None) or not len(_misc):
            return _cq

        async def _bq(sub, idx, sess, pre=""):
            try:
                _nt = "Organization" if sub.get("bvd_id") else "Individual"
                if _nt != "Organization":
                    logger.info("Empty BVD ID")
                    return _cq
                _nl = "Supplier" if _nt == "Organization" else "Individual"
                _nid = str(uuid.uuid4())
                if _nt == "Organization":
                    try:
                        _d = await get_main_supplier_bvdid_data(
                            required_columns=["ens_id", "session_id", "bvd_id"],
                            bvd_id=sub["bvd_id"],
                            session=sess
                        )
                        _eid = _d[0].get("ens_id") if _d else None
                        _nid = _eid or sub["bvd_id"]
                    except Exception as e:
                        logger.warning(f"[Warning] Error fetching BVD data: {e}")
                        _nid = sub["bvd_id"]

                _vn = f"{nv}_{pre}ultimately_owned_SUBSIDIARIES_{'supplier' if _nt == 'Organization' else 'individual'}_{idx}"

                _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                _mi = sub.get("media_indicator", "").lower() != "no"
                _pi = sub.get("pep_indicator", "").lower() != "no"
                _ri = _si or _mi or _pi
                _pc = sub.get('possible percentage change', '0')

                if str(_nid) == 'None':
                    #logger.info(f"here ---> _q4")
                    _nid = str(uuid.uuid4())

                return textwrap.dedent(f'''
                    MERGE ({_vn}:{_nl} {{
                        id: "{_nid}"
                    }})
                    SET 
                        {_vn}.type = "{_nt}",
                        {_vn}.update_time = "{datetime.utcnow().isoformat()}",
                        {_vn}.name = "{sub.get('name')}",
                        {_vn}.pep_indicator = "{str(_pi).lower()}",
                        {_vn}.media_indicator = "{str(_mi).lower()}",
                        {_vn}.sanctions_indicator = "{str(_si).lower()}",
                        {_vn}.risk_indicator = "{str(_ri).lower()}",
                        {_vn}.possible_percentage_change = "{str(_pc).lower()}"
                    MERGE ({_vn})-[:OTHER_ULTIMATE_BENEFICIARY_OF]->({nv})
                ''')
            except Exception as e:
                logger.error(f"[Error] Failed to build Cypher: {e}")
                return ""

        if _misc and len(_misc) < 50:
            _flag = True
            for idx, sub in enumerate(_misc, start=1):
                _cq += await _bq(sub, idx, sess=sess)
        else:
            _hr = []
            for sub in _misc:
                try:
                    _si = sub.get("sanctions_indicator", "").lower() != "no" or sub.get("watchlist_indicator", "").lower() != "no"
                    _mi = sub.get("media_indicator", "").lower() != "no"
                    _pi = sub.get("pep_indicator", "").lower() != "no"
                    _ri = _si or _mi or _pi
                    _do = sub.get('direct_ownership', '0')

                    if _ri:
                        try:
                            _of = float(_do)
                        except:
                            _of = 0.0
                        _hr.append((_of, sub))
                except Exception as e:
                    logger.warning(f"[Warning] Skipped sub due to error: {e}")
                    continue

            _top = sorted(_hr, key=lambda x: x[0], reverse=True)[:20]
            for idx, (_, sub) in enumerate(_top, start=1):
                _cq += await _bq(sub, idx, sess=sess, pre="top")

        _cq = f'''
            MATCH (a:Company {{id: "{cid}"}})
            MATCH (s:Supplier {{id: "{sid}"}})-[:SUPPLIER_OF]->(a)
            WITH s as {nv}
            ''' + _cq + f'''SET {nv}.other_ultimate_beneficiary_full_node = "{str(_flag).lower()}"'''
        try:
            _res = await _q0(_cq)
        except Exception as e:
            raise RuntimeError(f"Neo4j query failed for supplier {sid}: {e}")

        return _res
    except Exception as e:
        logger.error(f"[Error] _q4 failed: {e}")
        raise RuntimeError(f"_q4 failed: {e}")
def _q6(t: str) -> str:
    _c = re.sub(r'[^a-zA-Z0-9]', '_', t)
    _c = re.sub(r'__+', '_', _c)
    return _c.upper()
async def _q5(ext_misc, nv, cid, sid, sid_):
    _mm = ext_misc[0].get("management", []) if ext_misc else []
    _cq = ''
    _nvn = ''
    if not _mm:
        return ''

    def _cr(m):
        _si = m.get("sanctions_indicator", "").lower() != "no" or m.get("watchlist_indicator", "").lower() != "no"
        _mi = m.get("media_indicator", "").lower() != "no"
        _pi = m.get("pep_indicator", "").lower() != "no"
        return _si, _mi, _pi, _si or _mi or _pi

    if len(_mm) > 50:
        _mm = [m for m in _mm if _cr(m)[3]][:20]

    for idx, m in enumerate(_mm, start=1):
        try:
            _iid = m.get("id", str(uuid.uuid4()))
            _nm = m.get("name", "").replace('"', '\\"')
            _jt = m.get("job_title", "").replace('"', '\\"')
            _dp = m.get("department", "").replace('"', '\\"')
            _hr = m.get("heirarchy", "").replace('"', '\\"')
            _cp = m.get("current_or_previous", "").replace('"', '\\"')
            _ish = m.get("is_shareholder", "").lower() != "no"

            _si, _mi, _pi, _ri = _cr(m)
            _nvn = f"{nv}_management_individual_{idx}"

            _cq += textwrap.dedent(f'''
                MERGE ({_nvn}:Individual {{
                    id: "{_iid}"
                }})
                SET 
                    {_nvn}.type = "individual",
                    {_nvn}.name = "{_nm}",
                    {_nvn}.pep_indicator = "{str(_pi).lower()}",
                    {_nvn}.media_indicator = "{str(_mi).lower()}",
                    {_nvn}.sanctions_indicator = "{str(_si).lower()}",
                    {_nvn}.risk_indicator = "{str(_ri).lower()}",
                    {_nvn}.department = "{_dp.lower()}",
                    {_nvn}.heirarchy = "{_hr.lower()}",
                    {_nvn}.is_shareholder = "{str(_ish).lower()}",
                    {_nvn}.current_or_previous = "{_cp.lower()}",
                    {_nvn}.job_title = "{_jt.lower() if _jt else ''}"
                MERGE ({_nvn})-[:MANAGEMENT_OF]->({nv})
            ''')

            if _jt:
                _rel = _q6(_jt)
                _cq += textwrap.dedent(f'''
                MERGE ({_nvn})-[:{_rel}]->({nv})
            ''')
            _cq += "\n"
        except Exception as e:
            logger.warning(f"[Warning] Skipped management entry due to error: {e}")
            continue

    _main_cq = f'''
    MATCH (a:Company {{id: "{cid}"}})
    MATCH (s:Supplier {{id: "{sid}"}})-[:SUPPLIER_OF]->(a)
    WITH s as {nv}
    ''' + _cq
    try:
        _res = await _q0(_main_cq)
    except Exception as e:
        raise RuntimeError(f"Neo4j query failed for supplier {sid}: {e}")

    return _res


async def _q3(sess, cid: str, sid_: str):
    """
    Adds or updates supplier nodes and links them to an existing Aramco node.
    This version chunks the query per supplier to prevent Neo4j StackOverflow.
    """

    _chk = f'''
    MATCH (a:Company {{id: "{cid}"}})
    RETURN count(a) AS count
    '''
    try:
        _res = await _q0(_chk)
        if not _res or not _res.get("records") or _res["records"][0].get("count", 0) == 0:
            raise HTTPException(status_code=404, detail=f"Head node with id '{cid}' does not exist.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to verify Aramco node: {str(e)}")

    try:
        _d = await get_dynamic_ens_data("ovar", required_columns=["all"], ens_id=None, session_id=sid_, session=sess)
        _fs = [r for r in _d if r.get("kpi_area") == "overall_rating" and r.get("kpi_rating") in ["High", "Medium"]]
        if not _fs:
            raise HTTPException(status_code=404, detail="No suppliers with High/Medium rating found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch supplier data: {str(e)}")

    _ss = []

    for idx, s in enumerate(_fs, start=1):
        try:
            _nv = f"direct_supplier_{idx}"
            _sid = s["ens_id"]

            _m1 = await get_dynamic_ens_data("supplier_master_data", ["all"], _sid, None, sess)
            _cm = await get_dynamic_ens_data("company_profile", ["all"], _sid, None, sess)
            _om = await get_dynamic_ens_data("ovar", ["all"], _sid, None, sess)
            _ext = await get_dynamic_ens_data("external_supplier_data", ["all"], _sid, sid_, sess)

            _res_node, _sid = await _q2(_m1, _cm, _om, s, _nv, cid)

            _tasks = [
                _q5(_ext, _nv, cid, _sid, sess),
                _q9(_ext, _nv, cid, _sid, sess),
                _q10(_ext, _nv, cid, _sid, sess),
                _q11(_ext, _nv, cid, _sid, sess),
                _q7(_ext, _nv, cid, _sid, sess),
                _q4(_ext, _nv, cid, _sid, sess)
            ]

            _results = await asyncio.gather(*_tasks, return_exceptions=True)

            for _r in _results:
                if isinstance(_r, Exception):
                    raise _r

            _ss.append({"supplier": _sid, "status": "pass"})

        except Exception as e:
            logger.info(f"[❌ Error] Supplier {_sid} failed: {e}")
            _ss.append({"supplier": _sid, "status": "fail", "error": str(e)})

    return {
        "status": "pass",
        "message": "Suppliers updated in parallel steps",
        "supplier_statuses": _ss
    }
