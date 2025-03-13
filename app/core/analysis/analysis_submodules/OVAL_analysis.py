import asyncio
from app.core.utils.db_utils import *
import json
import ast


import json

async def ownership_analysis(data, session):
    print("Performing Ownership Structure Analysis... Started")

    kpi_area_module = "OWN"

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    try:
        kpi_template = {
            "kpi_area": kpi_area_module,
            "kpi_code": "",
            "kpi_definition": "",
            "kpi_flag": False,
            "kpi_value": None,
            "kpi_rating": "",
            "kpi_details": ""
        }

        OWN1A = kpi_template.copy()
        OWN1A["kpi_code"] = "OWN1A"
        OWN1A["kpi_definition"] = "Direct Shareholder With > 50% Ownership"  # TODO SET THRESHOLD

        required_columns = [
            "shareholders", "controlling_shareholders", "controlling_shareholders_type",
            "beneficial_owners", "beneficial_owners_intermediatory", "global_ultimate_owner",
            "global_ultimate_owner_type", "other_ultimate_beneficiary", "ultimately_owned_subsidiaries"
        ]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        shareholders = retrieved_data.get("shareholders", None)
        controlling_shareholders = retrieved_data.get("controlling_shareholders", None)

        # Check if all/any mandatory required data is None
        if all(var is None for var in [shareholders, controlling_shareholders]):
            print(f"{kpi_area_module} Analysis... Completed With No Data")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        def is_valid_ownership(value):
            """Checks if the value is a valid number greater than 50."""
            if not value or value == "n.a.":
                return False
            try:
                return float(value) > 50
            except ValueError:
                return False

        # ---- PERFORM ANALYSIS LOGIC HERE ----
        def process_ownership(owners_list):
            """Processes the list of owners and checks for >50% ownership."""
            for owner in owners_list:
                total = owner.get("total_ownership", "n.a.")
                direct = owner.get("direct_ownership", "n.a.")

                if is_valid_ownership(total):
                    OWN1A["kpi_value"] = json.dumps(owner)
                    OWN1A["kpi_details"] = f"Shareholder {owner.get('name')} has total ownership of {total}%"
                    OWN1A["kpi_rating"] = "HIGH"
                    return True  # Exit after finding the first valid owner

                if is_valid_ownership(direct):
                    OWN1A["kpi_value"] = json.dumps(owner)
                    OWN1A["kpi_details"] = f"Shareholder {owner.get('name')} has direct ownership of {direct}%"
                    OWN1A["kpi_rating"] = "HIGH"
                    return True  # Exit after finding the first valid owner
            return False

        if controlling_shareholders and process_ownership(controlling_shareholders):
            pass
        elif shareholders and process_ownership(shareholders):
            pass

        own_kpis = [OWN1A]
        insert_status = await upsert_kpi("oval", own_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}, {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}


async def ownership_flag(data, session):
    print("Performing Ownership Structure Analysis... Started")

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    try:

        kpi_template = {
            "kpi_area": "",
            "kpi_code": "",
            "kpi_definition": "",
            "kpi_flag": False,
            "kpi_value": '',
            "kpi_rating": "",
            "kpi_details": ""
        }

        SAN3A = kpi_template.copy()
        PEP3A = kpi_template.copy()
        AMR2A = kpi_template.copy()

        SAN3A["kpi_area"] = "SAN"
        SAN3A["kpi_code"] = "SAN3A"
        SAN3A["kpi_definition"] = "Associated Corporate Group - Sanctions Exposure"

        PEP3A["kpi_area"] = "PEP"
        PEP3A["kpi_code"] = "PEP3A"
        PEP3A["kpi_definition"] = "Associated Corporate Group - PeP Exposure"

        AMR2A["kpi_area"] = "AMR"
        AMR2A["kpi_code"] = "AMR2A"
        AMR2A["kpi_definition"] = "Associated Corporate Group - Media Exposure"

        sanctions_watchlist_findings = []
        pep_findings = []
        media_findings = []
        sanctions_watchlist_details = []
        pep_details = []
        media_details = []
        sanctions_watchlist_kpi_flag = False
        pep_kpi_flag = False
        media_kpi_flag = False


        required_columns = ["shareholders", "beneficial_owners", "global_ultimate_owner", "other_ultimate_beneficiary",
                            "ultimately_owned_subsidiaries"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value,
                                                    session_id_value, session)
        retrieved_data = retrieved_data[0]

        shareholders = retrieved_data.get("shareholders", None)
        beneficial_owners = retrieved_data.get("beneficial_owners", None)
        global_ultimate_owner = retrieved_data.get("global_ultimate_owner", None)
        other_ultimate_beneficiary = retrieved_data.get("other_ultimate_beneficiary", None)
        ultimately_owned_subsidiaries = retrieved_data.get("ultimately_owned_subsidiaries", None)

        # Check if all/any mandatory required data is None - (if so then add one general?) and return
        if all(var is None for var in
               [shareholders, beneficial_owners, global_ultimate_owner, other_ultimate_beneficiary,
                ultimately_owned_subsidiaries]):
            print(f"ownership_flag Analysis... Completed With No Data")
            return {"ens_id": ens_id_value, "module": "OVAL", "status": "completed", "info": "no_data"}

        # ---- PERFORM ANALYSIS LOGIC HERE
        if global_ultimate_owner is not None:
            rel_type_str = "Global Ultimate Owner:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in global_ultimate_owner:
                name = sh.get("name",None)
                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "global_ultimate_owner"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name)
                if media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))

        if shareholders is not None:
            rel_type_str = "Shareholders:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in shareholders:
                name = sh.get("name", None)
                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "shareholders"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name)
                if media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))

        if beneficial_owners is not None:
            rel_type_str = "Beneficial Owners:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in beneficial_owners:
                name = sh.get("name", None)
                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "beneficial_owners"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name)
                if media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))
        #
        if other_ultimate_beneficiary is not None:
            rel_type_str = "Other Ultimate Beneficiaries:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in other_ultimate_beneficiary:
                name = sh.get("name", None)
                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "other_ultimate_beneficiary"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name)
                if media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))

        if ultimately_owned_subsidiaries is not None:
            rel_type_str = "Ultimately Owned Subsidiaries:\n"
            sanc_names = []
            pep_names = []
            media_names = []
            sanc_flag_tmp = False
            pep_flag_tmp = False
            media_flag_tmp = False
            for sh in ultimately_owned_subsidiaries:
                name = sh.get("name", None)
                sanctions_indicator = sh.get("sanctions_indicator", "n.a.")
                watchlist_indicator = sh.get("watchlist_indicator", "n.a.")
                pep_indicator = sh.get("pep_indicator", "n.a.")
                media_indicator = sh.get("media_indicator", "n.a.")
                sh["corporate_group_type"] = "ultimately_owned_subsidiaries"

                if sanctions_indicator.lower() == 'yes' or watchlist_indicator.lower() == 'yes':
                    sanc_flag_tmp = True
                    sanctions_watchlist_findings.append(sh)
                    sanc_names.append(name)
                if pep_indicator.lower() == 'yes':
                    pep_flag_tmp = True
                    pep_findings.append(sh)
                    pep_names.append(name)
                if media_indicator.lower() == 'yes':
                    media_flag_tmp = True
                    media_findings.append(sh)
                    media_names.append(name)

            if sanc_flag_tmp:
                sanctions_watchlist_kpi_flag = True
                sanctions_watchlist_details.append(rel_type_str + ',\n'.join(sanc_names[:5]))
            if pep_flag_tmp:
                pep_kpi_flag = True
                pep_details.append(rel_type_str + ',\n'.join(pep_names[:5]))
            if media_flag_tmp:
                media_kpi_flag = True
                media_details.append(rel_type_str + ',\n'.join(media_names[:5]))


        if sanctions_watchlist_kpi_flag:
            total_san_count = len(sanctions_watchlist_findings)
            SAN3A["kpi_flag"] = True
            SAN3A["kpi_rating"] = "Medium"
            SAN3A["kpi_details"] = "Sanctions or watchlist exposure identified among following members of the corporate group:\n" +"\n \n".join(sanctions_watchlist_details) + ("\n...& "+str(total_san_count-5)+" more findings" if total_san_count > 6 else "")
            kpi_dict = {
                "count": total_san_count if total_san_count < 6 else "5 or more",
                "findings": sanctions_watchlist_findings
            }
            SAN3A["kpi_value"] = json.dumps(kpi_dict)

            san_kpis = [SAN3A]
            insert_status = await upsert_kpi("sape", san_kpis, ens_id_value, session_id_value, session)

            if insert_status["status"] == "success":
                print(f"SAN3A Analysis... Completed Successfully")
                return {"ens_id": ens_id_value, "module": "SAN3A", "status": "completed", "info": "analysed"}
            else:
                print(insert_status)
                return {"ens_id": ens_id_value, "module": "SAN3A", "status": "failure","info": "database_saving_error"}

        if pep_kpi_flag:
            total_pep_count = len(pep_findings)
            PEP3A["kpi_flag"] = True
            PEP3A["kpi_rating"] = "Medium"
            PEP3A["kpi_details"] = "PEP exposure identified among following members of the corporate group:\n"+"\n \n".join(pep_details) + ("\n...& "+str(total_pep_count-5)+" more findings" if total_pep_count > 5 else "")
            kpi_dict = {
                "count": total_pep_count if total_pep_count < 6 else "5 or more",
                "findings": pep_findings
            }
            PEP3A["kpi_value"] = json.dumps(kpi_dict)

            pep_kpis = [PEP3A]
            insert_status = await upsert_kpi("sown", pep_kpis, ens_id_value, session_id_value, session)

            if insert_status["status"] == "success":
                print(f"PEP3A Analysis... Completed Successfully")
                return {"ens_id": ens_id_value, "module": "PEP3A", "status": "completed", "info": "analysed"}
            else:
                print(insert_status)
                return {"ens_id": ens_id_value, "module": "PEP3A", "status": "failure","info": "database_saving_error"}

        if media_kpi_flag:
            total_med_count = len(media_findings)
            AMR2A["kpi_flag"] = True
            AMR2A["kpi_rating"] = "Medium"
            AMR2A["kpi_details"] = "Media exposure identified among following members of the corporate group:\n"+"\n \n".join(media_details) + ("\n...& "+str(total_med_count-5)+" more findings" if total_med_count > 5 else "")
            kpi_dict = {
                "count": total_med_count if total_med_count < 6 else "5 or more",
                "findings": media_findings
            }
            AMR2A["kpi_value"] = json.dumps(kpi_dict)

            amr_kpis = [AMR2A]
            insert_status = await upsert_kpi("rfct", amr_kpis, ens_id_value, session_id_value, session)

            if insert_status["status"] == "success":
                print(f"AMR2A Analysis... Completed Successfully")
                return {"ens_id": ens_id_value, "module": "AMR2A", "status": "completed", "info": "analysed"}
            else:
                print(insert_status)
                return {"ens_id": ens_id_value, "module": "AMR2A", "status": "failure","info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: OVAL EXTRA, {str(e)}")
        return {"ens_id": ens_id_value, "module": "OVAL EXTRA", "status": "failure", "info": str(e)}
