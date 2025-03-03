import asyncio
import json
from app.core.utils.db_utils import *

async def bankruptcy_and_financial_risk_analysis(data, session):
    print("Performing Bankruptcy Analysis.... Started")

    kpi_area_module = "BKR"

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    try:
        kpi_template = {
            "kpi_area": kpi_area_module,
            "kpi_code": "",
            "kpi_definition": "",
            "kpi_flag": False,  # Initialize kpi_flag as False
            "kpi_value": None,
            "kpi_rating": "",
            "kpi_details": ""
        }

        BKR1A = kpi_template.copy()
        BKR2A = kpi_template.copy()
        BKR3A = kpi_template.copy()

        BKR1A["kpi_code"] = "BKR1A"
        BKR1A["kpi_definition"] = "Financial Risk include for 1 year"

        BKR2A["kpi_code"] = "BKR3A"
        BKR2A["kpi_definition"] = "Qualitative Risk"

        BKR3A["kpi_code"] = "BKR4A"
        BKR3A["kpi_definition"] = "Payment Risk"

        required_columns = [
            "pr_more_risk_score_ratio",
            "pr_reactive_more_risk_score_ratio",
            "pr_qualitative_score",
            "pr_qualitative_score_date",
            "payment_risk_score"
        ]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value,
                                                    session_id_value, session)
        retrieved_data = retrieved_data[0]

        pr_more_risk_score_ratio = retrieved_data.get("pr_more_risk_score_ratio", {})
        pr_reactive_more_risk_score_ratio = retrieved_data.get("pr_reactive_more_risk_score_ratio", {})
        pr_qualitative_score = retrieved_data.get("pr_qualitative_score")
        pr_qualitative_score_date = retrieved_data.get("pr_qualitative_score_date")
        payment_risk_score = retrieved_data.get("payment_risk_score")

        # Ensure both are dictionaries
        if not isinstance(pr_more_risk_score_ratio, dict):
            pr_more_risk_score_ratio = {}
        if not isinstance(pr_reactive_more_risk_score_ratio, dict):
            pr_reactive_more_risk_score_ratio = {}

        # Combine both JSON objects into a single dictionary for financial ratios
        all_ratios = {**pr_more_risk_score_ratio, **pr_reactive_more_risk_score_ratio}

        healthy_ratings = ["AAA", "AA", "A"]
        adequate_ratings = ["BBB", "BB"]
        vulnerable_ratings = ["B", "CCC"]
        risky_ratings = ["CC", "C", "D"]

        kpi_rating = "INFO"
        kpi_details = []
        kpi_values = {}

        # Separate fields by risk level
        high_risk_fields = []
        medium_risk_fields = []
        low_risk_fields = []

        for ratio, value in all_ratios.items():
            if value == "n.a" or value is None:
                print(f"Skipping {ratio} because it is 'n.a' or None")
                continue

            # Add field names and values to kpi_values based on risk level
            if value in healthy_ratings:
                low_risk_fields.append(ratio)
                kpi_values[ratio] = value
            elif value in adequate_ratings:
                medium_risk_fields.append(ratio)
                kpi_values[ratio] = value
            elif value in vulnerable_ratings or value in risky_ratings:
                high_risk_fields.append(ratio)
                kpi_values[ratio] = value

        # Determine overall KPI rating for financial ratios
        if high_risk_fields:
            kpi_rating = "High"
            kpi_details = high_risk_fields
            BKR1A["kpi_flag"] = True
        elif medium_risk_fields:
            kpi_rating = "Medium"
            kpi_details = medium_risk_fields
            BKR1A["kpi_flag"] = True
        elif low_risk_fields:
            kpi_rating = "Low"
            kpi_details = low_risk_fields
            BKR1A["kpi_flag"] = True
        else:
            kpi_rating = "INFO"
            kpi_details = ["No financial information available"]

        # Update KPI values and details for BKR1A
        BKR1A["kpi_value"] = json.dumps(kpi_values) if kpi_values else None
        BKR1A["kpi_rating"] = kpi_rating

        if kpi_rating == "INFO":
            BKR1A["kpi_details"] = "No financial information available"
        else:
            BKR1A["kpi_details"] = f"Financial Risk Rating: {kpi_rating} due to {', '.join(kpi_details)}"

        # Process qualitative risk (BKR2A)
        if pr_qualitative_score is not None and pr_qualitative_score != "n.a":
            if pr_qualitative_score in ["A", "B"]:
                BKR2A["kpi_rating"] = "Low"
            elif pr_qualitative_score == "C":
                BKR2A["kpi_rating"] = "Medium"
            elif pr_qualitative_score in ["D", "E"]:
                BKR2A["kpi_rating"] = "High"
            BKR2A["kpi_value"] = pr_qualitative_score
            BKR2A["kpi_details"] = f"Qualitative Risk: {BKR2A['kpi_rating']}"
            if pr_qualitative_score_date is not None:
                BKR2A["kpi_details"] += f" (as of {pr_qualitative_score_date})"
            BKR2A["kpi_flag"] = True
        else:
            BKR2A["kpi_rating"] = "INFO"
            BKR2A["kpi_details"] = "No qualitative information available"

        # Process payment risk (BKR3A)
        if payment_risk_score is not None and payment_risk_score != "n.a":
            if payment_risk_score < 510:
                BKR3A["kpi_rating"] = "Low"
            elif 510 <= payment_risk_score <= 629:
                BKR3A["kpi_rating"] = "Medium"
            elif payment_risk_score >= 630:
                BKR3A["kpi_rating"] = "High"
            BKR3A["kpi_value"] = str(payment_risk_score)
            BKR3A["kpi_details"] = f"Payment Risk: {BKR3A['kpi_rating']}"
            BKR3A["kpi_flag"] = True
        else:
            BKR3A["kpi_rating"] = "INFO"
            BKR3A["kpi_details"] = "No payment risk information available"

        # Append all KPIs to the list
        bkr_kpis = [BKR1A, BKR2A, BKR3A]

        insert_status = await upsert_kpi("fstb", bkr_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure",
                    "info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}


async def financials_analysis(data, session):
    print("Performing FINANCIALS Analysis... Started")

    kpi_area_module = "FIN"
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    try:
        kpi_template = {
            "kpi_area": kpi_area_module,
            "kpi_code": "",
            "kpi_definition": "",
            "kpi_flag": True,
            "kpi_value": None,
            "kpi_rating": "",
            "kpi_details": ""
        }

        required_columns = [
            "operating_revenue", "profit_loss_after_tax", "ebitda", "cash_flow", "pl_before_tax",
            "roce_before_tax", "roe_before_tax", "roe_using_net_income", "profit_margin",
            "shareholders_fund", "total_assets", "current_ratio", "solvency_ratio"
        ]

        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value,
                                                    session_id_value, session)
        retrieved_data = retrieved_data[0]

        # Check if all required data is None
        if all(retrieved_data.get(col) is None for col in required_columns):
            print(f"{kpi_area_module} Analysis... Completed With No Data")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        fin_kpis = []

        # Dynamically generate KPI entries
        for idx, col in enumerate(required_columns, start=1):
            metric_data = retrieved_data.get(col)
            if metric_data and len(metric_data) > 0:
                kpi_entry = kpi_template.copy()
                kpi_entry["kpi_code"] = f"FIN{idx}A"
                kpi_entry["kpi_rating"] = "INFO"
                kpi_entry["kpi_definition"] = f"{col.replace('_', ' ').title()} Last 4 Years (USD, th)"
                kpi_entry["kpi_value"] = json.dumps(metric_data)

                details = "".join(f"[{val['closing_date']}]: {round(val['value'], 2)}\n" for val in metric_data)
                kpi_entry["kpi_details"] = details

                fin_kpis.append(kpi_entry)

        # Insert KPI data into database
        insert_status = await upsert_kpi("fstb", fin_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            print(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            print(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure","info": "database_saving_error"}

    except Exception as e:
        print(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}



