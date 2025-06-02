import json
from app.core.utils.db_utils import *
from app.schemas.logger import logger

# async def bankruptcy_and_financial_risk_analysis(data, session):
#     """
#     Perform bankruptcy and financial risk analysis
#
#     Args:
#         data (dict): Input data containing entity and session information
#         session : Database session
#
#     Returns:
#         dict: Analysis results with KPI information
#     """
#     print("Performing Bankruptcy Analysis.... Started")
#
#     kpi_area_module = "BKR"
#
#     ens_id_value = data.get("ens_id")
#     session_id_value = data.get("session_id")
#
#     try:
#         # Basic KPI template
#         kpi_template = {
#             "kpi_area": kpi_area_module,
#             "kpi_code": "",
#             "kpi_definition": "",
#             "kpi_flag": False,
#             "kpi_value": None,
#             "kpi_rating": "",
#             "kpi_details": ""
#         }
#
#         # Create KPI objects
#         BKR1A = kpi_template.copy()
#         BKR2A = kpi_template.copy()
#         BKR3A = kpi_template.copy()
#
#         # Define KPI details
#         BKR1A["kpi_code"] = "BKR1A"
#         BKR1A["kpi_definition"] = "Financial Risk Score"
#
#         BKR2A["kpi_code"] = "BKR2A"
#         BKR2A["kpi_definition"] = "Qualitative Risk"
#
#         BKR3A["kpi_code"] = "BKR3A"
#         BKR3A["kpi_definition"] = "Payment Risk"
#
#         # Required columns for data retrieval
#         required_columns = [
#             "pr_more_risk_score_ratio",
#             "pr_reactive_more_risk_score_ratio",
#             "pr_qualitative_score",
#             "pr_qualitative_score_date",
#             "payment_risk_score"
#         ]
#
#         # Retrieve external supplier data
#         retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
#
#         # Handle case where no data is retrieved
#         if not retrieved_data:
#             print("No external supplier data found for bankruptcy analysis")
#             return {
#                 "ens_id": ens_id_value,
#                 "module": kpi_area_module,
#                 "status": "failure",
#                 "info": "no_data_found"
#             }
#
#         retrieved_data = retrieved_data[0]
#
#         # Extract specific risk metrics
#         pr_more_risk_score_ratio = retrieved_data.get("pr_more_risk_score_ratio", {})
#         pr_reactive_more_risk_score_ratio = retrieved_data.get("pr_reactive_more_risk_score_ratio", {})
#         pr_qualitative_score = retrieved_data.get("pr_qualitative_score")
#         pr_qualitative_score_date = retrieved_data.get("pr_qualitative_score_date")
#         payment_risk_score = retrieved_data.get("payment_risk_score")
#
#         # Ensure risk ratio data is a dictionary
#         pr_more_risk_score_ratio = pr_more_risk_score_ratio if isinstance(pr_more_risk_score_ratio, dict) else {}
#         pr_reactive_more_risk_score_ratio = pr_reactive_more_risk_score_ratio if isinstance(
#             pr_reactive_more_risk_score_ratio, dict) else {}
#
#         # Combine risk ratio dictionaries
#         all_ratios = {**pr_more_risk_score_ratio, **pr_reactive_more_risk_score_ratio}
#
#         # Risk rating categories
#         healthy_ratings = ["AAA", "AA", "A"]
#         adequate_ratings = ["BBB", "BB"]
#         vulnerable_ratings = ["B", "CCC"]
#         risky_ratings = ["CC", "C", "D"]
#
#         # Initialize risk analysis variables
#         kpi_rating = "INFO"
#         kpi_details = []
#         kpi_values = {}
#
#         # Categorize risk fields
#         high_risk_fields = []
#         medium_risk_fields = []
#         low_risk_fields = []
#
#         # Analyze risk ratios
#         for ratio, value in all_ratios.items():
#             if value in ["n.a", None]:
#                 continue
#
#             # Categorize ratios by risk level
#             if value in healthy_ratings:
#                 low_risk_fields.append(ratio)
#                 kpi_values[ratio] = value
#             elif value in adequate_ratings:
#                 medium_risk_fields.append(ratio)
#                 kpi_values[ratio] = value
#             elif value in vulnerable_ratings or value in risky_ratings:
#                 high_risk_fields.append(ratio)
#                 kpi_values[ratio] = value
#
#         # Determine overall KPI rating
#         if high_risk_fields:
#             kpi_rating = "High"
#             kpi_details = high_risk_fields
#             BKR1A["kpi_flag"] = True
#         elif medium_risk_fields:
#             kpi_rating = "Medium"
#             kpi_details = medium_risk_fields
#             BKR1A["kpi_flag"] = True
#         elif low_risk_fields:
#             kpi_rating = "Low"
#             kpi_details = low_risk_fields
#             BKR1A["kpi_flag"] = True
#         else:
#             kpi_rating = "INFO"
#             kpi_details = ["No financial information available"]
#
#         # Update BKR1A KPI values
#         BKR1A["kpi_value"] = json.dumps(kpi_values) if kpi_values else None
#         BKR1A["kpi_rating"] = kpi_rating
#
#         # Set KPI details
#         if kpi_rating == "INFO":
#             BKR1A["kpi_details"] = "No financial information available"
#         else:
#             BKR1A["kpi_details"] = f"Financial Risk Rating: {kpi_rating} due to {', '.join(kpi_details)}"
#
#         # Process qualitative risk (BKR2A)
#         if pr_qualitative_score not in [None, "n.a"]:
#             if pr_qualitative_score in ["A", "B"]:
#                 BKR2A["kpi_rating"] = "Low"
#             elif pr_qualitative_score == "C":
#                 BKR2A["kpi_rating"] = "Medium"
#             elif pr_qualitative_score in ["D", "E"]:
#                 BKR2A["kpi_rating"] = "High"
#
#             BKR2A["kpi_value"] = pr_qualitative_score
#             BKR2A["kpi_flag"] = True
#             BKR2A["kpi_details"] = f"Qualitative Risk: {BKR2A['kpi_rating']}"
#             if pr_qualitative_score_date:
#                 BKR2A["kpi_details"] += f" (as of {pr_qualitative_score_date})"
#         else:
#             BKR2A["kpi_rating"] = "INFO"
#             BKR2A["kpi_details"] = "No qualitative information available"
#
#         # Process payment risk (BKR3A)
#         if payment_risk_score not in [None, "n.a"]:
#             if payment_risk_score < 510:
#                 BKR3A["kpi_rating"] = "Low"
#             elif 510 <= payment_risk_score <= 629:
#                 BKR3A["kpi_rating"] = "Medium"
#             elif payment_risk_score >= 630:
#                 BKR3A["kpi_rating"] = "High"
#
#             BKR3A["kpi_value"] = str(payment_risk_score)
#             BKR3A["kpi_flag"] = True
#             BKR3A["kpi_details"] = f"Payment Risk: {BKR3A['kpi_rating']}"
#         else:
#             BKR3A["kpi_rating"] = "INFO"
#             BKR3A["kpi_details"] = "No payment risk information available"
#
#         # Prepare KPIs for insertion
#         bkr_kpis = [BKR1A, BKR2A, BKR3A]
#
#         # Insert KPIs into database
#         insert_status = await upsert_kpi("fstb", bkr_kpis, ens_id_value, session_id_value, session)
#
#         # Return analysis results
#         if insert_status["status"] == "success":
#             print(f"{kpi_area_module} Analysis... Completed Successfully")
#             return {
#                 "ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed", "kpis": bkr_kpis
#             }
#         else:
#             print(insert_status)
#             return {
#                 "ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": "database_saving_error"
#             }
#
#     except Exception as e:
#         print(f"Error in module: {kpi_area_module}: {str(e)}")
#         return {
#             "ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)
#         }

async def financial_ratios_analysis(data, session):
    """
    Perform financial ratios analysis including quick ratio, current ratio, and debt to equity ratio

    Args:
        data (dict): Input data containing entity and session information
        session : Database session

    Returns:
        dict: Analysis results with KPI information
    """
    logger.info("Performing Financial Ratios Analysis.... Started")

    kpi_area_module = "BKR"

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    try:
        # Basic KPI template
        kpi_template = {
            "kpi_area": kpi_area_module,
            "kpi_code": "",
            "kpi_definition": "",
            "kpi_flag": False,
            "kpi_value": None,
            "kpi_rating": "",
            "kpi_details": ""
        }

        # Create KPI objects
        BKR4A = kpi_template.copy()
        BKR5A = kpi_template.copy()
        BKR6A = kpi_template.copy()

        BKR4A["kpi_code"] = "BKR4A"
        BKR4A["kpi_definition"] = "Quick Ratio"

        BKR5A["kpi_code"] = "BKR5A"
        BKR5A["kpi_definition"] = "Current Ratio"

        BKR6A["kpi_code"] = "BKR6A"
        BKR6A["kpi_definition"] = "Debt to Equity Ratio"

        required_columns = [
            "pr_more_risk_score_ratio",
            "pr_reactive_more_risk_score_ratio",
            "current_ratio",
            "long_and_short_term_debt",
            "long_term_debt",
            "total_shareholders_equity"
        ]

        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value,
                                                    session_id_value, session)

        if not retrieved_data:
            logger.warning("No external supplier data found for financial ratios analysis")
            return {
                "ens_id": ens_id_value,
                "module": kpi_area_module,
                "status": "failure",
                "info": "no_data_found"
            }
        retrieved_data = retrieved_data[0]

        pr_more_risk_score_ratio = retrieved_data.get("pr_more_risk_score_ratio", {})
        pr_reactive_more_risk_score_ratio = retrieved_data.get("pr_reactive_more_risk_score_ratio", {})
        current_ratio = retrieved_data.get("current_ratio")
        short_debt = retrieved_data.get("long_and_short_term_debt")
        long_debt = retrieved_data.get("long_term_debt")
        total_equity = retrieved_data.get("total_shareholders_equity")

        pr_more_risk_score_ratio = pr_more_risk_score_ratio if isinstance(pr_more_risk_score_ratio, dict) else {}
        pr_reactive_more_risk_score_ratio = pr_reactive_more_risk_score_ratio if isinstance(
            pr_reactive_more_risk_score_ratio, dict) else {}

        healthy_ratings = ["AAA", "AA", "A"]
        adequate_ratings = ["BBB", "BB"]
        vulnerable_ratings = ["B", "CCC"]
        risky_ratings = ["CC", "C", "D"]

        # Process Quick Ratio (BKR4A)
        quick_ratio_value = None

        # Prioritize pr_more_risk_score_ratio over pr_reactive_more_risk_score_ratio
        if "quick ratio" in pr_more_risk_score_ratio and pr_more_risk_score_ratio["quick ratio"] not in ["n.a", None]:
            quick_ratio_value = pr_more_risk_score_ratio["quick ratio"]
        elif "quick ratio" in pr_reactive_more_risk_score_ratio and pr_reactive_more_risk_score_ratio["quick ratio"] not in ["n.a", None]:
            quick_ratio_value = pr_reactive_more_risk_score_ratio["quick ratio"]

        if quick_ratio_value:
            if quick_ratio_value in healthy_ratings:
                BKR4A["kpi_rating"] = "Low"
            elif quick_ratio_value in adequate_ratings:
                BKR4A["kpi_rating"] = "Medium"
            elif quick_ratio_value in vulnerable_ratings or quick_ratio_value in risky_ratings:
                BKR4A["kpi_rating"] = "High"

            BKR4A["kpi_value"] = quick_ratio_value
            BKR4A["kpi_flag"] = True
            BKR4A["kpi_details"] = f"Quick Ratio: {BKR4A['kpi_rating']} ({quick_ratio_value})"
        else:
            BKR4A["kpi_rating"] = "INFO"
            BKR4A["kpi_details"] = "No quick ratio information available"

        # Process Current Ratio (BKR5A)
        if current_ratio not in [None, "n.a"]:
            try:
                if isinstance(current_ratio, list) and len(current_ratio) > 0:
                    first_ratio = current_ratio[0].get("value")
                    if first_ratio is not None:
                        current_ratio_float = float(first_ratio)
                    else:
                        raise ValueError("Missing 'value' in current ratio data")
                else:
                    current_ratio_float = float(current_ratio)

                if current_ratio_float < 0.5:
                    BKR5A["kpi_rating"] = "High"
                elif 0.5 <= current_ratio_float <= 1.5:
                    BKR5A["kpi_rating"] = "Medium"
                else:
                    BKR5A["kpi_rating"] = "Low"

                BKR5A["kpi_value"] = str(current_ratio_float)
                BKR5A["kpi_flag"] = True
                BKR5A["kpi_details"] = f"Current Ratio: {BKR5A['kpi_rating']} ({current_ratio_float})"
            except (ValueError, TypeError) as e:
                BKR5A["kpi_rating"] = "INFO"
                BKR5A["kpi_details"] = f"Invalid current ratio value: {str(e)}"
        else:
            BKR5A["kpi_rating"] = "INFO"
            BKR5A["kpi_details"] = "No current ratio information available"

        # Process Debt to Equity Ratio (BKR6A)
        if all(x not in [None, "n.a"] for x in [short_debt, long_debt, total_equity]) and total_equity != 0:
            try:
                short_debt_float = float(short_debt)
                long_debt_float = float(long_debt)
                total_equity_float = float(total_equity)

                if total_equity_float == 0:
                    BKR6A["kpi_rating"] = "High"
                    BKR6A["kpi_details"] = "Debt to Equity Ratio: High (Total equity is zero)"
                else:
                    debt_to_equity = (short_debt_float + long_debt_float) / total_equity_float

                    if debt_to_equity > 5:
                        BKR6A["kpi_rating"] = "High"
                    elif 2 <= debt_to_equity <= 5:
                        BKR6A["kpi_rating"] = "Medium"
                    else:
                        BKR6A["kpi_rating"] = "Low"

                    BKR6A["kpi_value"] = str(round(debt_to_equity, 2))
                    BKR6A["kpi_flag"] = True
                    BKR6A["kpi_details"] = f"Debt to Equity Ratio: {BKR6A['kpi_rating']} ({round(debt_to_equity, 2)})"
            except (ValueError, TypeError):
                BKR6A["kpi_rating"] = "INFO"
                BKR6A["kpi_details"] = "Invalid debt or equity values"
        else:
            BKR6A["kpi_rating"] = "INFO"
            BKR6A["kpi_details"] = "No debt to equity information available"

        financial_ratio_kpis = [BKR4A, BKR5A, BKR6A]
        insert_status = await upsert_kpi("fstb", financial_ratio_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            logger.info("Financial Ratios Analysis... Completed Successfully")
            return {
                "ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed",
                "kpis": financial_ratio_kpis
            }
        else:
            logger.error(insert_status)
            return {
                "ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": "database_saving_error"
            }

    except Exception as e:
        logger.error(f"Error in module: {kpi_area_module}: {str(e)}")
        return {
            "ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)
        }

async def main_financial_analysis(data, session):
    """
    Main function to coordinate risk analysis conditionally

    Args:
        data (dict): Input data containing entity and session information
        session (str): Database session

    Returns:
        dict: Analysis results
    """
    logger.info("Running Conditional Financial Analysis")

    try:
        financial_ratios_result = await financial_ratios_analysis(data, session)

        # Check if any BKR KPI is present and if its flag is false
        if (financial_ratios_result.get('kpis') and
            all(kpi.get('kpi_flag', False) == False for kpi in financial_ratios_result['kpis'] if kpi.get('kpi_area') == 'BKR')):

            logger.info("All BKR KPI flags are False or KPIs not present - Running Financial Analysis")
            financials_result = await financials_analysis(data, session)

            # Retrieve updated KPIs
            ens_id_value = data.get("ens_id")
            session_id_value = data.get("session_id")
            required_column = ["all"]
            updated_kpis = await get_dynamic_ens_data("fstb", required_column, ens_id_value, session_id_value, session)

            logger.debug(f"Financials Analysis Result: {financials_result}")

            logger.info("Financial Main completed")
            return {
                "financial_ratios_analysis": financial_ratios_result,
                "financials_analysis": financials_result,
                "inserted_kpis": updated_kpis
            }


        else:
            logger.warning("Some BKR KPI flags are True - Skipping Financial Analysis")
            return {
                "financial_ratios_analysis": financial_ratios_result,
                "message": "Skipping financials due to BKR KPIs having True flags"
            }

    except Exception as e:
        logger.error(f"[ERROR] Error in main financial analysis: {str(e)}")
        return {
            "status": "failure", "error": str(e)
        }

async def financials_analysis(data, session):
    logger.info("Performing FINANCIALS Analysis... Started")

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

        def format_num(value_str):
            try:
                value = float(value_str)
                value = value * 1000
                if value >= 1_000_000_000:
                    return f"{value / 1_000_000_000:.0f}B"
                elif value >= 1_000_000:
                    return f"{value / 1_000_000:.0f}M"
                else:
                    return f"{value:,.0f}"
            except:
                return value_str

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
            logger.info(f"{kpi_area_module} Analysis... Completed With No Data")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        fin_kpis = []
        ratios = ['roce_before_tax', 'roe_before_tax', 'roe_using_net_income', 'profit_margin', 'current_ratio', 'solvency_ratio']
        # Dynamically generate KPI entries
        for idx, col in enumerate(required_columns, start=1):
            metric_data = retrieved_data.get(col)
            if metric_data and len(metric_data) > 0:
                kpi_entry = kpi_template.copy()
                kpi_entry["kpi_code"] = f"FIN{idx}A"
                kpi_entry["kpi_rating"] = "INFO"
                kpi_entry["kpi_definition"] = f"{col.replace('_', ' ').title()} - Recent Years (%)" if col.title().lower() in ratios else f"{col.replace('_', ' ').title()} - Recent Years (USD)"
                kpi_entry["kpi_value"] = json.dumps(metric_data)

                formatted_metric_data = []
                for val in metric_data:
                    if col.title().lower() in ratios:
                        formatted_metric_data.append({
                            "closing_date": val.get("closing_date"),
                            "value": val.get("value")
                        })
                    else:
                        formatted_value = format_num(val.get("value"))
                        formatted_metric_data.append({
                            "closing_date": val.get("closing_date"),
                            "value": formatted_value
                        })

                kpi_entry["kpi_value"] = json.dumps(formatted_metric_data)
                details = "".join(f"[{val['closing_date']}]: {val['value']}\n" for val in formatted_metric_data)
                kpi_entry["kpi_details"] = details

                fin_kpis.append(kpi_entry)
        # Insert KPI data into database
        insert_status = await upsert_kpi("fstb", fin_kpis, ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            logger.info(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            logger.error(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": "database_saving_error"}

    except Exception as e:
        logger.error(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}

async def default_events_analysis(data, session):
    logger.info("Performing DEFAULT EVENTS Analysis... Started")

    kpi_area_module = "BKR"
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    try:
        kpi_template = {
            "kpi_area": kpi_area_module,
            "kpi_code": "",
            "kpi_definition": "",
            "kpi_flag": True,
            "kpi_value": None,
            "kpi_rating": "HIGH",
            "kpi_details": ""
        }

        required_columns = ["default_events"]
        retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id_value, session_id_value, session)
        retrieved_data = retrieved_data[0]

        default_events = retrieved_data.get("default_events")
        if not default_events:
            logger.info(f"{kpi_area_module} Analysis... Completed With No Data")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_data"}

        relevant_events = [event for event in default_events if "Default" in event.get("LEGAL_EVENTS_TYPES_VALUE", [])][:5]

        if not relevant_events:
            logger.info(f"{kpi_area_module} Analysis... Completed With No Relevant Data")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "no_relevant_data"}

        kpi_entry = kpi_template.copy()
        kpi_entry["kpi_code"] = "BKR7A"
        kpi_entry["kpi_definition"] = "Bankruptcy Risk"
        kpi_entry["kpi_value"] = json.dumps(relevant_events)

        details = "".join(
            f"[{event['LEGAL_EVENTS_DATE']}]: {event['LEGAL_EVENTS_DESCRIPTION']}\n"
            for event in relevant_events
        )
        kpi_entry["kpi_details"] = details
        logger.info([kpi_entry])

        insert_status = await upsert_kpi("fstb", [kpi_entry], ens_id_value, session_id_value, session)

        if insert_status["status"] == "success":
            logger.info(f"{kpi_area_module} Analysis... Completed Successfully")
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "completed", "info": "analysed"}
        else:
            logger.error(insert_status)
            return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": "database_saving_error"}

    except Exception as e:
        logger.error(f"Error in module: {kpi_area_module}: {str(e)}")
        return {"ens_id": ens_id_value, "module": kpi_area_module, "status": "failure", "info": str(e)}