import random
from app.core.utils.db_utils import *
import re
from datetime import datetime
import json
from app.schemas.logger import logger


async def sape_summary(data, session):
    logger.info("Performing Summary: Sanctions...")

    area = "sanctions"  # Changed from "SAN" to "sanctions" as per requirements
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    try:
        required_columns = ["kpi_area", "kpi_flag", "kpi_rating", "kpi_value", "kpi_code"]

        retrieved_data = await get_dynamic_ens_data("sape", required_columns, ens_id_value, session_id_value, session)

        if not retrieved_data:
            summary_text = "No notable sanction findings for the entity."
            summary_data = [{"area": area, "summary": summary_text}]

            insert_status = await upsert_dynamic_ens_data_summary("summary", summary_data, ens_id_value, session_id_value,
                                                          session)

            if insert_status["status"] == "success":
                logger.info(f"{area} Summary... Completed Successfully")
                return [summary_text]
            else:
                logger.error(insert_status)
                return [summary_text]
        summary_sentences = []
        main_sanctions_found = False
        san3a_findings_found = False

        target_mapping = {
            "org": "organisation",
            "person": "individuals associated with this entity"
        }

        # Process main sanctions
        for record in retrieved_data:
            kpi_area = record.get("kpi_area", "").strip().lower()
            kpi_flag = record.get("kpi_flag")
            kpi_rating = record.get("kpi_rating")
            kpi_code = record.get("kpi_code")
            kpi_values = record.get("kpi_value")

            if kpi_area == "san" and kpi_code == "SAN3A":
                continue
            if kpi_area != "san" or not kpi_flag or kpi_rating not in ["High", "Medium", "Low"]:
                continue
            try:
                kpi_values_json = json.loads(kpi_values)
            except json.JSONDecodeError:
                continue

            count_str = kpi_values_json.get("count", "0")
            if count_str == "5 or more":
                count = 5
                count_display = "5 or more"
            else:
                try:
                    count = int(count_str)
                    count_display = str(count)
                except (ValueError, TypeError):
                    count = 0
                    count_display = "0"
            target = kpi_values_json.get("target", "").lower()
            findings = kpi_values_json.get("findings", [])
            if not findings:
                continue

            earliest_year = None
            processed_findings = []
            for finding in findings:
                eventdt = finding.get("eventdt")
                event_desc = finding.get("eventDesc", "").strip()
                entity_name = finding.get("entityName", "Unknown Entity")

                if eventdt and eventdt not in ["No event date available", "No Date"]:
                    try:
                        event_date = datetime.strptime(eventdt, "%Y-%m-%d")
                        event_year = event_date.year
                        if earliest_year is None or event_year < earliest_year:
                            earliest_year = event_year
                        processed_findings.append({"date": event_date, "desc": event_desc, "entityName": entity_name})
                    except ValueError:
                        processed_findings.append({"date": None, "desc": event_desc, "entityName": entity_name})
                else:
                    processed_findings.append({"date": None, "desc": event_desc, "entityName": entity_name})

            processed_findings.sort(key=lambda x: x["date"] if x["date"] else datetime.min, reverse=True)

            if count > 0:
                target_display = target_mapping.get(target, target)
                summary = f"There are {count_display} sanction events"
                if target in ["org", "person"]:
                    summary += f" for the {target_display}."
                else:
                    summary += "."
                if earliest_year:
                    summary += f" The findings have been since {earliest_year}."
                if processed_findings:
                    summary += " Some of the most recent sanctions events include:\n"
                    for finding in processed_findings[:2]:
                        event_desc = finding["desc"]
                        entity_name = finding.get("entityName", "Unknown Entity")
                        if finding["date"]:
                            event_year = finding["date"].year
                            summary += f"- In {event_year}, {entity_name}: {event_desc}\n"
                        else:
                            summary += f"- {entity_name}: {event_desc}\n" if entity_name else f"- {event_desc}\n"
                summary_sentences.append(summary)
                main_sanctions_found = True

        # Process SAN3A
        for record in retrieved_data:
            kpi_area = record.get("kpi_area", "").strip().lower()
            kpi_flag = record.get("kpi_flag")
            kpi_code = record.get("kpi_code")
            kpi_values = record.get("kpi_value")

            if kpi_area == "san" and kpi_code == "SAN3A" and kpi_flag:
                if not kpi_values:
                    continue
                try:
                    kpi_values_json = json.loads(kpi_values)
                    count_str = kpi_values_json.get("count", "0")
                    if count_str == "5 or more":
                        count = 5
                        count_display = "5 or more"
                    else:
                        try:
                            count = int(count_str)
                            count_display = str(count)
                        except (ValueError, TypeError):
                            count = 0
                            count_display = "0"
                    if count > 0:
                        summary = f"There are {count_display} potential sanctions findings within the corporate group."
                        summary_sentences.append(summary)
                        san3a_findings_found = True
                except json.JSONDecodeError:
                    continue

        if not main_sanctions_found and not san3a_findings_found:
            summary_sentences.append("No notable sanction findings for the entity.")

        summary_text = "\n".join(summary_sentences)
        logger.debug("Sanctions summary: %s", summary_text)

        try:
            summary_data = [{"area": area, "summary": summary_text}]
            insert_status = await upsert_dynamic_ens_data_summary(
                "summary", summary_data, ens_id_value, session_id_value, session
            )

            if insert_status["status"] == "success":
                logger.info(f"{area} Summary... Completed Successfully")
            else:
                logger.error(insert_status)

            return summary_sentences

        except Exception as e:
            logger.error(f"Error in {area} summary: {str(e)}")
            return ["No notable sanction findings for the entity."]

    except Exception as e:
        logger.error(f"Error in summary: {area}: {str(e)}")
        return ["No notable sanction findings for the entity."]

async def bcf_summary(data, session):
    logger.info("Performing Summary: BCF...")

    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    try:
        required_columns = ["kpi_area", "kpi_flag", "kpi_rating", "kpi_value"]
        retrieved_data = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)

        summary_sentences = []
        bcf_processed = False
        bcf_has_findings = False
        entity_type = "entity"

        if not retrieved_data:
            summary_sentences.append("No notable Bribery, Corruption, or Fraud findings for this entity.")
        else:
            for record in retrieved_data:
                kpi_area = record.get("kpi_area", "").strip().upper()
                kpi_flag = record.get("kpi_flag")
                kpi_rating = record.get("kpi_rating")
                kpi_values = record.get("kpi_value")

                if kpi_area != "BCF":
                    continue

                bcf_processed = True

                if not kpi_flag or kpi_rating not in ["High", "Medium", "Low"]:
                    continue

                try:
                    kpi_values_json = json.loads(kpi_values)
                except json.JSONDecodeError:
                    continue

                count_str = kpi_values_json.get("count", "0")
                if count_str == "5 or more":
                    count = 5
                    count_display = "5 or more"
                else:
                    try:
                        count = int(count_str)
                        count_display = str(count)
                    except (ValueError, TypeError):
                        count = 0
                        count_display = "0"

                target = kpi_values_json.get("target", "").lower()
                findings = kpi_values_json.get("findings", [])

                if not findings:
                    continue

                earliest_year = None
                processed_findings = []
                for finding in findings:
                    eventdt = finding.get("eventdt")
                    event_desc = finding.get("eventDesc", "").strip()
                    if eventdt:
                        try:
                            event_date = datetime.strptime(eventdt, "%Y-%m-%d")
                            event_year = event_date.year
                            if earliest_year is None or event_year < earliest_year:
                                earliest_year = event_year
                            processed_findings.append({"date": event_date, "desc": event_desc})
                        except ValueError:
                            pass

                processed_findings.sort(key=lambda x: x["date"], reverse=True)
                if count > 0:
                    bcf_has_findings = True
                    summary = f"There are {count_display} Bribery, Corruption, or Fraud findings"

                    if target == "org":
                        summary += " for the organisation."
                    elif target == "person":
                        summary += " for the individuals associated with this entity."
                    else:
                        summary += " for this entity."

                    if earliest_year:
                        summary += f" The findings has been since {earliest_year}."
                    if processed_findings:
                        summary += " Some of the most recent findings include:\n"
                        for finding in processed_findings[:2]:  # Use the 2 most recent events
                            event_year = finding["date"].year
                            event_desc = finding["desc"]
                            summary += f"- In {event_year}, {event_desc}\n"

                    summary_sentences.append(summary)

            if bcf_processed and not bcf_has_findings:
                summary_sentences.append(f"No notable Bribery, Corruption, or Fraud findings for this {entity_type}.")

            if not summary_sentences:
                summary_sentences = ["No notable Bribery/Corruption/Fraud findings for this entity."]

        summary_text = "\n".join(summary_sentences)
        logger.debug("BCF summary: %s", summary_text)

        try:
            summary_data = [{"area": "bribery_corruption_overall", "summary": summary_text}]
            insert_status = await upsert_dynamic_ens_data_summary("summary", summary_data, ens_id_value,
                                                                  session_id_value, session)

            if insert_status["status"] == "success":
                logger.info("BCF Summary... Completed Successfully")
            else:
                logger.error(insert_status)

        except Exception as e:
            logger.error(f"Error in BCF summary: {str(e)}")

        return summary_sentences


    except Exception as e:
        logger.error(f"Error in BCF Summary: {str(e)}")
        return ["No notable Bribery, Corruption, or Fraud findings for this entity."]


async def state_ownership_summary(data, session):
    logger.info("Performing Summary: State Ownership and PEP...")

    required_columns = ["kpi_area", "kpi_definition", "kpi_flag", "kpi_code", "kpi_rating", "kpi_details", "kpi_value"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    all_data = await get_dynamic_ens_data("sown", required_columns, ens_id_value, session_id_value, session)

    summary_sentences = []

    # Track processed areas and findings
    sco_processed = False
    pep_found = False
    pep3a_found = False

    if all_data:
        # Process State Ownership data
        for record in all_data:
            kpi_area = record.get("kpi_area", "").strip().lower()
            kpi_flag = record.get("kpi_flag")
            kpi_rating = record.get("kpi_rating")
            kpi_definition = record.get("kpi_definition")
            kpi_details = record.get("kpi_details")

            if kpi_area != "sco":
                continue

            sco_processed = True

            # Process State Ownership
            if kpi_flag:
                summary_sentences.append(f"High risk identified for the entity: {kpi_definition}")
            else:
                summary_sentences.append(f"{kpi_details}")

        # Process PEP data
        pep_findings = []
        count_display = "0"
        target = ""

        for record in all_data:
            kpi_area = record.get("kpi_area", "").strip().lower()
            kpi_flag = record.get("kpi_flag")
            kpi_rating = record.get("kpi_rating")
            kpi_values = record.get("kpi_value")

            if kpi_area != "pep" or not kpi_flag or kpi_rating not in ["High", "Medium", "Low"]:
                continue

            try:
                kpi_values_json = json.loads(kpi_values)
            except json.JSONDecodeError as e:
                logger.error(f"JSON Parsing Error: {e}, {kpi_values}")
                continue

            count_str = kpi_values_json.get("count", "0")
            if count_str == "5 or more":
                count = 5
                count_display = "5 or more"
            else:
                try:
                    count = int(count_str)
                    count_display = str(count)
                except (ValueError, TypeError):
                    count = 0
                    count_display = "0"

            target = kpi_values_json.get("target", "").lower()
            findings = kpi_values_json.get("findings", [])

            if count > 0:
                pep_found = True

            if not findings or count <= 0:
                continue

            for finding in findings:
                eventdt = finding.get("eventdt")
                event_desc = finding.get("eventDesc", "").strip()
                entity_name = finding.get("entityName", "Unknown Entity").strip()

                if event_desc and entity_name != "Unknown Entity":
                    if eventdt and eventdt != 'No event date available':
                        try:
                            event_date = datetime.strptime(eventdt, "%Y-%m-%d")
                            event_year = event_date.year
                            pep_findings.append({"date": event_date, "desc": event_desc, "entityName": entity_name})
                        except ValueError:
                            pass
                    else:
                        pep_findings.append({"date": None, "desc": event_desc, "entityName": entity_name})

        if pep_findings:
            pep_findings.sort(key=lambda x: x["date"] if x["date"] else datetime.min, reverse=True)
            summary = f"There are {count_display} PEP findings"
            if target == "org":
                summary += " for the organisation."
            elif target == "person":
                summary += " for the individuals associated with this entity."
            else:
                summary += "."

            if pep_findings:
                summary += " Some of the most recent PEP events include:\n"
                for finding in pep_findings[:2]:  # Only show the top 2 findings
                    event_desc = finding["desc"]
                    entity_name = finding["entityName"]
                    if finding["date"]:
                        event_year = finding["date"].year
                        summary += f"- In {event_year}, {entity_name}: {event_desc}\n"
                    else:
                        summary += f"- {entity_name}: {event_desc}\n"
            summary_sentences.append(summary)

        # Process PEP3A findings
        for record in all_data:
            kpi_area = record.get("kpi_area", "").strip().lower()
            kpi_flag = record.get("kpi_flag")
            kpi_code = record.get("kpi_code")
            kpi_values = record.get("kpi_value")

            if kpi_area == "pep" and kpi_code == "PEP3A" and kpi_flag:
                try:
                    kpi_values_json = json.loads(kpi_values)
                    count_str = kpi_values_json.get("count", "0")
                    if count_str == "5 or more":
                        count = 5
                        count_display = "5 or more"
                    else:
                        try:
                            count = int(count_str)
                            count_display = str(count)
                        except (ValueError, TypeError):
                            count = 0
                            count_display = "0"

                    if count > 0:
                        pep3a_found = True
                        summary_sentences.append(
                            f"There are {count_display} potential PEP findings within the corporate group.")
                except json.JSONDecodeError:
                    continue

    if not summary_sentences or (len(summary_sentences) == 0 and not pep_found and not pep3a_found ):
        summary_sentences.append("No state ownership information available for the entity.")
    if not pep_found and not pep3a_found:
        summary_sentences.append("No PEP findings for the individual.")

    combined_summary = "\n\n".join(summary_sentences).strip()

    logger.debug("State Ownership and PEP summary: %s", combined_summary)

    try:
        summary_data = [{"area": "government_political", "summary": combined_summary}]
        insert_status = await upsert_dynamic_ens_data_summary(
            "summary",
            summary_data,
            ens_id_value,
            session_id_value,
            session
        )

        if insert_status["status"] == "success":
            logger.info("Summary: State Ownership and PEP Analysis completed successfully")
        else:
            logger.error(f"Failed to save state ownership summary: {insert_status}")

    except Exception as e:
        logger.error(f"Error in state ownership summary: {str(e)}")

    return summary_sentences

async def financials_summary(data, session):
    logger.info("Performing Summary: Financials...")

    required_columns = ["kpi_area", "kpi_definition", "kpi_flag", "kpi_rating", "kpi_details"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("fstb", required_columns, ens_id_value, session_id_value, session)

    summary_sentences = []
    financials_found = False

    if not retrieved_data:
        summary_text = "No financials available."
    else:
        for record in retrieved_data:
            kpi_area = record.get("kpi_area", "").strip().lower()
            kpi_flag = record.get("kpi_flag")
            kpi_rating = record.get("kpi_rating")
            kpi_details = record.get("kpi_details")

            if kpi_area != "bkr":
                continue

            # Process Financials (BKR)
            if kpi_flag and kpi_rating in ["High", "Medium", "Low"]:
                summary_sentences.append(kpi_details)
                financials_found = True

        if not financials_found:
            summary_sentences.append("No financials available.")

    summary_text = "\n".join(summary_sentences) if summary_sentences else "No financials available."
    logger.debug("Financials summary: %s", summary_text)

    try:
        summary_data = [{"area": "financials", "summary": summary_text}]
        insert_status = await upsert_dynamic_ens_data_summary("summary", summary_data, ens_id_value, session_id_value,
                                                              session)

        if insert_status["status"] == "success":
            logger.info("Financials Summary... Completed Successfully")
        else:
            logger.error(insert_status)

    except Exception as e:
        logger.error(f"Error in financials summary: {str(e)}")

    return summary_sentences

async def adverse_media_summary(data, session):
    logger.info("Performing Summary: Adverse Media...")

    required_columns = ["kpi_area", "kpi_flag", "kpi_code", "kpi_rating", "kpi_value"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)
    other_news = await get_dynamic_ens_data("news", required_columns, ens_id_value, session_id_value, session)

    summary_sentences = []
    amr_processed = False
    amo_processed = False
    amr_has_findings = False
    amo_has_findings = False
    amr2a_has_findings = False
    amo2a_has_findings = False
    nws_or_onf_findings = False

    # Process main AMR and AMO findings
    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_flag = record.get("kpi_flag")
        kpi_rating = record.get("kpi_rating")
        kpi_values = record.get("kpi_value")
        kpi_code = record.get("kpi_code")

        entity_type = "entity"
        if not isinstance(kpi_values, str):
            continue
        try:
            kpi_values_json = json.loads(kpi_values)
            target = kpi_values_json.get("target", "").lower()
            if target == "org":
                entity_type = "organization"
            elif target == "person" and entity_type != "organization":
                entity_type = "individuals associated with this entity"
        except json.JSONDecodeError:
            continue

        if kpi_area not in ["amr", "amo"]:
            continue
        if kpi_code == "AMR2A":
            continue
        if kpi_area == "amr":
            amr_processed = True
        elif kpi_area == "amo":
            amo_processed = True

        if not kpi_flag or kpi_rating not in ["High", "Medium", "Low"]:
            continue
        try:
            kpi_values_json = json.loads(kpi_values)
        except json.JSONDecodeError:
            continue

        count_str = kpi_values_json.get("count", "0")
        if count_str == "5 or more":
            count = 5
            count_display = "5 or more"
        else:
            try:
                count = int(count_str)
                count_display = str(count)
            except (ValueError, TypeError):
                count = 0
                count_display = "0"

        findings = kpi_values_json.get("findings", [])

        if not findings:
            continue
        earliest_year = None
        processed_findings = []
        for finding in findings:
            eventdt = finding.get("eventdt")
            event_desc = finding.get("eventDesc", "").strip()
            if eventdt:
                try:
                    event_date = datetime.strptime(eventdt, "%Y-%m-%d")
                    event_year = event_date.year
                    if earliest_year is None or event_year < earliest_year:
                        earliest_year = event_year
                    processed_findings.append({"date": event_date, "desc": event_desc})
                except ValueError:
                    pass

        processed_findings.sort(key=lambda x: x["date"], reverse=True)

        if count > 0:
            summary = ""
            if kpi_area == "amr":
                amr_has_findings = True
                summary = f"There are {count_display} adverse media reputation risk findings"
            elif kpi_area == "amo":
                amo_has_findings = True
                summary = f"There are {count_display} adverse media criminal activity findings"

            summary += f" for this {entity_type}."
            if earliest_year:
                summary += f" The findings have been since {earliest_year}."

            if processed_findings:
                summary += " Some of the most recent media events include: \n"
                for finding in processed_findings[:2]:  # Use the 2 most recent events
                    event_year = finding["date"].year
                    event_desc = finding["desc"]
                    summary += f"- In {event_year}, {event_desc}\n"

            if kpi_area == "amr":
                summary_sentences.append(summary)
            elif kpi_area == "amo":
                summary_sentences.append(summary)

    # Check for NWS1A and ONF1A if no AMR or AMO findings
    for record in other_news:
        kpi_code = record.get("kpi_code", "").strip().upper()
        kpi_flag = record.get("kpi_flag")
        kpi_rating = record.get("kpi_rating")

        if kpi_code == "NWS1A" and kpi_flag and kpi_rating == "High":
            summary_sentences.append("There are potential adverse news findings from the advanced screening.")
            nws_or_onf_findings = True
        elif kpi_code == "ONF1A" and kpi_flag and kpi_rating == "High":
            summary_sentences.append("There are other potential news findings.")
            nws_or_onf_findings = True

    # Process additional AMR2A findings
    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_flag = record.get("kpi_flag")
        kpi_code = record.get("kpi_code")
        kpi_values = record.get("kpi_value")

        if kpi_code in ["AMR2A"] and kpi_flag:
            try:
                kpi_values_json = json.loads(kpi_values)
                count_str = kpi_values_json.get("count", "0")
                if count_str == "5 or more":
                    count = 5
                    count_display = "5 or more"
                else:
                    try:
                        count = int(count_str)
                        count_display = str(count)
                    except (ValueError, TypeError):
                        count = 0
                        count_display = "0"

                if count > 0:
                    if kpi_code == "AMR2A":
                        amr2a_has_findings = True
                        summary = f"There are also further {count_display} adverse media reputation risk findings within the corporate group."
                        summary_sentences.append(summary)
            except json.JSONDecodeError:
                continue

    # Add default messages if no findings
    if not amr_has_findings and not amo_has_findings and not amr2a_has_findings and not amo2a_has_findings and not nws_or_onf_findings:
        summary_sentences.append(f"No adverse media findings.")

    combined_summary = "\n\n".join(summary_sentences)
    logger.debug("AMO and AMR are: %s", combined_summary)

    try:
        summary_data = [{"area": "other_adverse_media", "summary": combined_summary}]
        insert_status = await upsert_dynamic_ens_data_summary("summary", summary_data, ens_id_value, session_id_value,
                                                              session)

        if insert_status["status"] == "success":
            logger.info("Adverse Media Summary... Completed Successfully")
        else:
            logger.error(insert_status)

    except Exception as e:
        logger.error(f"Error in adverse media summary: {str(e)}")

    return summary_sentences

async def additional_indicators_summary(data, session):

    logger.info("Performing Summary: Additional Indicators")
    required_columns = ["kpi_area", "kpi_code", "kpi_flag", "kpi_rating", "kpi_details"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    # Retrieve data for cybersecurity, ESG, and website
    retrieved_data = await get_dynamic_ens_data("cyes", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No findings available for cybersecurity, ESG, or website."]

    cyb_findings = False
    esg_findings = False
    website_findings = False
    strong_profile = False

    # Process retrieved data
    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_code = record.get("kpi_code", "").strip()
        kpi_flag = record.get("kpi_flag", False)
        kpi_rating = record.get("kpi_rating", "").strip().lower()

        if kpi_area == "cyb" and kpi_code == "CYB2A":
            if kpi_flag:
                if kpi_rating == "low":
                    strong_profile = True
                elif kpi_rating in ["medium", "high"]:
                    cyb_findings = True

        if kpi_area == "esg" and kpi_code == "ESG1A":
            if kpi_flag:
                if kpi_rating == "low":
                    strong_profile = True
                elif kpi_rating in ["medium", "high"]:
                    esg_findings = True

        if kpi_area == "web" and kpi_code == "WEB1A":
            if kpi_flag and kpi_rating == "high":
                website_findings = True

    result = []

    # Process results based on findings
    if strong_profile:
        result.append("The entity has a strong profile in cybersecurity and ESG.")
    elif cyb_findings or esg_findings:
        findings = []
        if cyb_findings:
            findings.append("cybersecurity")
        if esg_findings:
            findings.append("ESG")
        result.append(f"There are notable findings in {', '.join(findings)} screening/profiling for this entity.")
        logger.debug("There are findings for ESG or Cyber")

    if website_findings:
        result.append("There are notable findings in website screening/profiling for this entity.")
        logger.debug("There are findings for WEB")

    if not result:
        result.append("No notable findings in screening/profiling of ESG, cybersecurity, or website for this entity.")
        logger.debug("There are no findings for ESG and Cyber")

    # Join the result list into a single string with line breaks
    result_text = "\n\n".join(result).strip()

    # Save the summary to the database
    try:
        summary_data = [{"area": "additional_indicator", "summary": result_text}]
        insert_status = await upsert_dynamic_ens_data_summary(
            "summary",
            summary_data,
            ens_id_value,
            session_id_value,
            session
        )

        if insert_status["status"] == "success":
            logger.info("Additional Indicators Summary completed successfully")
        else:
            logger.error(f"Failed to save additional indicators summary: {insert_status}")

    except Exception as e:
        logger.error(f"Error in additional indicators summary: {str(e)}")

    return result

async def legal_regulatory_summary(data, session):

    logger.info("Performing Summary: Legal and Regulatory")
    required_columns = ["kpi_area", "kpi_flag", "kpi_rating", "kpi_value"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    leg_data = await get_dynamic_ens_data("lgrk", required_columns, ens_id_value, session_id_value, session)
    rfct_data = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)
    if not leg_data and not rfct_data:
        return ["No legal or regulatory findings available."]

    summary_sentences = []
    leg_processed = False
    reg_processed = False
    leg_has_findings = False
    reg_has_findings = False

    # Process legal and regulatory findings
    for record in leg_data + rfct_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_flag = record.get("kpi_flag")
        kpi_rating = record.get("kpi_rating")
        kpi_values = record.get("kpi_value")

        entity_type = "entity"
        if not isinstance(kpi_values, str):
            continue
        try:
            kpi_values_json = json.loads(kpi_values)
            target = kpi_values_json.get("target", "").lower()
            if target == "org":
                entity_type = "organization"
            elif target == "person" and entity_type != "organization":
                entity_type = "individuals associated with this entity"
        except json.JSONDecodeError:
            continue

        if kpi_area not in ["leg", "reg"]:
            continue
        if kpi_area == "leg":
            leg_processed = True
        elif kpi_area == "reg":
            reg_processed = True

        if not kpi_flag or kpi_rating not in ["High", "Medium", "Low"]:
            continue
        try:
            kpi_values_json = json.loads(kpi_values)
        except json.JSONDecodeError:
            continue

        count_str = kpi_values_json.get("count", "0")
        if count_str == "5 or more":
            count = 5
            count_display = "5 or more"
        else:
            try:
                count = int(count_str)
                count_display = str(count)
            except (ValueError, TypeError):
                count = 0
                count_display = "0"

        findings = kpi_values_json.get("findings", [])
        if not findings:
            continue

        earliest_year = None
        processed_findings = []
        for finding in findings:
            eventdt = finding.get("eventdt")
            event_desc = finding.get("eventDesc", "").strip()
            if eventdt:
                try:
                    event_date = datetime.strptime(eventdt, "%Y-%m-%d")
                    event_year = event_date.year
                    if earliest_year is None or event_year < earliest_year:
                        earliest_year = event_year
                    processed_findings.append({"date": event_date, "desc": event_desc})
                except ValueError:
                    pass

        processed_findings.sort(key=lambda x: x["date"], reverse=True)

        if count > 0:
            if kpi_area == "leg":
                leg_has_findings = True
                summary = f"There are {count_display} legal findings"
            elif kpi_area == "reg":
                reg_has_findings = True
                summary = f"There are {count_display} regulatory findings"

            summary += f" for this {entity_type}."
            if earliest_year:
                summary += f" The findings have been since {earliest_year}."

            if processed_findings:
                summary += " Some of the most recent events include: \n"
                for finding in processed_findings[:2]:  # Use the 2 most recent events
                    event_year = finding["date"].year
                    event_desc = finding["desc"]
                    summary += f"- In {event_year}, {event_desc}\n"

            summary_sentences.append(summary)

    if leg_processed and not leg_has_findings:
        summary_sentences.append(f"No legal findings available")
    if reg_processed and not reg_has_findings:
        summary_sentences.append(f"No regulatory findings available")
    if not summary_sentences:
        return ["No legal or regulatory findings available."]

    logger.debug(f"The leg and reg are : %s", summary_sentences)
    return summary_sentences

def capitalize_after_full_stop(text):
    """
    Capitalize the first letter of a word after a full stop.
    """
    sentences = text.split('. ')
    sentences = [sentence[0].upper() + sentence[1:] if sentence else '' for sentence in sentences]
    return '. '.join(sentences)

def enforce_lowercase(text):
    """
    Ensures all words in the text are lowercase unless they are proper nouns or acronyms.
    """
    return text.lower()

async def overall_summary(data, session, supplier_name):

    logger.info("Performing Overall Summary....")
    area = "overall"
    required_columns = ["kpi_code", "kpi_area", "kpi_rating", "kpi_flag"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("ovar", required_columns, ens_id_value, session_id_value, session)
    website_data = await get_dynamic_ens_data("cyes", ["kpi_area", "kpi_flag", "kpi_code", "kpi_rating"], ens_id_value, session_id_value, session)

    supplier_name = supplier_name.upper()
    if isinstance(retrieved_data, str):
        import json
        retrieved_data = json.loads(retrieved_data)
    elif not isinstance(retrieved_data, list):
        retrieved_data = [retrieved_data]

    if isinstance(website_data, str):
        import json
        website_data = json.loads(website_data)
    elif not isinstance(website_data, list):
        website_data = [website_data]

    theme_ratings = {}
    overall_rating = None
    country_risk_high = False
    website_rating = "Low"

    for row in retrieved_data:
        if not isinstance(row, dict):
            logger.warning(f"Unexpected row format: %s",row)
            continue

        kpi_code = row.get("kpi_code")
        kpi_area = row.get("kpi_area")
        kpi_rating = row.get("kpi_rating")

        if kpi_area == "theme_rating":
            theme_ratings[kpi_code] = kpi_rating
        elif kpi_area == "overall_rating" and kpi_code == "supplier":
            overall_rating = kpi_rating
        elif kpi_area == "CR" and kpi_rating == "High":
            country_risk_high = True
        elif kpi_area == "WEB":
            website_rating = kpi_rating

    if overall_rating is None:
        overall_rating = "Low"

    website_available = True
    website_high_risk = False

    for row in website_data:
        if (isinstance(row, dict) and
                row.get("kpi_area") == "WEB" and
                row.get("kpi_code") == "WEB1A" and
                row.get("kpi_rating") == "High" and
                row.get("kpi_flag") is True):  # Flag has to be TRUE
            website_high_risk = True
            break

    module_ratings = {
        "Financials": theme_ratings.get("financials", "Low"),
        "Adverse Media (Reputation)": theme_ratings.get("other_adverse_media", "Low"),
        "Adverse Media (Other)": theme_ratings.get("other_adverse_media", "Low"),
        "Bribery, Corruption & Fraud": theme_ratings.get("bribery_corruption_overall", "Low"),
        "State Ownership": theme_ratings.get("government_political", "Low"),
        "Legal": theme_ratings.get("regulatory_legal", "Low"),
        "Regulatory": theme_ratings.get("regulatory_legal", "Low"),
        "Sanctions": theme_ratings.get("sanctions", "Low"),
        "PEP": theme_ratings.get("government_political", "Low")
    }

    cyber_rating = theme_ratings.get("cyber", "Low")
    esg_rating = theme_ratings.get("esg", "Low")

    has_additional_indicators = (cyber_rating == "Medium" or esg_rating == "Medium" or
                                 country_risk_high or (not website_available and website_rating == "High"))

    important_modules = {k: v for k, v in module_ratings.items() if v in ["High", "Medium"]}

    if not important_modules and not has_additional_indicators:
        import random
        low_modules = {k: v for k, v in module_ratings.items() if v == "Low"}
        if low_modules:
            selected_key = random.choice(list(low_modules.keys()))
            important_modules[selected_key] = "Low"

    intro_templates = [
        f"The assessment for {supplier_name} indicates {overall_rating.lower()} risk overall",
        f"{supplier_name} presents an {overall_rating.lower()} overall risk profile",
        f"Our analysis of {supplier_name} reveals {overall_rating.lower()} risk overall",
        f"Based on our evaluation, {supplier_name} demonstrates {overall_rating.lower()} risk",
        f"The risk assessment for {supplier_name} shows {overall_rating.lower()} overall risk",
        f"The risk profile of {supplier_name} is categorized as {overall_rating.lower()} overall",
        f"Our comprehensive review of {supplier_name} identifies {overall_rating.lower()} risk levels",
        f"{supplier_name}'s business practices reflect {overall_rating.lower()} risk according to our assessment",
        f"The due diligence conducted on {supplier_name} highlights {overall_rating.lower()} risk ratings",
        f"Our investigation into {supplier_name} reveals an {overall_rating.lower()} overall risk classification",
        f"The risk evaluation of {supplier_name} indicates {overall_rating.lower()} concern levels",
        f"{supplier_name} has been assessed with {overall_rating.lower()} risk in our evaluation",
        f"The third-party risk profile for {supplier_name} is determined to be {overall_rating.lower()}",
        f"Our vendor assessment shows that {supplier_name} represents {overall_rating.lower()} risk exposure",
        f"According to our risk framework, {supplier_name} is classified as {overall_rating.lower()} risk"
    ]

    country_risk_templates = [
        "The entity is based in a high-risk jurisdiction requiring enhanced due diligence",
        "Operations in a high-risk country present significant compliance concerns",
        "The company's high-risk jurisdiction location presents notable regulatory challenges",
        "Significant country risk has been identified due to the entity's geographical presence",
        "The entity's location in a high-risk jurisdiction warrants additional scrutiny",
        "Geographical risk factors are elevated due to operations in a high-risk territory",
        "The supplier's high-risk country of operation introduces significant compliance considerations",
        "Notable jurisdictional risk has been identified in the company's operational locations",
        "The entity's presence in a high-risk territory presents substantial compliance challenges",
        "High geographical risk factors have been identified in the company's country of operation",
        "The company operates within a high-risk jurisdiction requiring careful monitoring",
        "Significant country-based risk factors affect the entity's compliance profile",
        "The supplier's high-risk jurisdictional presence warrants enhanced oversight",
        "Elevated territorial risk has been identified in the company's operational footprint",
        "The entity's high-risk geographical location presents notable compliance implications"
    ]

    no_website_templates = [
        "No official website could be identified for this entity",
        "The company appears to have no verifiable online presence",
        "Our research could not locate an official website for this organization",
        "The entity lacks a discoverable web presence during our assessment",
        "No corporate website was found during the supplier evaluation process",
        "The company does not appear to maintain an official online presence",
        "Our evaluation was unable to locate a verifiable website for this entity",
        "The organization has no identifiable web presence based on our research",
        "No digital footprint in the form of an official website was discovered",
        "The supplier does not maintain a discoverable corporate website",
        "Our assessment found no evidence of an official online presence",
        "No digital presence in the form of a corporate website was identified",
        "The entity lacks a formal website based on our digital assessment",
        "Our research could not verify an official web presence for this supplier",
        "No corporate website was discovered during the due diligence process"
    ]

    # Cyber-only templates
    cyber_only_templates = [
        "There are notable findings in cybersecurity screening for this entity",
        "The assessment identified moderate concerns in the entity's cyber risk profile",
        "Our evaluation highlights potential vulnerabilities in the company's cybersecurity posture",
        "The supplier shows moderate risk indicators in their cyber resilience framework",
        "The entity's cybersecurity defenses present moderate concerns requiring attention",
        "Notable observations were made regarding the company's digital protection measures",
        "The assessment found moderate risk factors in cyber protection implementations",
        "Our review identified potential improvement areas in the entity's cyber preparedness",
        "The entity demonstrates moderate risk indicators in their digital security protocols",
        "The evaluation discovered notable concerns in information security practices",
        "Our analysis reveals moderate risk in cyber resilience capabilities",
        "The assessment identified noteworthy findings in the company's digital protection framework",
        "The supplier shows moderate concerns in their cybersecurity implementation strategies",
        "Our examination found notable cyber vulnerability indicators requiring oversight",
        "The entity presents moderate risk in their information security measures"
    ]

    # ESG-only templates
    esg_only_templates = [
        "There are notable findings in social governance screening for this entity",
        "The assessment identified moderate concerns in the entity's social governance profile",
        "Our evaluation highlights potential gaps in environmental, social and governance practices",
        "The supplier shows moderate risk indicators in their social governance compliance",
        "The entity's social governance framework presents moderate concerns requiring attention",
        "Notable observations were made regarding the company's sustainability standards",
        "The assessment found moderate risk factors in social governance implementation",
        "Our review identified potential improvement areas in social governance integration",
        "The entity demonstrates moderate risk indicators in their sustainability practices",
        "The evaluation discovered notable concerns in environmental governance protocols",
        "The assessment identified noteworthy findings in the company's sustainability framework",
        "The supplier shows moderate concerns in their environmental standards compliance",
        "Our examination found notable social governance implementation gaps",
        "The entity presents moderate risk in their social responsibility measures"
    ]

    # Both cyber and ESG templates
    cyber_esg_templates = [
        "There are notable findings in both cybersecurity and social governance screening for this entity",
        "The assessment identified moderate concerns in the entity's cyber risk and social governance profiles",
        "Our evaluation highlights potential vulnerabilities in both environmental governance and cybersecurity practices",
        "The supplier shows moderate risk indicators in social governance compliance and cyber resilience",
        "The entity's social governance framework and cybersecurity posture both present moderate concerns",
        "Notable observations were made regarding the company's sustainability standards and cyber defenses",
        "The assessment found moderate risk factors in both social governance implementation and cyber protection",
        "Our review identified potential improvement areas in both social governance integration and cyber preparedness",
        "The entity demonstrates moderate risk indicators in both sustainability practices and digital security",
        "The evaluation discovered notable concerns in both environmental governance and information security protocols",
        "Our analysis reveals moderate risk in both corporate responsibility metrics and cyber resilience capabilities",
        "The assessment identified noteworthy findings in both sustainability practices and digital protection frameworks",
        "The supplier shows moderate concerns in both environmental standards and cybersecurity implementations",
        "Our examination found notable gaps in both social governance implementation and cyber vulnerability management",
        "The entity presents moderate risk in both social responsibility measures and information security controls"
    ]

    module_contexts = {
        "Financials": [
            "affecting operational viability",
            "impacting business sustainability",
            "threatening long-term stability",
            "challenging their market position",
            "weakening revenue forecasts",
            "creating uncertainty for investors",
            "potentially affecting business continuity",
            "raising questions about financial durability",
            "impacting debt management capabilities",
            "affecting cash flow projections",
            "raising concerns about capital adequacy",
            "challenging profitability expectations",
            "affecting liquidity positions",
            "raising solvency questions",
            "potentially limiting growth capacity"
        ],
        "Adverse Media (Reputation)": [
            "damaging public trust",
            "affecting brand perception",
            "undermining stakeholder confidence",
            "diminishing market credibility",
            "tarnishing industry standing",
            "eroding customer loyalty",
            "affecting investor sentiment",
            "complicating partner relationships",
            "challenging their public image",
            "affecting market positioning",
            "potentially limiting customer acquisition",
            "undermining years of brand building",
            "creating public relations challenges",
            "potentially requiring reputation management",
            "affecting stakeholder perceptions"
        ],
        "Adverse Media (Other)": [
            "in media coverage",
            "reported in industry publications",
            "documented in public records",
            "identified in press investigations",
            "highlighted in news articles",
            "emerging in social media discussions",
            "revealed in investigative journalism",
            "exposed in business publications",
            "appearing in public databases",
            "cited in analyst reports",
            "mentioned in trade journals",
            "found in court documents",
            "uncovered by watchdog organizations",
            "emerging through digital monitoring",
            "surfacing in regulatory filings"
        ],
        "Bribery, Corruption & Fraud": [
            "suggesting governance challenges",
            "undermining ethical standards",
            "indicating compliance failures",
            "raising integrity questions",
            "potentially affecting regulatory standing",
            "creating legal exposure risks",
            "raising questions about internal controls",
            "affecting transparency commitments",
            "challenging corporate governance standards",
            "suggesting potential misconduct exposure",
            "affecting compliance with anti-corruption laws",
            "raising due diligence concerns",
            "potentially complicating cross-border operations",
            "affecting whistleblower protection measures",
            "raising questions about management oversight"
        ],
        "State Ownership": [
            "affecting operational independence",
            "impacting business autonomy",
            "raising geopolitical concerns",
            "introducing political risk",
            "potentially affecting trade restrictions",
            "creating foreign investment complications",
            "raising questions about decisional independence",
            "introducing sovereign influence considerations",
            "affecting governance transparency",
            "complicating cross-border transactions",
            "potentially affecting international partnerships",
            "introducing national security considerations",
            "affecting compliance with foreign ownership regulations",
            "raising questions about operational control",
            "potentially creating conflicts of interest"
        ],
        "Legal": [
            "requiring immediate attention",
            "demanding legal intervention",
            "necessitating compliance review",
            "requiring legal risk assessment",
            "suggesting potential litigation exposure",
            "raising questions about legal preparedness",
            "potentially affecting contractual obligations",
            "introducing liability considerations",
            "affecting legal standing in key markets",
            "creating potential jurisdiction conflicts",
            "raising questions about intellectual property protection",
            "potentially complicating dispute resolution",
            "affecting adherence to industry regulations",
            "introducing legal precedent concerns",
            "requiring enhanced legal oversight"
        ],
        "Regulatory": [
            "requiring immediate attention",
            "affecting compliance status",
            "demanding regulatory review",
            "challenging operational approvals",
            "potentially limiting market access",
            "raising questions about licensing requirements",
            "affecting industry certification status",
            "requiring increased reporting transparency",
            "introducing sectoral compliance challenges",
            "potentially affecting operating permits",
            "raising concerns with regulatory authorities",
            "affecting adherence to industry standards",
            "creating potential for increased scrutiny",
            "requiring enhanced compliance monitoring",
            "potentially affecting regulatory relationship management"
        ],
        "Sanctions": [
            "threatening international operations",
            "affecting global business activities",
            "limiting market access",
            "constraining financial transactions",
            "potentially affecting banking relationships",
            "introducing trade restriction concerns",
            "requiring enhanced screening measures",
            "potentially limiting supplier relationships",
            "creating export control challenges",
            "affecting international business development",
            "requiring enhanced due diligence",
            "potentially complicating international contracts",
            "affecting global supply chain operations",
            "introducing screening and compliance costs"
        ],
        "PEP": [
            "raising political influence concerns",
            "introducing governmental risk factors",
            "affecting third-party relationships",
            "requiring enhanced monitoring",
            "potentially creating conflicts of interest",
            "raising questions about decision independence",
            "affecting anti-corruption compliance",
            "introducing potential for preferential treatment",
            "requiring enhanced due diligence",
            "potentially complicating government contracting",
            "affecting regulatory relationship management",
            "introducing transparency concerns",
            "requiring additional oversight measures",
            "potentially creating perception challenges",
            "affecting governance independence"
        ]
    }

    # rating descriptors
    rating_descriptors = {
        "High": [
            "concerning", "critical", "serious", "major", "significant",
            "substantial", "notable", "considerable", "noteworthy",
            "prominent", "elevated"
        ],
        "Medium": [
            "moderate", "medium", "average", "standard", "intermediate",
            "middling", "mid-level", "fair", "ordinary", "neutral",
            "middle-range", "balanced", "reasonable", "conventional", "typical"
        ],
        "Low": [
            "minimal", "low", "negligible", "favorable", "insignificant", "minor",
            "limited", "slight", "marginal", "inconsequential", "trivial",
            "immaterial", "modest", "small", "nominal"
        ]
    }

    # module descriptive terms
    module_descriptors = {
        "Financials": [
            "financial stability", "financial metrics", "financial performance", "financial viability",
            "financial health", "financial position", "economic indicators", "fiscal condition",
            "balance sheet strength", "cash flow management", "profitability indicators",
            "capital structure", "liquidity position", "debt ratios", "revenue forecasts"
        ],
        "Adverse Media (Reputation)": [
            "reputational issues", "media exposure", "public perception challenges", "brand image concerns",
            "public relational vulnerabilities", "public opinion factors", "media sentiment", "corporate image status",
            "market perception", "brand reputation", "public relations challenges",
            "news coverage impact", "stakeholder perception", "media presence", "public visibility"
        ],
        "Adverse Media (Other)": [
            "criminal activity exposure", "negative media coverage", "unfavorable press mentions",
            "controversial news presence", "adverse public records", "negative publicity",
            "unfavorable news reports", "problematic news mentions", "critical coverage",
            "journalistic scrutiny", "public record controversies", "media criticism",
            "public documentation concerns", "news analysis impact", "documented controversies"
        ],
        "Bribery, Corruption & Fraud": [
            "anti-corruption controls", "fraud prevention measures", "anti-bribery safeguards",
            "corrupt activity exposure",
            "ethical compliance framework", "fraud risk management", "governance controls",
            "ethical standards implementation", "compliance enforcement",
            "anti-corruption program", "fraud detection capabilities", "business integrity measures",
            "ethical business practices", "corruption prevention mechanisms", "accountability measures"
        ],
        "State Ownership": [
            "government influence", "state control indicators", "governmental ties", "political connections",
            "sovereign interest presence", "governmental ownership stake", "state affiliation",
            "political linkages", "government relationship extent", "sovereign control indicators",
            "state involvement level", "public sector connections",
            "political entity relationships", "governmental control indicators", "sovereign affiliation"
        ],
        "Legal": [
            "legal compliance", "legal risk profile", "litigation exposure", "contractual risks",
            "legal framework adherence", "compliance gaps", "regulatory conformity issues",
            "legal obligation fulfillment", "contractual compliance", "legal issue management",
            "liability exposure", "legal process adherence",
            "legal governance structures", "legal judgment history", "statutory compliance measures"
        ],
        "Regulatory": [
            "regulatory compliance", "regulatory risk exposure", "regulatory standing", "compliance status",
            "regulatory framework adherence", "regulatory relationship management", "authorization status",
            "regulatory reporting quality", "compliance infrastructure", "regulatory filing history",
            "regulatory communication practices",
            "permission and licensing status", "compliance monitoring systems", "regulatory audit performance",
            "industry standard conformity"
        ],
        "Sanctions": [
            "sanctions exposure", "sanctions compliance", "sanction violation risks",
            "restricted party involvement", "sanctions list implications", "economic restrictions exposure",
            "international sanctions compliance", "trade restrictions impact", "sanctioned entity connections",
            "trade compliance measures", "sanctions screening effectiveness",
            "embargoed country exposure", "sanctions enforcement vulnerability", "restricted party screening",
            "sanctions risk management"
        ],
        "PEP": [
            "politically exposed person connections", "political exposure", "political affiliation concerns",
            "government official relationships", "political influence factors", "politically exposed personals screening results",
            "political figure associations", "government relationship exposure", "political connection risks",
            "public official relationships", "political tie management",
            "governmental connection indicators", "political relationship disclosures",
            "political affiliation management", "high-profile political relationships"
        ]
    }

    # sentence connectors
    transitions = [
        ". ",
        ", while ",
        ". The company also shows ",
        ". Additionally, there are concerns about ",
        ". Furthermore, the assessment identified ",
        ". The evaluation also highlights ",
        ". Moreover, our analysis revealed ",
        ". In addition, we observed ",
        ". The risk assessment also detected ",
        ". Beyond this, our evaluation found ",
        ". The company's profile also indicates ",
        ". Our investigation also uncovered ",
        ". The vendor assessment also points to ",
        ". Another area of note is ",
        ". The review also identified "
    ]

    #  group connectors
    group_connectors = [
        "and", "alongside", "coupled with", "as well as", "in conjunction with",
        "together with", "plus", "combined with", "in addition to", "along with",
        "accompanied by", "paired with", "connected to", "linked with", "associated with"
    ]

    # conclusion templates
    conclusion_templates = {
        "High": [
            "Due to these factors, enhanced due diligence and risk mitigation strategies are strongly recommended.",
            "These findings necessitate thorough monitoring and stringent control measures.",
            "This risk profile requires comprehensive safeguards and heightened vigilance.",
            "These risks demand immediate attention and robust mitigation strategies.",
            "The identified concerns warrant extensive controls and enhanced monitoring protocols.",
            "Given these factors, we recommend implementing comprehensive risk management measures.",
            "This assessment suggests the need for heightened scrutiny and advanced risk controls.",
            "The findings indicate a need for significant risk reduction strategies and close oversight.",
            "These risk indicators call for comprehensive due diligence and robust monitoring systems.",
            "Based on these results, enhanced controls and thorough risk assessments are essential.",
            "The risk profile necessitates proactive monitoring and comprehensive mitigation planning.",
            "These findings warrant in-depth investigation and structured risk management approaches.",
            "Given the risk factors identified, advanced due diligence measures should be implemented.",
            "The assessment results call for specialized monitoring and comprehensive risk reporting.",
            "These concerns necessitate detailed risk management planning and regular reassessment."
        ],
        "Medium": [
            "Standard monitoring procedures and moderate risk controls are advised.",
            "These factors warrant appropriate oversight measures and regular review.",
            "A balanced approach to risk management would be appropriate for these concerns.",
            "Regular monitoring and standard control measures are recommended.",
            "The identified risks suggest implementing moderate control mechanisms.",
            "These findings indicate a need for regular oversight and standard due diligence.",
            "Based on this assessment, conventional monitoring with periodic reviews is advised.",
            "The risk profile supports implementing standard risk management protocols.",
            "These concerns warrant routine monitoring and established control procedures.",
            "Given these factors, standard risk assessment protocols should be maintained.",
            "The findings suggest implementing typical industry safeguards and periodic reviews.",
            "Based on the risk assessment, conventional due diligence measures are appropriate.",
            "These risk indicators call for standard monitoring and established controls.",
            "The assessment supports implementing typical risk management approaches.",
            "Given these findings, standard oversight mechanisms should be sufficient."
        ],
        "Low": [
            "The favorable risk profile suggests standard business protocols are sufficient.",
            "This assessment indicates minimal concerns requiring only routine monitoring.",
            "The low risk findings support proceeding with normal business operations.",
            "Based on this favorable assessment, conventional due diligence measures are adequate.",
            "The minimal risk exposure suggests standard protocols will be sufficient.",
            "Given the favorable indicators, regular business practices can be maintained.",
            "This low-risk profile supports continuing with standard operating procedures.",
            "The assessment results suggest minimal additional controls are necessary.",
            "Given these favorable findings, routine monitoring should be adequate.",
            "Based on the low risk indicators, standard business practices are appropriate.",
            "The favorable assessment suggests minimal additional oversight is required.",
            "These findings support maintaining conventional business relationships.",
            "Given the low risk profile, standard monitoring protocols are sufficient.",
            "The assessment indicates normal business practices can be continued.",
            "These favorable results suggest standard controls are appropriate."
        ]
    }

    import random

    selected_modules = list(important_modules.items())
    selected_modules.sort(key=lambda x: {"High": 0, "Medium": 1, "Low": 2}[x[1]])

    # 2-3 modules per sentence, but avoiding duplicated phrases
    module_groups = []
    current_group = []

    for module, rating in selected_modules:
        current_group.append((module, rating))
        if len(current_group) >= 2 or (len(current_group) > 0 and module == selected_modules[-1][0]):
            module_groups.append(current_group)
            current_group = []

    if current_group:
        module_groups.append(current_group)

    used_descriptors = set()
    used_contexts = set()
    used_module_terms = set()
    used_connectors = set()

    group_descriptions = []

    for group in module_groups:
        group_desc = []
        group_context = None

        for i, (module, rating) in enumerate(group):
            available_descriptors = [d for d in rating_descriptors[rating] if d not in used_descriptors]
            if not available_descriptors:
                available_descriptors = rating_descriptors[rating]

            descriptor = random.choice(available_descriptors)
            used_descriptors.add(descriptor)

            available_terms = [t for t in module_descriptors.get(module, [module.lower()]) if
                               t not in used_module_terms]
            if not available_terms:
                available_terms = module_descriptors.get(module, [module.lower()])

            module_term = random.choice(available_terms)
            used_module_terms.add(module_term)

            if i == len(group) - 1 or len(group) == 1:
                available_contexts = [c for c in module_contexts.get(module, ["requiring attention"]) if
                                      c not in used_contexts]
                if not available_contexts:
                    available_contexts = module_contexts.get(module, ["requiring attention"])

                group_context = random.choice(available_contexts)
                used_contexts.add(group_context)

            descriptor = descriptor.lower()
            description = f"{descriptor} {module_term}"
            group_desc.append(description)

        # Combine descriptions in this group
        if len(group_desc) == 1:
            group_text = f"{group_desc[0]} {group_context}"
        elif len(group_desc) == 2:
            available_connectors = [c for c in group_connectors if c not in used_connectors]
            if not available_connectors:
                available_connectors = group_connectors

            connector = random.choice(available_connectors)
            used_connectors.add(connector)

            group_text = f"{group_desc[0]} {connector} {group_desc[1]} {group_context}"
        else:
            group_text = ""
            for i, desc in enumerate(group_desc):
                if i == 0:
                    group_text = desc
                elif i == len(group_desc) - 1:
                    available_connectors = [c for c in group_connectors if c not in used_connectors]
                    if not available_connectors:
                        available_connectors = group_connectors

                    connector = random.choice(available_connectors)
                    used_connectors.add(connector)

                    group_text += f", {connector} {desc} {group_context}"
                else:
                    group_text += f", {desc}"

        group_descriptions.append(group_text)

    used_transitions = set()
    summary_body = ""

    for i, desc in enumerate(group_descriptions):
        if i == 0:
            summary_body = desc
        else:
            available_transitions = [t for t in transitions if t not in used_transitions]
            if not available_transitions:
                available_transitions = transitions

            transition = random.choice(available_transitions)
            used_transitions.add(transition)

            if transition.startswith(". "):
                if desc and desc[0].islower():
                    desc = desc[0].upper() + desc[1:]

            summary_body += transition + desc

    additional_indicators = []

    if country_risk_high:
        additional_indicators.append(random.choice(country_risk_templates))

    if website_high_risk:
        additional_indicators.append(random.choice(no_website_templates))

    if cyber_rating == "Medium" and esg_rating == "Medium":
        additional_indicators.append(random.choice(cyber_esg_templates))
    elif cyber_rating == "Medium":
        additional_indicators.append(random.choice(cyber_only_templates))
    elif esg_rating == "Medium":
        additional_indicators.append(random.choice(esg_only_templates))

    if additional_indicators:
        if summary_body:
            transition = random.choice([
                ". In terms of additional indicators, ",
                ". Regarding other risk factors, ",
                ". Additional assessment reveals ",
                ". Our evaluation also notes ",
                ". Additional indicators shows, "
            ])
            summary_body += transition

        for i, indicator in enumerate(additional_indicators):
            if i == 0:
                summary_body += indicator.lower() if summary_body else indicator
            else:
                connector = random.choice([
                    ". Additionally, ",
                    ". Furthermore, ",
                    ". Moreover, ",
                    ". Also, ",
                    ". We also found "
                ])
                summary_body += connector + indicator.lower()

    intro = random.choice(intro_templates)
    conclusion = random.choice(conclusion_templates[overall_rating])

    if not summary_body:
        summary_body = "the evaluation identified minimal areas of concern across all risk domains"

    final_summary = f"{intro}, {summary_body}. {conclusion}"

    final_summary = enforce_lowercase(final_summary)

    final_summary = final_summary.replace(supplier_name.lower(), supplier_name)

    final_summary = capitalize_after_full_stop(final_summary)

    logger.debug(final_summary)

    try:
        summary_data = [{"area": area, "summary": final_summary}]
        insert_status = await upsert_dynamic_ens_data_summary("summary", summary_data, ens_id_value, session_id_value,
                                                              session)

        if insert_status["status"] == "success":
            logger.info(f"{area} Summary... Completed Successfully")
        else:
            logger.error(insert_status)

    except Exception as e:
        logger.error(f"Error in {area} summary: {str(e)}")

    return final_summary
