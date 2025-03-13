import random
from app.core.utils.db_utils import *
import re
from datetime import datetime
import json

async def sape_summary(data, session):
    required_columns = ["kpi_area", "kpi_flag", "kpi_rating", "kpi_value"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("sape", required_columns, ens_id_value, session_id_value, session)
    if not retrieved_data:
        return ["No notable sanction findings for the entity."]

    summary_sentences = []
    sanctions_found = False

    target_mapping = {
        "org": "organisation",
        "person": "individuals associated with this entity"
    }

    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_flag = record.get("kpi_flag")
        kpi_rating = record.get("kpi_rating")
        kpi_values = record.get("kpi_value")

        if kpi_area != "san" or not kpi_flag or kpi_rating not in ["High", "Medium", "Low"]:
            continue

        try:
            kpi_values_json = json.loads(kpi_values)
        except json.JSONDecodeError:
            continue

        count = kpi_values_json.get("count", 0)
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

        # Sort findings by date (if available), with entries without dates at the end
        processed_findings.sort(key=lambda x: x["date"] if x["date"] else datetime.min, reverse=True)

        if count > 0:
            # Map target to the desired output
            target_display = target_mapping.get(target, target)
            summary = f"There are {count} sanction events"
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
            sanctions_found = True

    if not sanctions_found:
        summary_sentences.append("No notable sanction findings for the entity.")
    return summary_sentences

async def bcf_summary(data, session):
    required_columns = ["kpi_area", "kpi_flag", "kpi_rating", "kpi_value"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No notable Bribery, Corruption, or Fraud findings for this entity."]

    summary_sentences = []
    bcf_processed = False
    bcf_has_findings = False
    entity_type = "entity"

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
        try:
            count = int(kpi_values_json.get("count", 0))
        except (ValueError, TypeError):
            count = 0

        target = kpi_values_json.get("target", "").lower()
        findings = kpi_values_json.get("findings", [])

        if not findings:
            continue
        if target in ["org", "organization"]:
            entity_type = "organization"
        elif target in ["person"]:
            entity_type = "individual"

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
            summary = f"There are {count} Bribery, Corruption, or Fraud findings"
            if target == "org":
                summary += " for the organisation."
            elif target == "person":
                summary += " for the individuals associated with this entity."
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
        return ["No notable BCF findings for this entity."]

    return summary_sentences

async def state_ownership_summary(data, session):
    required_columns = ["kpi_area", "kpi_definition", "kpi_flag", "kpi_rating", "kpi_details", "kpi_value"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    all_data = await get_dynamic_ens_data("sown", required_columns, ens_id_value, session_id_value, session)

    summary_sentences = []
    pep_found = False

    # Process State Ownership data
    if all_data:
        for record in all_data:
            kpi_area = record.get("kpi_area", "").strip().lower()
            kpi_flag = record.get("kpi_flag")
            kpi_rating = record.get("kpi_rating")
            kpi_definition = record.get("kpi_definition")
            kpi_details = record.get("kpi_details")

            if kpi_area != "sco":
                continue
            # Process State Ownership
            if kpi_flag:
                summary_sentences.append(f" High risk identified for the entity: {kpi_definition}")
            else:
                summary_sentences.append(f" {kpi_rating} risk identified for the entity: {kpi_details}")

    # Process PEP data
    if all_data:
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
                print("JSON Parsing Error:", e, kpi_values)
                continue

            count = kpi_values_json.get("count", 0)
            target = kpi_values_json.get("target", "").lower()
            findings = kpi_values_json.get("findings", [])
            if count > 0:
                pep_found = True
            if not findings or count <= 0:
                continue
            earliest_year = None
            processed_findings = []
            for finding in findings:
                eventdt = finding.get("eventdt")
                event_desc = finding.get("eventDesc", "").strip()
                if eventdt and eventdt != 'No event date available':
                    try:
                        event_date = datetime.strptime(eventdt, "%Y-%m-%d")
                        event_year = event_date.year
                        if earliest_year is None or event_year < earliest_year:
                            earliest_year = event_year
                        entity_name = finding.get("entityName", "Unknown Entity")
                        processed_findings.append({"date": event_date, "desc": event_desc, "entityName": entity_name})
                    except ValueError:
                        pass
                else:
                    entity_name = finding.get("entityName", "Unknown Entity")
                    processed_findings.append({"date": None, "desc": event_desc, "entityName": entity_name})

            processed_findings.sort(key=lambda x: x["date"] if x["date"] else datetime.min, reverse=True)

            if count > 0:
                summary = f"There are {count} PEP findings"
                if target == "org":
                    summary += " for the organisation."
                elif target == "person":
                    summary += " for the individuals associated with this entity."
                else:
                    summary += "."
                if earliest_year:
                    summary += f" The findings have been since {earliest_year}."
                if processed_findings:
                    summary += " Some of the most recent PEP events include:\n"
                    for finding in processed_findings[:2]:
                        event_desc = finding["desc"]
                        entity_name = finding.get("entityName", "Unknown Entity")
                        if finding["date"]:
                            event_year = finding["date"].year
                            summary += f"- In {event_year}, {entity_name}: {event_desc}\n"
                        else:
                            summary += f"- {entity_name}: {event_desc}\n"
                summary_sentences.append(summary)

    if not summary_sentences or (len(summary_sentences) == 0 and not pep_found):
        summary_sentences.append("No state ownership information available for the entity.")
    if not pep_found:
        summary_sentences.append("No PEP findings for the individual.")
    return summary_sentences

async def financials_summary(data, session):
    required_columns = ["kpi_area", "kpi_definition", "kpi_flag", "kpi_rating", "kpi_details"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("fstb", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No financials available."]

    summary_sentences = []
    financials_found = False

    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_flag = record.get("kpi_flag")
        kpi_rating = record.get("kpi_rating")
        kpi_definition = record.get("kpi_definition")
        kpi_details = record.get("kpi_details")

        if kpi_area != "bkr":
            continue

        # Process Financials (BKR)
        if kpi_flag and kpi_rating in ["High", "Medium", "Low"]:
            summary_sentences.append(kpi_details)
            financials_found = True

    if not financials_found:
        summary_sentences.append("No financials available.")

    return summary_sentences

async def adverse_media_summary(data, session):
    required_columns = ["kpi_area", "kpi_flag", "kpi_rating", "kpi_value"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No adverse media findings for this entity."]

    summary_sentences = []
    amr_processed = False
    amo_processed = False
    amr_has_findings = False
    amo_has_findings = False

    entity_type = "entity"
    for record in retrieved_data:
        kpi_values = record.get("kpi_value")
        if not isinstance(kpi_values, str):
            continue
        try:
            kpi_values_json = json.loads(kpi_values)
            target = kpi_values_json.get("target", "").lower()
            if target == "org":
                entity_type = "organization"
                break
            elif target == "person" and entity_type != "organization":
                entity_type = "individuals associated with this entity"
        except json.JSONDecodeError:
            continue

    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_flag = record.get("kpi_flag")
        kpi_rating = record.get("kpi_rating")
        kpi_values = record.get("kpi_value")

        if kpi_area not in ["amr", "amo"]:
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
        try:
            count = int(kpi_values_json.get("count", 0))
        except (ValueError, TypeError):
            count = 0

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
            if kpi_area == "amr":
                amr_has_findings = True
                summary = f"There are {count} adverse media reputation risk findings"
            elif kpi_area == "amo":
                amo_has_findings = True
                summary = f"There are {count} adverse media criminal activity findings"

            summary += f" for this {entity_type}."
            if earliest_year:
                summary += f" The findings has been since {earliest_year}."

            if processed_findings:
                summary += " Some of the most recent media events include: \n"
                for finding in processed_findings[:2]:  # Use the 2 most recent events
                    event_year = finding["date"].year
                    event_desc = finding["desc"]
                    summary += f"- In {event_year}, {event_desc}\n"

            summary_sentences.append(summary)

    if not amr_has_findings and not amo_has_findings:
        summary_sentences.append(f"No adverse media findings for this {entity_type}.")
    elif not amo_has_findings:
        summary_sentences.append(f"No adverse media criminal findings for this {entity_type}.")

    if not summary_sentences:
        return ["No adverse media findings for this entity."]

    return summary_sentences

async def cybersecurity_summary(data, session):
    required_columns = ["kpi_area", "kpi_code", "kpi_flag", "kpi_details"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("cyes", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No cybersecurity findings available."]

    summary_sentences = []

    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_code = record.get("kpi_code", "").strip()
        kpi_flag = record.get("kpi_flag")
        kpi_details = record.get("kpi_details")

        if kpi_area != "cyb" or kpi_code != "CYB2A":
            continue

        # Process CYB2A record
        if kpi_details:
            summary_sentences.append(kpi_details)

    if not summary_sentences:
        summary_sentences.append("No cybersecurity findings available.")

    return summary_sentences

async def esg_summary(data, session):
    required_columns = ["kpi_area", "kpi_code", "kpi_flag", "kpi_details"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("cyes", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No ESG data available."]

    summary_sentences = []

    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_code = record.get("kpi_code", "").strip()
        kpi_flag = record.get("kpi_flag")
        kpi_details = record.get("kpi_details")

        if kpi_area != "esg" or kpi_code != "ESG1A":
            continue

        if kpi_flag:
            if kpi_details:
                summary_sentences.append(kpi_details)
        else:
            summary_sentences.append("No ESG score available.")

    if not summary_sentences:
        summary_sentences.append("No ESG data available.")

    return summary_sentences

async def legal_regulatory_summary(data, session):
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
    entity_type = "entity"  # Default value

    # Determine entity type from all records first before processing findings
    def determine_entity_type(records):
        nonlocal entity_type

        for record in records:
            kpi_values = record.get("kpi_value")
            if not isinstance(kpi_values, str):
                continue

            try:
                kpi_values_json = json.loads(kpi_values)
                if isinstance(kpi_values_json, list) and kpi_values_json:
                    kpi_values_json = kpi_values_json[0]

                target = kpi_values_json.get("target", "").lower()
                if target in ["org", "organization"]:
                    entity_type = "organization"
                    return
                elif target == "person" and entity_type != "organization":
                    entity_type = "individuals associated with this entity"
            except json.JSONDecodeError:
                continue

    determine_entity_type(leg_data)
    if entity_type != "organization":
        determine_entity_type(rfct_data)

    def process_records(records, expected_area, is_legal=False):
        nonlocal leg_processed, reg_processed, leg_has_findings, reg_has_findings

        for record in records:
            kpi_area = record.get("kpi_area", "").strip().lower()
            kpi_flag = record.get("kpi_flag")
            kpi_rating = record.get("kpi_rating")
            kpi_values = record.get("kpi_value")

            if kpi_area != expected_area:
                continue
            if is_legal:
                leg_processed = True
            else:
                reg_processed = True
            if not kpi_flag or kpi_rating not in ["High", "Medium", "Low"]:
                continue
            try:
                kpi_values_json = json.loads(kpi_values)
                if isinstance(kpi_values_json, list) and kpi_values_json:  # If it's a list, take the first element
                    kpi_values_json = kpi_values_json[0]
            except json.JSONDecodeError:
                continue
            try:
                count = int(kpi_values_json.get("count", 0))
            except (ValueError, TypeError):
                count = 0
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
                if is_legal:
                    leg_has_findings = True
                    summary = f"There are {count} legal findings"
                else:
                    reg_has_findings = True
                    summary = f"There are {count} regulatory findings"

                summary += f" for this {entity_type}."

                if earliest_year:
                    summary += f" The findings has been since {earliest_year}."
                if processed_findings:
                    summary += " Some of the most recent events include: \n"
                    for finding in processed_findings[:2]:  # Use the 2 most recent events
                        event_year = finding["date"].year
                        event_desc = finding["desc"]
                        summary += f"- In {event_year}, {event_desc}\n"

                summary_sentences.append(summary)

    process_records(leg_data, "leg", is_legal=True)
    process_records(rfct_data, "reg")

    if leg_processed and not leg_has_findings:
        summary_sentences.append(f"No legal findings for this {entity_type}.")
    if reg_processed and not reg_has_findings:
        summary_sentences.append(f"No regulatory findings for this {entity_type}.")
    if not summary_sentences:
        return ["No legal or regulatory findings available."]

    return summary_sentences


async def overall_summary(data, session, supplier_name):
    required_columns = ["kpi_code", "kpi_area", "kpi_rating"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("ovar", required_columns, ens_id_value, session_id_value, session)

    if isinstance(retrieved_data, str):
        import json
        retrieved_data = json.loads(retrieved_data)
    elif not isinstance(retrieved_data, list):
        retrieved_data = [retrieved_data]

    theme_ratings = {}
    overall_rating = None

    for row in retrieved_data:
        if not isinstance(row, dict):
            print("Unexpected row format:", row)
            continue

        kpi_code = row.get("kpi_code")
        kpi_area = row.get("kpi_area")
        kpi_rating = row.get("kpi_rating")

        if kpi_area == "theme_rating":
            theme_ratings[kpi_code] = kpi_rating
        elif kpi_area == "overall_rating" and kpi_code == "supplier":
            overall_rating = kpi_rating

    if overall_rating is None:
        overall_rating = "Low"

    # Map the KPI codes to the module names
    module_ratings = {
        "Financials": theme_ratings.get("financials", "Low"),
        "Adverse Media (Reputation)": theme_ratings.get("other_adverse_media", "Low"),
        "Adverse Media (Other)": theme_ratings.get("other_adverse_media", "Low"),
        "Cybersecurity": theme_ratings.get("cyber", "Low"),
        "ESG": theme_ratings.get("esg", "Low"),
        "Bribery, Corruption & Fraud": theme_ratings.get("bribery_corruption_overall", "Low"),
        "State Ownership": theme_ratings.get("government_political", "Low"),
        "Legal": theme_ratings.get("regulatory_legal", "Low"),
        "Regulatory": theme_ratings.get("regulatory_legal", "Low"),
        "Sanctions": theme_ratings.get("sanctions", "Low"),
        "PEP": theme_ratings.get("government_political", "Low")
    }

    important_modules = {k: v for k, v in module_ratings.items() if v in ["High", "Medium"]}

    # If no high/medium risks, include at least one low-risk module for balance
    if not important_modules:
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

    #  module based phrasings
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
        "Cybersecurity": [
            "in their digital infrastructure",
            "affecting data protection measures",
            "impacting information security",
            "in their network defenses",
            "exposing potential vulnerabilities",
            "raising digital governance questions",
            "affecting customer data protection",
            "potentially exposing sensitive information",
            "in their cybersecurity protocols",
            "within their IT security framework",
            "affecting system integrity assurance",
            "challenging incident response capabilities",
            "within their data management practices",
            "raising concerns about defense-in-depth strategies",
            "affecting resilience against digital threats"
        ],
        "ESG": [
            "raising sustainability concerns",
            "affecting compliance with standards",
            "impacting corporate responsibility",
            "challenging environmental commitments",
            "affecting social responsibility metrics",
            "raising questions about governance practices",
            "potentially affecting investor ESG ratings",
            "challenging industry best practices",
            "affecting carbon reduction initiatives",
            "potentially limiting access to ESG-focused capital",
            "complicating regulatory compliance",
            "affecting stakeholder engagement",
            "raising questions about long-term sustainability",
            "affecting social impact measurement",
            "challenging their position on ethical issues"
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
            "affecting cross-border payments",
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
            "PR vulnerabilities", "public opinion factors", "media sentiment", "corporate image status",
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
        "Cybersecurity": [
            "cybersecurity posture", "cyber risk exposure", "cybersecurity vulnerabilities",
            "digital protection measures", "information security stance", "cyber defense readiness",
            "network security status", "data protection framework", "IT security infrastructure",
            "digital risk management", "security protocol effectiveness",
            "threat prevention capabilities", "cyber resilience", "data safeguards", "security architecture"
        ],
        "ESG": [
            "ESG practices", "sustainability metrics", "environmental compliance", "social responsibility measures",
            "governance standards", "corporate responsibility framework", "ethical business conduct",
            "sustainability indicators", "environmental impact management", "social impact measures",
            "corporate governance quality", "ethical business standards",
            "sustainability commitment", "environmental stewardship", "social performance indicators"
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
            "government official relationships", "political influence factors", "PEP screening results",
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
            # descriptor that hasn't been used
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

    # Connect the group descriptions
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

    intro = random.choice(intro_templates)
    conclusion = random.choice(conclusion_templates[overall_rating])
    final_summary = f"{intro}, {summary_body}. {conclusion}"

    print(final_summary)
    return final_summary
