import random
from app.core.utils.db_utils import *
import re
import random

# def generate_summary(supplier_name):
#
#     financial_rating = "High"
#     liquidity_ratio = "1.5"
#     net_profit_margin = "8%"
#     debt_to_equity_ratio = "0.8"
#     revenue_growth = "5%"
#
#     adverse_media_exposure = "Low"
#     sentiment_score = "72"
#     risk_factor_score = "65"
#     public_sentiment_index = "80"
#     media_mentions_count = "3"
#
#     cybersecurity_risk = "Moderate"
#     threat_detection_efficiency = "85"
#     incident_response_time = "4"
#     security_compliance_score = "90"
#     vulnerability_count = "2"
#
#     esg_score = "75/100"
#     carbon_footprint_reduction = "10"
#     governance_risk_rating = "70"
#     social_responsibility_index = "78"
#     sustainability_initiative_score = "82"
#
#     intro_sentences = [
#         f"For {supplier_name}, the following report presents a concise summary of key performance indicators (KPIs) across financial, adverse media, cybersecurity, and ESG domains.",
#         f"For {supplier_name}, this report highlights significant findings across financial, adverse media, cybersecurity, and ESG KPIs, categorizing them into high, medium, and low impact levels.",
#         f"For {supplier_name}, an overview of critical risk areas in financial, adverse media, cybersecurity, and ESG KPIs is provided below, based on severity levels."
#     ]
#
#     conclusion_sentences = [
#         "Overall, the findings indicate a balanced performance with key areas for improvement and continued monitoring.",
#         "The organization maintains a stable position across most KPIs, with targeted actions recommended for risk mitigation.",
#         "While certain risks exist, proactive measures can strengthen resilience across all assessed domains."
#     ]
#
#     financial_sentences = [
#         f"Financial review confirms a {financial_rating} status with a liquidity ratio of {liquidity_ratio}, net profit margin of {net_profit_margin}, debt-to-equity ratio of {debt_to_equity_ratio}, and revenue growth of {revenue_growth}.",
#         f"Our analysis of financial metrics reveals a {financial_rating} rating, characterized by a liquidity ratio of {liquidity_ratio}, net profit margin of {net_profit_margin}, debt-to-equity ratio of {debt_to_equity_ratio}, and revenue growth of {revenue_growth}.",
#         f"Key financial indicators—including a liquidity ratio of {liquidity_ratio}, net profit margin of {net_profit_margin}, debt-to-equity ratio of {debt_to_equity_ratio}, and revenue growth of {revenue_growth}—support the overall {financial_rating} performance.",
#         f"Financial stability is reflected in the {financial_rating} rating, underscored by metrics such as a liquidity ratio of {liquidity_ratio}, net profit margin of {net_profit_margin}, debt-to-equity ratio of {debt_to_equity_ratio}, and revenue growth of {revenue_growth}.",
#         f"The financial analysis highlights a {financial_rating} status with a liquidity ratio of {liquidity_ratio}, net profit margin of {net_profit_margin}, debt-to-equity ratio of {debt_to_equity_ratio}, and revenue growth of {revenue_growth}."
#     ]
#
#     adverse_media_sentences = [
#         f"Adverse media monitoring indicates a {adverse_media_exposure} level of exposure, with a sentiment score of {sentiment_score}, a risk factor score of {risk_factor_score}, a public sentiment index of {public_sentiment_index}, and {media_mentions_count} media mentions.",
#         f"Media analysis shows a {adverse_media_exposure} exposure, evidenced by a sentiment score of {sentiment_score}, risk factor score of {risk_factor_score}, public sentiment index of {public_sentiment_index}, and {media_mentions_count} media mentions.",
#         f"A comprehensive review reveals {adverse_media_exposure} adverse media exposure, with a sentiment score of {sentiment_score}, risk factor score of {risk_factor_score}, public sentiment index of {public_sentiment_index}, and {media_mentions_count} media mentions.",
#         f"The adverse media assessment reports a {adverse_media_exposure} exposure, supported by a sentiment score of {sentiment_score}, risk factor score of {risk_factor_score}, public sentiment index of {public_sentiment_index}, and {media_mentions_count} media mentions.",
#         f"Findings from media monitoring indicate a {adverse_media_exposure} exposure level, with key metrics including a sentiment score of {sentiment_score}, risk factor score of {risk_factor_score}, public sentiment index of {public_sentiment_index}, and {media_mentions_count} media mentions."
#     ]
#
#     cybersecurity_sentences = [
#         f"Cybersecurity evaluations reveal a {cybersecurity_risk} risk level, with threat detection efficiency at {threat_detection_efficiency}%, an incident response time of {incident_response_time} hours, a security compliance score of {security_compliance_score}%, and {vulnerability_count} vulnerabilities identified.",
#         f"Our cybersecurity review indicates a {cybersecurity_risk} risk, evidenced by a threat detection efficiency of {threat_detection_efficiency}%, incident response time of {incident_response_time} hours, security compliance score of {security_compliance_score}%, and {vulnerability_count} vulnerabilities.",
#         f"Assessment of cyber metrics confirms a {cybersecurity_risk} risk level with key indicators such as a threat detection efficiency of {threat_detection_efficiency}%, an incident response time of {incident_response_time} hours, a security compliance score of {security_compliance_score}%, and {vulnerability_count} vulnerabilities.",
#         f"Detailed cybersecurity analysis shows a {cybersecurity_risk} risk, characterized by a threat detection efficiency of {threat_detection_efficiency}%, an incident response time of {incident_response_time} hours, a security compliance score of {security_compliance_score}%, and {vulnerability_count} vulnerabilities.",
#         f"The cybersecurity framework is assessed at a {cybersecurity_risk} risk level, with performance metrics including a threat detection efficiency of {threat_detection_efficiency}%, an incident response time of {incident_response_time} hours, a security compliance score of {security_compliance_score}%, and {vulnerability_count} vulnerabilities."
#     ]
#
#     esg_sentences = [
#         f"ESG performance is evaluated with an overall score of {esg_score}, a carbon footprint reduction of {carbon_footprint_reduction}%, a governance risk rating of {governance_risk_rating}, a social responsibility index of {social_responsibility_index}, and a sustainability initiative score of {sustainability_initiative_score}.",
#         f"The ESG analysis confirms robust performance with a score of {esg_score}, a carbon footprint reduction of {carbon_footprint_reduction}%, a governance risk rating of {governance_risk_rating}, a social responsibility index of {social_responsibility_index}, and a sustainability initiative score of {sustainability_initiative_score}.",
#         f"Our review of ESG metrics reports an overall score of {esg_score}, along with a carbon footprint reduction of {carbon_footprint_reduction}%, a governance risk rating of {governance_risk_rating}, a social responsibility index of {social_responsibility_index}, and a sustainability initiative score of {sustainability_initiative_score}.",
#         f"Detailed ESG assessments show a score of {esg_score} with accompanying metrics: carbon footprint reduction at {carbon_footprint_reduction}%, governance risk rating of {governance_risk_rating}, social responsibility index of {social_responsibility_index}, and a sustainability initiative score of {sustainability_initiative_score}.",
#         f"The ESG report indicates a comprehensive performance with an overall score of {esg_score}, a carbon footprint reduction of {carbon_footprint_reduction}%, a governance risk rating of {governance_risk_rating}, a social responsibility index of {social_responsibility_index}, and a sustainability initiative score of {sustainability_initiative_score}."
#     ]
#
#     summary = [
#         random.choice(intro_sentences),
#         random.choice(financial_sentences),
#         random.choice(adverse_media_sentences),
#         random.choice(cybersecurity_sentences),
#         random.choice(esg_sentences),
#         random.choice(conclusion_sentences)
#     ]
#     return " ".join(summary)

async def sape_summary(data, session):
    required_columns = ["kpi_area", "kpi_definition", "kpi_flag", "kpi_rating", "kpi_details"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("sape", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No notable sanction findings for the entity.", "No PEP findings for the individual."]

    summary_sentences = []
    sanctions_found = False
    pep_found = False

    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_flag = record.get("kpi_flag")
        kpi_rating = record.get("kpi_rating")
        kpi_definition = record.get("kpi_definition")
        kpi_details = record.get("kpi_details")

        if not kpi_flag or kpi_rating not in ["High", "Medium", "Low"]:
            continue

        # Process Sanctions (SAN)
        if kpi_area == "san":
            # Split kpi_details into individual sanction entries
            sanction_entries = kpi_details.split("Following sanctions imposed :")
            for entry in sanction_entries[1:]:  # Skip the first empty entry
                entry = entry.strip()
                if "Appeared with/on Sanctions Lists" in entry or "Associated with/on Sanctions Lists" in entry:

                    name = entry.split(":")[0].strip()
                    summary_sentences.append(f"{kpi_definition}: {name}: Appeared/Associated with/on Sanctions Lists")
                    sanctions_found = True

        # Process PEP (PEP)
        elif kpi_area == "pep":
            pep_entries = kpi_details.split("Following PeP findings :")
            for entry in pep_entries[1:]:  # Skip the first empty entry
                entry = entry.strip()
                if ":" in entry:
                    # Extract the PEP name
                    pep_name = entry.split(":")[1].strip()
                    summary_sentences.append(f"{kpi_definition}: {pep_name}")
                    pep_found = True

    if not sanctions_found:
        summary_sentences.append("No notable sanction findings for the entity.")
    if not pep_found:
        summary_sentences.append("No PEP findings for the individual.")

    return summary_sentences


async def bcf_summary(data, session):
    required_columns = ["kpi_area", "kpi_details", "kpi_rating", "kpi_flag"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No notable BCF findings for the entity.", "No BCF findings for the individual."]

    summary_sentences = []

    for record in retrieved_data:
        kpi_area = record.get("kpi_area")
        kpi_details = record.get("kpi_details")
        kpi_rating = record.get("kpi_rating")
        flag = record.get("kpi_flag", False)  # Default to False if not present

        if kpi_area != "BCF" or not kpi_details or not kpi_rating or not flag:
            continue

        # Remove date from kpi_details if present
        kpi_details = kpi_details.split(" (Date: ")[0].strip()

        # Check if kpi_details contains only irrelevant 'None' or empty values
        if not kpi_details or kpi_details.lower() == "none - none (none)" or kpi_details.lower() == "none":
            continue

        summary_sentences.append(kpi_details)

    if not summary_sentences:
        return ["No notable BCF findings for the entity.", "No BCF findings for the individual."]

    return summary_sentences


async def state_ownership_summary(data, session):

    required_columns = ["kpi_area", "kpi_definition", "kpi_flag", "kpi_rating", "kpi_details"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("sown", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No state ownership information available for the entity."]

    summary_sentences = []

    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_flag = record.get("kpi_flag")
        kpi_rating = record.get("kpi_rating")
        kpi_definition = record.get("kpi_definition")
        kpi_details = record.get("kpi_details")

        if kpi_area != "sco":
            continue

        # Process State Ownership
        if kpi_flag:
            summary_sentences.append(f" High: {kpi_definition}")
        else:
            summary_sentences.append(f" {kpi_rating}: {kpi_details}")

    if not summary_sentences:
        summary_sentences.append("No state ownership information available for the entity.")

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
    required_columns = ["kpi_area", "kpi_definition", "kpi_flag", "kpi_rating", "kpi_details"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    retrieved_data = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)

    if not retrieved_data:
        return ["No adverse media findings for the entity.", "No adverse media findings for the individual."]

    summary_sentences = []
    amr_found = False
    amo_found = False

    for record in retrieved_data:
        kpi_area = record.get("kpi_area", "").strip().lower()
        kpi_flag = record.get("kpi_flag")
        kpi_rating = record.get("kpi_rating")
        kpi_definition = record.get("kpi_definition")
        kpi_details = record.get("kpi_details")

        if not kpi_flag or kpi_rating not in ["High", "Medium", "Low"]:
            continue

        if kpi_area == "amr":
            # Case 1: Reputation Risk (AMR)
            entries = kpi_details.split("Reputation risk due to the following events:")
            for entry in entries[1:]:  # Skip the first empty entry
                entry = entry.strip()
                if ":" in entry:
                    event_type = entry.split(":")[0].strip()
                    date = entry.split("(Date:")[1].split(")")[0].strip() if "(Date:" in entry else ""
                    summary_sentences.append(f"{kpi_definition}: {event_type} (Date: {date})")
                    amr_found = True

        elif kpi_area == "amo":
            # Case 2: Other Criminal Activities (AMO)
            entries = kpi_details.split("Criminal activity discovered:")
            for entry in entries[1:]:  # Skip the first empty entry
                entry = entry.strip()
                if entry:
                    event_lines = entry.split("\n")
                    events = []
                    for line in event_lines:
                        if line.strip().startswith(("1.", "2.", "3.", "4.", "5.", "6.", "7.", "8.", "9.")):
                            event_type = line.split(" - ")[0].strip()
                            date = line.split("(Date:")[1].split(")")[0].strip() if "(Date:" in line else ""
                            events.append(f"{event_type} (Date: {date})")
                    if events:
                        summary_sentences.append(f"{kpi_definition}: {', '.join(events)}")
                        amo_found = True

    if not amr_found:
        summary_sentences.append("No adverse media findings for the entity.")
    if not amo_found:
        summary_sentences.append("No adverse media findings for the individual.")

    return summary_sentences[:2]

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

import re
async def legal_regulatory_summary(data, session):
    required_columns = ["kpi_area", "kpi_definition", "kpi_flag", "kpi_rating", "kpi_details"]
    ens_id_value = data.get("ens_id")
    session_id_value = data.get("session_id")

    leg_data = await get_dynamic_ens_data("lgrk", required_columns, ens_id_value, session_id_value, session)
    rfct_data = await get_dynamic_ens_data("rfct", required_columns, ens_id_value, session_id_value, session)

    if not leg_data and not rfct_data:
        return ["No legal or regulatory findings available."]

    summary_sentences = []

    def clean_text(text):
        text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces and newlines
        text = re.sub(r'\(Date: None\)', '', text)  # Remove '(Date: None)'
        text = re.sub(r'Risk identified due to the following events:', '', text)  # Remove the unwanted prefix
        return text

    def remove_multiple_dates(text):
        return re.sub(r'\(Date: \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\)', '', text)

    def process_records(records, expected_area, is_legal=False):
        seen_kpi_codes = set()
        for record in records:
            kpi_area = record.get("kpi_area", "").strip().lower()
            kpi_flag = record.get("kpi_flag")
            kpi_rating = record.get("kpi_rating")
            kpi_definition = record.get("kpi_definition")
            kpi_details = record.get("kpi_details")

            if kpi_area != expected_area or not kpi_flag or kpi_rating not in ["High", "Medium"]:
                continue

            kpi_code = record.get("kpi_code", "")  # Extract KPI code if available
            if kpi_code in seen_kpi_codes:
                continue  # Skip if already processed this KPI code
            seen_kpi_codes.add(kpi_code)

            details_list = []
            if isinstance(kpi_details, list):
                for detail in kpi_details:
                    detail = clean_text(detail)
                    if detail and "None - None" not in detail:
                        details_list.append(remove_multiple_dates(detail))
                        break  # Only keep the first relevant article
            elif isinstance(kpi_details, str):
                for detail in kpi_details.split("\n"):
                    detail = clean_text(detail)
                    if detail and "None - None" not in detail:
                        details_list.append(remove_multiple_dates(detail))
                        break  # Only keep the first relevant article

            if details_list:
                summary_sentences.append(f"{kpi_definition}: {', '.join(details_list)}")
    process_records(leg_data, "leg", is_legal=True)
    process_records(rfct_data, "reg")

    return summary_sentences if summary_sentences else ["No legal or regulatory findings available."]

async def overall_summary(data, session, supplier_name):
    financials_data = await financials_summary(data, session)
    adverse_media_data = await adverse_media_summary(data, session)
    cybersecurity_data = await cybersecurity_summary(data, session)
    esg_data = await esg_summary(data, session)
    bcf_data = await bcf_summary(data, session)
    state_ownership_data = await state_ownership_summary(data, session)
    legal_regulatory_data = await legal_regulatory_summary(data, session)
    sape_data = await sape_summary(data, session)  # Retrieve Sanctions and PEP data

    # Extract key metrics for financials
    financials_rating = "High" if "High" in financials_data[0] else "Medium" if "Medium" in financials_data[0] else "Low"

    # Extract key metrics for adverse media (AMR and AMO)
    adverse_media_amr_rating = "High" if "High" in adverse_media_data[0] else "Medium" if "Medium" in adverse_media_data[0] else "Low"
    adverse_media_amo_rating = "High" if "High" in adverse_media_data[0] else "Medium" if "Medium" in adverse_media_data[0] else "Low"

    # Extract key metrics for cybersecurity
    cybersecurity_rating = "High" if "High" in cybersecurity_data[0] else "Medium" if "Medium" in cybersecurity_data[0] else "Low"

    # Extract key metrics for ESG
    esg_rating = "High" if "High" in esg_data[0] else "Medium" if "Medium" in esg_data[0] else "Low"

    # Extract key metrics for BCF
    bcf_rating = "High" if "High" in bcf_data[0] else "Medium" if "Medium" in bcf_data[0] else "Low"

    # Extract key metrics for State Ownership
    state_ownership_rating = "High" if "High" in state_ownership_data[0] else "Medium" if "Medium" in state_ownership_data[0] else "Low"

    # Extract key metrics for Legal and Regulatory
    legal_rating = "High" if "High" in legal_regulatory_data[0] else "Medium" if "Medium" in legal_regulatory_data[0] else "Low"
    regulatory_rating = "High" if "High" in legal_regulatory_data[0] else "Medium" if "Medium" in legal_regulatory_data[0] else "Low"

    # Extract key metrics for Sanctions and PEP
    sanctions_rating = "High" if "High" in sape_data[0] else "Medium" if "Medium" in sape_data[0] else "Low"
    pep_rating = "High" if "High" in sape_data[0] else "Medium" if "Medium" in sape_data[0] else "Low"

    # Define sentence templates
    intro_sentences = [
        f"For {supplier_name}, the following report presents a concise summary of key performance indicators (KPIs) across financial, adverse media, cybersecurity, and ESG domains.",
        f"For {supplier_name}, this report highlights significant findings across financial, adverse media, cybersecurity, and ESG KPIs, categorizing them into high, medium, and low impact levels.",
        f"For {supplier_name}, an overview of critical risk areas in financial, adverse media, cybersecurity, and ESG KPIs is provided below, based on severity level."
    ]

    conclusion_sentences = [
        "Overall, the findings indicate a balanced performance with key areas for improvement and continued monitoring.",
        "The organization maintains a stable position across most KPIs, with targeted actions recommended for risk mitigation.",
        "While certain risks exist, proactive measures can strengthen resilience across all assessed domains."
    ]

    # Define KPI-specific sentence templates
    kpi_sentence_templates = {
        "financials": [
            f"The financial assessment indicates a {financials_rating} risk level, reflecting overall stability and key financial metrics.",
            f"Financial performance is rated as {financials_rating}, considering factors such as revenue trends, cost management, and financial sustainability.",
            f"A {financials_rating} financial risk rating suggests a need for ongoing monitoring of financial health and operational efficiency."
        ],
        "adverse_media_amr": [
            f"Adverse media analysis for reputational risk indicates a {adverse_media_amr_rating} risk level, highlighting potential impacts on brand perception.",
            f"The reputational risk from adverse media is rated {adverse_media_amr_rating}, suggesting moderate to high exposure to negative publicity.",
            f"Reputational risk is assessed at a {adverse_media_amr_rating} level, indicating significant adverse media coverage."
        ],
        "adverse_media_amo": [
            f"Adverse media analysis for other criminal activities indicates a {adverse_media_amo_rating} risk level, with notable incidents reported.",
            f"The risk level for other criminal activities is {adverse_media_amo_rating}, reflecting potential legal and operational challenges.",
            f"Other criminal activities are rated {adverse_media_amo_rating}, suggesting a need for enhanced monitoring and controls."
        ],
        "cybersecurity": [
            f"Cybersecurity analysis indicates a {cybersecurity_rating} risk level, reflecting vulnerabilities in IT infrastructure and data protection.",
            f"The cybersecurity risk rating is {cybersecurity_rating}, highlighting potential threats to digital assets and systems.",
            f"Cybersecurity is assessed at a {cybersecurity_rating} risk level, indicating moderate exposure to cyber threats."
        ],
        "esg": [
            f"ESG analysis indicates a {esg_rating} risk level, reflecting challenges in environmental, social, and governance practices.",
            f"The ESG risk rating is {esg_rating}, driven by factors such as carbon emissions, labor practices, and board diversity.",
            f"ESG performance is rated {esg_rating}, suggesting room for improvement in sustainability initiatives."
        ],
        "bcf": [
            f"Bribery, corruption, and fraud analysis indicates a {bcf_rating} risk level, reflecting the organization's exposure to unethical practices.",
            f"The risk level for bribery, corruption, and fraud is {bcf_rating}, highlighting potential compliance and reputational risks.",
            f"Bribery, corruption, and fraud are rated {bcf_rating}, indicating a relatively low exposure to such risks."
        ],
        "state_ownership": [
            f"State ownership analysis indicates a {state_ownership_rating} risk level, reflecting the organization's governance structure.",
            f"The state ownership risk rating is {state_ownership_rating}, suggesting limited government influence.",
            f"State ownership is assessed at a {state_ownership_rating} risk level, indicating no significant state control."
        ],
        "legal": [
            f"Legal analysis indicates a {legal_rating} risk level, reflecting potential litigation and regulatory challenges.",
            f"The legal risk rating is {legal_rating}, driven by factors such as ongoing lawsuits and compliance issues.",
            f"Legal risks are rated {legal_rating}, suggesting a stable legal environment with minimal concerns."
        ],
        "regulatory": [
            f"Regulatory analysis indicates a {regulatory_rating} risk level, reflecting the organization's compliance with industry regulations.",
            f"The regulatory risk rating is {regulatory_rating}, highlighting potential fines and penalties.",
            f"Regulatory risks are assessed at a {regulatory_rating} level, indicating moderate exposure to regulatory scrutiny."
        ],
        "sanctions": [
            f"Sanctions analysis indicates a {sanctions_rating} risk level, reflecting the organization's exposure to global sanctions.",
            f"The sanctions risk rating is {sanctions_rating}, suggesting limited exposure to sanctioned entities.",
            f"Sanctions are rated {sanctions_rating}, indicating a low risk of association with sanctioned parties."
        ],
        "pep": [
            f"PEP analysis indicates a {pep_rating} risk level, reflecting the organization's exposure to politically exposed persons.",
            f"The PEP risk rating is {pep_rating}, highlighting potential reputational and compliance risks.",
            f"PEP risks are assessed at a {pep_rating} level, indicating minimal exposure to politically exposed persons."
        ]
    }

    # Function to generate a random sentence for a KPI if the rating is High or Medium
    def generate_kpi_sentence(kpi_name, rating):
        if rating in ["High", "Medium"]:
            return random.choice(kpi_sentence_templates[kpi_name])
        return None

    # Construct the summary
    summary = [random.choice(intro_sentences)]

    # Add sentences for each KPI if the rating is High or Medium
    financial_sentence = generate_kpi_sentence("financials", financials_rating)
    if financial_sentence:
        summary.append(financial_sentence)

    adverse_media_amr_sentence = generate_kpi_sentence("adverse_media_amr", adverse_media_amr_rating)
    if adverse_media_amr_sentence:
        summary.append(adverse_media_amr_sentence)

    adverse_media_amo_sentence = generate_kpi_sentence("adverse_media_amo", adverse_media_amo_rating)
    if adverse_media_amo_sentence:
        summary.append(adverse_media_amo_sentence)

    cybersecurity_sentence = generate_kpi_sentence("cybersecurity", cybersecurity_rating)
    if cybersecurity_sentence:
        summary.append(cybersecurity_sentence)

    esg_sentence = generate_kpi_sentence("esg", esg_rating)
    if esg_sentence:
        summary.append(esg_sentence)

    bcf_sentence = generate_kpi_sentence("bcf", bcf_rating)
    if bcf_sentence:
        summary.append(bcf_sentence)

    state_ownership_sentence = generate_kpi_sentence("state_ownership", state_ownership_rating)
    if state_ownership_sentence:
        summary.append(state_ownership_sentence)

    legal_sentence = generate_kpi_sentence("legal", legal_rating)
    if legal_sentence:
        summary.append(legal_sentence)

    regulatory_sentence = generate_kpi_sentence("regulatory", regulatory_rating)
    if regulatory_sentence:
        summary.append(regulatory_sentence)

    sanctions_sentence = generate_kpi_sentence("sanctions", sanctions_rating)
    if sanctions_sentence:
        summary.append(sanctions_sentence)

    pep_sentence = generate_kpi_sentence("pep", pep_rating)
    if pep_sentence:
        summary.append(pep_sentence)

    # Add conclusion
    summary.append(random.choice(conclusion_sentences))

    # Join the summary into a single string
    final_summary = " ".join(summary)
    return final_summary