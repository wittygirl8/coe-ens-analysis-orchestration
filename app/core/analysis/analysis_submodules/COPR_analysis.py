import json

from app.core.utils.db_utils import get_dynamic_ens_data
from app.core.utils.db_utils import *
import re

async def company_profile(data, session):
    print("Performing Company Profile...")

    ens_id = data.get("ens_id")
    session_id = data.get("session_id")
    required_columns = ["name", "country", "location", "address", "website", "is_active", "operation_type", "legal_form",
                        "national_identifier", "national_identifier_type", "alias", "incorporation_date", "shareholders","operating_revenue_usd",
                        "num_subsidiaries", "num_companies_in_corp_grp","management", "no_of_employee"]

    retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id,
                                                session_id, session)
    retrieved_data = retrieved_data[0]
    print("Processing retrieved company data...")

    supplier_master_data = await get_dynamic_ens_data("supplier_master_data", ["national_id"], ens_id,
                                                      session_id, session)
    supplier_national_id = supplier_master_data[0].get("national_id") if supplier_master_data else None

    def format_alias(items):
        if isinstance(items, list):
            items = list(set(items))[:7]
            return "\n\n".join(items)
        return items

    import re

    def format_shareholders(shareholders):
        if isinstance(shareholders, list):
            found_count = 0
            top_shareholders = shareholders
            formatted_shareholders = []
            for shareholder in top_shareholders:
                if isinstance(shareholder, dict):
                    name = shareholder.get("name", "")
                    ownership = shareholder.get("direct_ownership", "-")
                    if ownership is None:
                        continue
                    if ownership == "-":
                        continue  # SKIP
                    if ownership == "n.a.":
                        ownership_string = ""
                    if "ng" in ownership.lower():
                        ownership_string = " (<= 0.01%)"
                    elif "fc" in ownership.lower():
                        ownership_string = " (Foreign company)"
                    elif "wo" in ownership.lower():
                        ownership_string = " (Wholly owned, >= 98%)"
                    elif "mo" in ownership.lower():
                        ownership_string = " (Majority owned, > 50%)"
                    elif "jo" in ownership.lower():
                        ownership_string = " (Jointly owned, = 50%)"
                    elif "t" in ownership.lower():
                        ownership_string = " (Sole trader, = 100%)"
                    elif "reg" in ownership.lower():
                        ownership_string = " (Beneficial Owner from register, = 100%)"
                    elif "gp" in ownership.lower():
                        ownership_string = " (General partner)"
                    elif "dm" in ownership.lower():
                        ownership_string = " (Director / Manager)"
                    elif "ve" in ownership.lower():
                        ownership_string = " (Vessel)"
                    elif "br" in ownership.lower():
                        ownership_string = " (Branch)"
                    elif "cqp1" in ownership.lower():
                        ownership_string = " (50% + 1 Share)"
                    elif not re.match(r'^\d', ownership):
                        ownership_string = ""
                    else:
                        ownership_string = f" ({ownership}%)"
                    formatted_shareholders.append(f"{name}{ownership_string}")
                    found_count += 1
                    if found_count > 6:  # Limit to 7 shareholders
                        break
            return "\n\n".join(formatted_shareholders)  # Double newline break
        return None

    def format_national_identifier(national_identifier, national_identifier_type, supplier_national_id):
        if isinstance(national_identifier, list) and isinstance(national_identifier_type, list):
            zipped_identifiers = list(zip(national_identifier_type, national_identifier))
            for identifier_type, identifier in zipped_identifiers:
                if identifier == supplier_national_id:
                    return f"{identifier_type}: {identifier}"

            if zipped_identifiers:
                return f"{zipped_identifiers[0][0]}: {zipped_identifiers[0][1]}"

        return national_identifier if isinstance(national_identifier, str) else None

    def management_names(management):
        if isinstance(management, list):
            management = management[:7]
            return "\n".join([person.get("name", "") for person in management if
                              isinstance(person, dict) and "name" in person])
        return None

    def format_revenue(revenue_data):
        if isinstance(revenue_data, list) and revenue_data:
            latest_revenue = revenue_data[0].get("value")
            latest_date = revenue_data[0].get("closing_date","")
            if latest_revenue is not None:
                return f"{format_revenue_num(latest_revenue)} (USD - {latest_date})"
        return None

    def format_revenue_num(value_str):
        try:
            value = float(value_str)
            if value >= 1_000_000_000:
                return f"{value / 1_000_000_000:.0f}B"
            elif value >= 1_000_000:
                return f"{value / 1_000_000:.0f}M"
        except:
            return value_str

    def format_incorporation_date(date):
        if date:
            try:
                return date.strftime("%d/%m/%Y")
            except AttributeError:
                from datetime import datetime
                try:
                    date_obj = datetime.strptime(date, "%Y-%m-%d")
                    return date_obj.strftime("%d/%m/%Y")
                except ValueError:
                    return date
        return None

    company_data = {
        "name": retrieved_data.get("name"),
        "location": retrieved_data.get("location"),
        "address": retrieved_data.get("address"),
        "website": retrieved_data.get("website"),
        "active_status": retrieved_data.get("is_active"),
        "operation_type": "Publicly quoted" if retrieved_data.get("legal_form") == "Public limited companies" else "Private",
        "legal_status": retrieved_data.get("legal_form"),
        "national_identifier": format_national_identifier(retrieved_data.get("national_identifier"), retrieved_data.get("national_identifier_type"),supplier_national_id),
        "alias": format_alias(retrieved_data.get("alias")),
        "incorporation_date": format_incorporation_date(retrieved_data.get("incorporation_date")),
        "shareholders": format_shareholders(retrieved_data.get("shareholders")),
        "revenue": format_revenue(retrieved_data.get("operating_revenue_usd")),
        "subsidiaries": f"{retrieved_data.get('num_subsidiaries')} entities" if retrieved_data.get("num_subsidiaries") else None,
        "corporate_group": f"{retrieved_data.get('num_companies_in_corp_grp')} entities" if retrieved_data.get("num_companies_in_corp_grp") else None,
        "key_executives": management_names(retrieved_data.get("management")),
        "employee": f"{retrieved_data.get('no_of_employee')} employees" if retrieved_data.get("no_of_employee") else None
    }
    print(json.dumps(company_data, indent=2))
    columns_data = [company_data]
    result = await upsert_dynamic_ens_data("company_profile", columns_data, ens_id, session_id, session)

    if result.get("status") == "success":
        print("Company profile saved successfully.")
    else:
        print(f"Error saving company profile: {result.get('error')}")

    return {"ens_id": ens_id, "module": "COPR", "status": "completed"}