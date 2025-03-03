from app.core.utils.db_utils import get_dynamic_ens_data
from app.core.utils.db_utils import insert_dynamic_ens_data

async def company_profile(data, session):
    print("Fetching data from external supplier table...")

    ens_id = data.get("ens_id")
    session_id = data.get("session_id")
    required_columns = ["name", "country", "location", "address", "website", "is_active", "operation_type", "legal_form",
                        "national_identifier", "alias", "incorporation_date", "shareholders", "operating_revenue",
                        "num_subsidiaries", "num_companies_in_corp_grp",
                        "management", "no_of_employee"]

    retrieved_data = await get_dynamic_ens_data("external_supplier_data", required_columns, ens_id,
                                                session_id, session)
    retrieved_data = retrieved_data[0]

    print("Processing retrieved company data...")

    def format_alias(items):
        if isinstance(items, list):
            return "\n".join(items)
        return items

    def format_shareholders(shareholders):
        if isinstance(shareholders, list):
            top_shareholders = shareholders[:7]  # Limit to 7 shareholders
            return "\n".join(
                [shareholder.get("name", "") for shareholder in top_shareholders if isinstance(shareholder, dict)])
        return None

    def format_national_identifier(value):
        if isinstance(value, list) and value:
            return str(value[0])
        return value if isinstance(value, str) else None

    def management_names(management):
        if isinstance(management, list):
            management = management[:7]
            return "\n".join([person.get("name", "") for person in management if
                              isinstance(person, dict) and "name" in person])  # Ensure it's a dict
        return None

    def format_revenue(revenue_data):
        if isinstance(revenue_data, list) and revenue_data:
            latest_revenue = revenue_data[0].get("value")
            if latest_revenue is not None:
                return f"{latest_revenue:.3f} (USD, thousands)"
        return None

    company_data = {
        "name": retrieved_data.get("name"),
        "location": retrieved_data.get("location"),
        "address": retrieved_data.get("address"),
        "website": retrieved_data.get("website"),
        "active_status": retrieved_data.get("is_active"),
        "operation_type": "Publicly quoted" if retrieved_data.get("legal_form") == "Public limited companies" else "Private",
        "legal_status": retrieved_data.get("legal_form"),
        "national_identifier": format_national_identifier(retrieved_data.get("national_identifier")),
        "alias": format_alias(retrieved_data.get("alias")),
        "incorporation_date": f"{retrieved_data.get("incorporation_date")}" if retrieved_data.get("incorporation_date") else None,
        "shareholders": format_shareholders(retrieved_data.get("shareholders")),
        "revenue": format_revenue(retrieved_data.get("operating_revenue")),
        "subsidiaries": f"{retrieved_data.get('num_subsidiaries')} entities" if retrieved_data.get("num_subsidiaries") else None,
        "corporate_group": f"{retrieved_data.get('num_companies_in_corp_grp')} entities" if retrieved_data.get("num_companies_in_corp_grp") else None,
        "key_executives": management_names(retrieved_data.get("management")),
        "employee": f"{retrieved_data.get('no_of_employee')} employees" if retrieved_data.get("no_of_employee") else None
    }

    columns_data = [company_data]
    print(columns_data)
    # insert_dynamic_ens_data endpoint
    result = await insert_dynamic_ens_data("company_profile", columns_data, ens_id, session_id, session)

    if result.get("status") == "success":
        print("Company profile saved successfully.")
    else:
        print(f"Error saving company profile: {result.get('error')}")

    return {"ens_id": ens_id, "module": "COPR", "status": "completed"}