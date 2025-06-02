# util functions 
import json
from app.schemas.logger import logger
##########################################################

# for google
def get_country_code_google(country_name, country_data):
    for country in country_data:
        if country["countryName"].lower() == country_name.lower():
            return country["countryCode"]
    return None  
def get_country_google(country_code, country_data):
    country_mapping = {entry['countryCode']: entry['countryName'] for entry in country_data}
    country = country_mapping.get(country_code, "not found")
    return country

# for bing
def get_country_code_bing(country_name, country_data):
    for country in country_data["countries"]:
        if country["country_name"].lower() == country_name.lower():
            return country["country_code"]
    return None

def get_country_bing(country_code, country_data):
    country_mapping = {entry['country_code']: entry['country_name'] for entry in country_data["countries"]}
    country = country_mapping.get(country_code, "not found")
    return country

##########################################################

def aggregate_verified_flag(data):
    try:
        # Check if input is a valid list of dictionaries
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise ValueError("LLM Output Error: Input must be a list of dictionaries.")

        # Ensure the required keys exist in all dictionaries
        required_keys = {"country", "company", "verified"}
        if not all(required_keys.issubset(item.keys()) for item in data):
            logger.error("Each dictionary must contain the keys")
            raise KeyError(f"Each dictionary must contain the keys: {required_keys}")

        # Aggregate country and company (assuming they are consistent across the list)
        country = data[0]['country']
        company = data[0]['company']

        # Count 'Yes' and 'No' occurrences in the 'verified' field
        yes_count = sum(1 for item in data if item['verified'].lower() == 'yes')
        no_count = sum(1 for item in data if item['verified'].lower() == 'no')

        # Determine the aggregated 'verified' flag based on the presence of 'Yes'
        if yes_count > 0:
            aggregated_verified = 'Yes'
        else:
            aggregated_verified = 'No'

        # Calculate the percentage of 'Yes' responses
        total_count = len(data)
        yes_percentage = (yes_count / total_count) * 100 if total_count > 0 else 0

        # Return the aggregated result along with the '%_yes' key and 'num_articles' key
        return {
            "country": str(country),
            "company": str(company),
            "verified": str(aggregated_verified),
            "num_yes": int(yes_count),
            "num_analysed": int(total_count)
        }
    except Exception as e:
        logger.error(f"Aggregation of verified flag error: {str(e)}")

def calculate_metric(num_true, num_analyzed, max_articles=10):
    if num_analyzed == 0:
        return 0  # No articles analyzed, metric is 0
    
    true_percentage = num_true / num_analyzed
    weight = num_analyzed / max_articles
    metric = true_percentage * weight
    return metric

def filter_supplier_data(json_data, national_id, max_results:int):

    try:
        # Extract the data list from the JSON
        supplier_data = json_data.get('data', [])
        potential_pass = False
        matched = False

        # 0. No Matches from Orbis
        if not supplier_data:
            return {}, False, False

        # logger.debug("- ------------------------- ALL MATCHES:")
        # logger.debug(json.dumps(supplier_data, indent=2))

        # ------------ 1. check if there are any 'Selected' matches in the 'HINT' key inside the 'MATCH' dictionary
        selected_matches = [supplier for supplier in supplier_data
                            if 'MATCH' in supplier and '0' in supplier['MATCH']
                            and supplier['MATCH']['0']['HINT'] == 'Selected']

        if selected_matches:
            # Get top selected match with national id, else get top match
            temp = selected_matches[0]
            for match in selected_matches:
                if str(match.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')) == national_id:
                    logger.debug("THIS FINDING MATCHES THE UPLOADED NATIONAL ID -------------")
                    temp = match
                    break
            selected_final_match = temp
            matched = True

            return selected_final_match, potential_pass, matched

        # ------------ 2. If no 'Selected' matches, check for 'Potential' matches with score > 0.90
        high_scoring_potential_matches = [supplier for supplier in supplier_data
                             if 'MATCH' in supplier and '0' in supplier['MATCH']
                             and supplier['MATCH']['0']['HINT'] == 'Potential'
                             and supplier['MATCH']['0']['SCORE'] > 0.85]

        if high_scoring_potential_matches:
            potential_pass = True
            sorted_potential_matches = sorted(high_scoring_potential_matches, key=lambda x: x['MATCH']['0']['SCORE'], reverse=True)
            temp = max(sorted_potential_matches, key=lambda x: x['MATCH']['0'].get('SCORE', 0))
            for match in sorted_potential_matches:
                if str(match.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')) == national_id:
                    logger.debug("THIS FINDING MATCHES THE UPLOADED NATIONAL ID -------------")
                    temp = match
                    potential_pass = False
                    matched = True
                    break

            selected_final_match = temp
            return selected_final_match, potential_pass, matched

        # ------------ 3. If no 'Selected' or 'Potential' matches with score > 0.85, check any match > 0.65
        low_scoring_matches = [supplier for supplier in supplier_data
                             if 'MATCH' in supplier and '0' in supplier['MATCH']
                             and supplier['MATCH']['0']['SCORE'] > 0.65]

        if low_scoring_matches:
            potential_pass = True
            sorted_low_scoring_matches = sorted(low_scoring_matches, key=lambda x: x['MATCH']['0']['SCORE'],reverse=True)
            temp = max(sorted_low_scoring_matches, key=lambda x: x['MATCH']['0'].get('SCORE', 0))
            for match in sorted_low_scoring_matches:
                logger.warning("IN LOW SCORING MATCHES")
                logger.debug(match)
                if str(match.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')) == national_id:
                    logger.info("THIS FINDING MATCHES THE UPLOADED NATIONAL ID -------------")
                    temp = match
                    potential_pass = False
                    matched = True
                    break
            top_scoring_match = temp

            if top_scoring_match:
                return top_scoring_match, potential_pass, matched

        # ------------ 4. Any match at all, first if national id exists, else top match
        any_match = max(supplier_data, key=lambda x: x['MATCH']['0'].get('SCORE', 0))
        if any_match:
            potential_pass = True
            for match in supplier_data:
                if str(match.get('MATCH', {}).get('0', {}).get('NATIONAL_ID', 'N/A')) == national_id:
                    logger.debug("THIS FINDING MATCHES THE UPLOADED NATIONAL ID -------------")
                    any_match = match
                    potential_pass = False
                    matched = True
                    break
            return any_match, potential_pass, matched

        # ------------ 5. No match found at all, both false
        return {}, potential_pass, matched

    except Exception as e:
        logger.error(f"Error found filtering supplier data ---> {str(e)}")
        return {}, False, False
