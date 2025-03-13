import requests
# from RepGen.data_extraction import *

from dotenv import load_dotenv
import openai
from openai import AzureOpenAI


def run_ts_analysis(client, model, article, name, country, url):
    
    message_text = [
    {
        "role": "system",
        "content": (
            "You are an AI assistant tasked with validating company information based on news articles. Follow these rules strictly:\n\n"
            "1. Determine if the company mentioned in the article exists and has a social presence online.\n"
            "2. A company has 'social presence' if the article explicitly mentions websites, social media platforms, or online activities related to the company.\n"
            "3. Your output must be a JSON object with the following keys:\n"
            "   - 'country': The input country.\n"
            "   - 'company': The input company name.\n"
            "   - 'verified': 'Yes' if the article confirms the company's existence and its social presence, otherwise 'No'.\n"
            # "   - 'ids': Should be a unique identifier number that is found in the article. Which means national ids/ VAT ID/ TAX ID/ Registration ID etc.\n"
            "4. Provide only the JSON object in your response, with no explanations, reasoning, or additional content outside of these rules.\n"
            "5. Check if the URL has the company's name. If the website is the official website for that company or any sub-page within the companies website, you could factor that in since it suggests factual existence."
        )
    }]

    if country == "not found":
        message_text.append({
            "role": "user",
            "content": (
                f"Please analyze the following news article and determine if the company '{name}' has a social presence."
                f"The article is as follows: {article}"
                f"URL link to assess: {url}"
            )
        })

    else:
        message_text.append({
            "role": "user",
            "content": (
                f"Please analyze the following news article and determine if the company '{name}' from {country} has a social presence."
                f"The article is as follows: {article}"
                f"URL link to assess: {url}"
            )
        })

    try:
        response = client.chat.completions.create(
            model=model,  
            # response_format={"type":"json_object"},
            messages=message_text,
            temperature=0.2  
        )

        response_content = response.choices[0].message.content.strip()
        result = eval(response_content)
        if isinstance(result, dict) and all(key in result for key in ["country", "company", "verified"]):

            token_usage = {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens
            }

            return result, token_usage
        else:
            raise "Invalid Response Format"
        
    except Exception as e:
        return {"error": str(e)}
        