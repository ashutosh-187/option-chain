import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

def load_access_token():
    with open('access_token.json', 'r') as file:
        data = json.load(file)
        return data['access_token']

def subscribe_to_multiple_symbols(token, subscription_list):
        body = {
            "instruments": subscription_list,
            "xtsMessageCode": 1501
        }
        url = os.getenv("xts_url")+"/instruments/subscription"
        headers = {
            "Content-Type": "application/json",
            "Authorization": token 
        }
        response = requests.post(url, json=body, headers=headers)
        return response.text

if __name__ == "__main__":
    token = load_access_token()
    # subscribe_to_multiple_symbols(token)