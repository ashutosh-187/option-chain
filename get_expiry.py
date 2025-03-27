import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

def load_access_token():
    with open('access_token.json', 'r') as file:
        data = json.load(file)
        return data['access_token']
    
def get_expiry():
    token = load_access_token()
    headers = {
            "Content-Type": "application/json",
            "Authorization": token 
    }
    expiry_url = os.getenv("xts_url") + "/instruments/instrument/expiryDate"
    params = {
        "exchangeSegment": 2,
        "series": "OPTIDX",
        "symbol": "NIFTY"
    }
    expiry_response = requests.get(expiry_url, headers=headers, params=params)
    expiry_json = expiry_response.json()
    print(expiry_json)

if __name__ =="__main__":
    get_expiry()