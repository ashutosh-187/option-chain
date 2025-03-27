import os
from dotenv import load_dotenv
import requests
import json
from pymongo import MongoClient

connection = MongoClient("mongodb://localhost:27017/")
db = connection['option_chain']
collection = db['master_data']

load_dotenv()

fno_header = 'ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|LotSize|Multiplier|UnderlyingInstrumentId|UnderlyingIndexName|ContractExpiration|StrikePrice|OptionType|DisplayName|PriceNumerator|PriceDenominator|DetailedDescription'.split('|')
eq_header = 'ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|LotSize|Multiplier|DisplayName|ISIN|PriceNumerator|PriceDenominator|DetailedDescription'.split('|')

def load_access_token():
    with open('access_token.json', 'r') as file:
        data = json.load(file)
        return data['access_token']

def load_access_token():
    with open('access_token.json', 'r') as file:
        data = json.load(file)
        return data['access_token']
    
def parse_fno_data(json_string):
    data_dict = json.loads(json_string)
    fno_header = 'ExchangeSegment|ExchangeInstrumentID|InstrumentType|Name|Description|Series|NameWithSeries|InstrumentID|PriceBand.High|PriceBand.Low|FreezeQty|TickSize|LotSize|Multiplier|UnderlyingInstrumentId|UnderlyingIndexName|ContractExpiration|StrikePrice|OptionType|DisplayName|PriceNumerator|PriceDenominator|DetailedDescription'
    headers = fno_header.split('|')
    result_lines = data_dict['result'].strip().split('\n')
    parsed_results = []
    for line in result_lines:
        values = line.split('|')
        line_dict = {}
        for i, header in enumerate(headers):
            if i < len(values):
                line_dict[header] = values[i]
        parsed_results.append(line_dict)
    return parsed_results

def master_call(collection):
    token = load_access_token()
    exchange_segments = {
        "exchangeSegmentList": ["NSEFO", "BSEFO"]
    }
    headers = {
            "Content-Type": "application/json",
            "Authorization": token 
    }
    master_url = os.getenv("xts_url")+"/instruments/master"
    master_response = requests.post(master_url, json=exchange_segments)
    master_dict = parse_fno_data(master_response.text)
    with open('master_response.json', 'w') as outfile:
        json.dump(master_dict, outfile)
    collection.insert_many(master_dict)
    print(":)")

def fetch_master_file_data(name, Series):
    query = {"Name": name, "Series": Series}
    result = collection.find(query).sort("ContractExpiration", -1).limit(1)
    for document in result:
        filtered_data = {
            "ExchangeSegment": document.get("ExchangeSegment"),
            "Name": document.get("Name"),
            "Description": document.get("Description"),
            "Series": document.get("Series"),
            "ContractExpiration": document.get("ContractExpiration"),
            "StrikePrice": document.get("StrikePrice")
        }
    return filtered_data

if __name__ == "__main__":
    master_call(collection)
