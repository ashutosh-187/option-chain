from pymongo import MongoClient

connection = MongoClient("mongodb://localhost:27017/")
db = connection['option_chain']
collection = db['master_data']

def process_live_market_data(live_market_data):
    live_market_data_dict = {}
    for data in live_market_data:
        if ':' in data:
            key, value = data.split(':', 1)
            mapping = {
                'ltp': 'last_traded_price',
                't': "exchange_instrument_id"
            }
            if key in mapping:
                live_market_data_dict[mapping[key]] = value
    return live_market_data_dict

def get_latest_expiry_data(strike_price, name, series):
    query = {
        "Name": name,
        "Series": series,
        "StrikePrice": strike_price
    }
    result = list(collection.find(query))
    if result:
        latest_expiry = max(result, key=lambda x: x['ContractExpiration'])
        return latest_expiry
    return []

def identify_option_type(contract):
    query = {
        "Name": contract.get('Name'),
        "Series": contract.get("Series"),
        "StrikePrice": contract.get("StrikePrice"),
        "ContractExpiration": contract.get("ContractExpiration")
    }
    query_result = list(collection.find(query))
    for contract in query_result:
        if contract.get("OptionType") == "3":
            call_contract = contract
        elif contract.get("OptionType") == "4":
            put_contract = contract
    option_contract = {
        "call": {
            "ExchangeInstrumentID": call_contract.get("ExchangeInstrumentID"),
            "Description": call_contract.get("Description"),
            "Series": call_contract.get("Series"),
            "ContractExpiration": call_contract.get("ContractExpiration"),
            "StrikePrice": call_contract.get("StrikePrice"),
            "OptionType": call_contract.get("OptionType")
        },
        "put": {
            "ExchangeInstrumentID": put_contract.get("ExchangeInstrumentID"),
            "Description": put_contract.get("Description"),
            "Series": put_contract.get("Series"),
            "ContractExpiration": put_contract.get("ContractExpiration"),
            "StrikePrice": put_contract.get("StrikePrice"),
            "OptionType": put_contract.get("OptionType")
        }
    }
    return option_contract

def prepare_subscription_list(contracts, exchange_segment):
    subscription_list = []
    for contract in contracts:
        subscription_list.append(
            {
                "exchangeSegment": exchange_segment, 
                "exchangeInstrumentID": contract.get("ExchangeInstrumentID")  
            }
        )
    return subscription_list

if __name__ == "__main__":
    prepare_subscription_list()