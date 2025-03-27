from master_file import fetch_master_file_data
from get_index import get_indexes
from web_socket import (
    connect_to_market_data,
    last_traded_price_queue,
    disconnect_from_market_data,
    get_option_chain_queue
    )
from helper import (
    get_latest_expiry_data,
    identify_option_type,
    prepare_subscription_list,
    )
import threading
import json

def load_access_token():
    with open('access_token.json', 'r') as file:
        data = json.load(file)
        return data['access_token']
    
def option_chain(name, series):
    data_from_master_file = fetch_master_file_data(name=name, Series=series)
    if data_from_master_file.get("Name") == "NIFTY" or data_from_master_file.get("Name") ==  "BANKNIFTY":
        exchange_segment = 1
        index = get_indexes(exchange_segment)
        if data_from_master_file.get("Name") == "NIFTY":
            print("Option chain for NIFTY")
            instrument_id = index.get("NIFTY")
        elif data_from_master_file.get("Name") == "BANKNIFTY":
            print("Option chain for BANK NIFTY")
            instrument_id = index.get("NIFTY_BANK")
        websocket_thread = threading.Thread(
            target=connect_to_market_data,
            args=([
            {
                "exchangeSegment": exchange_segment, 
                "exchangeInstrumentID": instrument_id
            }
            ],)
        )
        websocket_thread.daemon = True
        websocket_thread.start()
        price_queue = last_traded_price_queue()
        last_traded_price = price_queue.get()
        if websocket_thread.is_alive():
            print("Stopping websocket thread...")
            websocket_thread.join(timeout=1)
        disconnect_from_market_data()
        strike_distance = 50
        atm = round(float(last_traded_price)/strike_distance)*strike_distance
        print("last_traded_price =>", last_traded_price)
        print("ATM =>", atm)
        strike_prices = []
        strike_prices = [atm + (i * strike_distance) for i in range(-5, 6)]
        print(strike_prices)
        call_options = []
        put_options = []
        for strike in strike_prices:
            print("**********STRIKE*************")
            print(strike)
            latest_contract = get_latest_expiry_data(strike_price=str(strike), name=name, series=series)
            if len(latest_contract) == 0:
                print(f"No contract found in the database with strike price of {strike}.")
                continue
            option_contract = identify_option_type(latest_contract)
            call_options.append(option_contract.get("call"))
            put_options.append(option_contract.get("put"))
            call_option_subscription_list = prepare_subscription_list(call_options, exchange_segment=2)
            put_option_subscription_list = prepare_subscription_list(put_options, exchange_segment=2)
        subscribe = call_option_subscription_list + put_option_subscription_list
        connect_to_market_data(subscribe)
    elif data_from_master_file.get("Name") == "SENSEX":
        exchange_segment = 11
        index = get_indexes(exchange_segment)
        if data_from_master_file.get("Name") == "SENSEX":
            print("Option chain for SENSEX")
            instrument_id = index.get("SENSEX")
        websocket_thread = threading.Thread(
            target=connect_to_market_data,
            args=([
            {
                "exchangeSegment": exchange_segment, 
                "exchangeInstrumentID": instrument_id
            }
            ],)
        )
        websocket_thread.daemon = True
        websocket_thread.start()
        price_queue = last_traded_price_queue()
        last_traded_price = price_queue.get()
        if websocket_thread.is_alive():
            print("Stopping websocket thread...")
            websocket_thread.join(timeout=1)
        disconnect_from_market_data()
        strike_distance = 100
        atm = round(float(last_traded_price)/strike_distance)*strike_distance
        print("last_traded_price =>", last_traded_price)
        print("ATM =>", atm)
        strike_prices = []
        strike_prices = [atm + (i * strike_distance) for i in range(-5, 6)]
        print(strike_prices)
        call_options = []
        put_options = []
        for strike in strike_prices:
            print("**********STRIKE*************")
            print(strike)
            latest_contract = get_latest_expiry_data(strike_price=str(strike), name=name, series=series)
            if len(latest_contract) == 0:
                print(f"No contract found in the database with strike price of {strike}.")
                continue
            option_contract = identify_option_type(latest_contract)
            call_options.append(option_contract.get("call"))
            put_options.append(option_contract.get("put"))
            call_option_subscription_list = prepare_subscription_list(call_options, exchange_segment=12)
            put_option_subscription_list = prepare_subscription_list(put_options, exchange_segment=12)
        subscribe = call_option_subscription_list + put_option_subscription_list
        connect_to_market_data(subscribe)
        print("HELLO")
        option_chain = get_option_chain_queue()
        while not option_chain.empty():
            data = option_chain.get()
            print(data)

if __name__ == "__main__":
    # option_chain(name="SENSEX", series="IO") # 3 CE
    option_chain(name="NIFTY", series="OPTIDX") # 3 CE
    # option_chain(name="BANKNIFTY", series="OPTIDX") # 3 CE