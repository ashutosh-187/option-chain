import json
import os
from dotenv import load_dotenv
import socketio
from subscribtion import subscribe_to_multiple_symbols
from generate_access_token import access_token
from helper import process_live_market_data
from queue import Queue

load_dotenv()
sio = socketio.Client()
market_data_queue = Queue(maxsize=1)
option_chain_queue = Queue()

def load_access_token():
    with open('access_token.json', 'r') as file:
        data = json.load(file)
        return data['access_token']

def connect_to_market_data(sub_list):
        access_token()
        token = load_access_token()
        userID = os.getenv("userID")
        publishFormat = "JSON"
        broadcastFormat = "Partial"
        url = os.getenv("xts_url")
        socket_connection_url = f"{url}/?token={token}&userID={userID}&publishFormat={publishFormat}&broadcastMode={broadcastFormat}"
        
        @sio.event
        def connect_error(data):
            print('Connection failed:', data)

        @sio.event
        def disconnect():
            print('Disconnected from websocket')

        @sio.on('1501-json-partial')
        def on_message1501_json_partial(data):
            if isinstance(data, str):
                live_market_data = data.split(",")
                processed_data = process_live_market_data(live_market_data)
                last_traded_price = processed_data.get("last_traded_price")
                if last_traded_price:
                    if market_data_queue.empty():
                        market_data_queue.put(last_traded_price)
                option_chain_queue.put(processed_data)
                # print(processed_data)


        @sio.on('connect')
        def connect(data=None):
            print("connected with websocket.")
            token = load_access_token()
            # print(sub_list)
            subscribe_to_multiple_symbols(token, sub_list)

        sio.connect(
            socket_connection_url,
            headers = {},
            namespaces=None,
            transports='websocket',
            socketio_path='/apimarketdata/socket.io'
        )
        sio.wait()

def disconnect_from_market_data():
    try:
        if sio.connected:
            sio.disconnect()
    except Exception as e:
        print(f"Error disconnecting from websocket: {e}")


def last_traded_price_queue():
    return market_data_queue

def get_option_chain_queue():
    return option_chain_queue

if __name__ == "__main__":
    list_187 = [
        {
            "exchangeSegment": 2, 
            "exchangeInstrumentID": 51119
        }
    ]
    connect_to_market_data(list_187)
    # exchange_segment=1
# exchange_instrument_id=26001