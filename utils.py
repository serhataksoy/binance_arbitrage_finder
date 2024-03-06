import threading
import time
import websocket
import json
import requests
import pika
from queue import Queue
import pandas as pd

class BinanceDataScraper:
    def __init__(self, rabbitmq_parameters):
        self.base_url = "https://api.binance.com"
        self.symbols = []
        self.rabbitmq_parameters = rabbitmq_parameters
        self.message_queue = Queue()
        self.start_rabbitmq_publisher_thread()

    def start_rabbitmq_publisher_thread(self):
        def publisher():
            connection = pika.BlockingConnection(self.rabbitmq_parameters) #I thought for simplicity of problem BlockingConnection would be enough instead of SelectConnection
            channel = connection.channel()
            channel.queue_declare(queue='market_data')

            while True:
                message = self.message_queue.get()
                if message is None: 
                    break
                channel.basic_publish(exchange='',
                                      routing_key='market_data',
                                      body=json.dumps(message))
                print(f"Published to RabbitMQ: {message}")
                self.message_queue.task_done()
            connection.close()

        self.publisher_thread = threading.Thread(target=publisher)
        self.publisher_thread.start()

    def fetch_trading_pairs(self):
        """
        Getting trading pairs data from the Binance API.
        """
        url = f"{self.base_url}/api/v3/ticker/24hr"
        response = requests.get(url)
        data = response.json()
        return data

    def get_low_liquidity_pairs(self, limit=497):
        """
        Filters and returns trading pairs with low liquidity.
        """
        data = self.fetch_trading_pairs()
        filtered_pairs = [pair for pair in data if float(pair['quoteVolume']) > 0]
        sorted_pairs = sorted(filtered_pairs, key=lambda x: float(x['quoteVolume']))
        return sorted_pairs[:limit]

    def update_symbols(self):
        """
        Updates the symbols list with low liquidity pairs and additional specific pairs.
        """
        low_liquidity_pairs = self.get_low_liquidity_pairs()
        additional_pairs = ['BTCUSDT', 'ETHBTC', 'BNBUSDT']
        for pair in additional_pairs:
            if pair not in [p['symbol'] for p in low_liquidity_pairs]:
                low_liquidity_pairs.append({'symbol': pair})
        
        self.symbols = [pair['symbol'] for pair in low_liquidity_pairs]

        return self.symbols

    def on_message(self, ws, message):
        json_message = json.loads(message)
        symbol = json_message['data']['s']
        bid_price = float(json_message['data']['b'])
        ask_price = float(json_message['data']['a'])
        timestamp = time.time()
        message = {'symbol': symbol, 'bid': bid_price, 'ask': ask_price, 'timestamp': timestamp}
        self.message_queue.put(message)  # Enqueue the message instead of directly publishing

    def on_error(self, error):
        print("Error occurred:", error)

    def on_close(self, close_status_code, close_msg):
        print("### Connection Closed ###")

    def on_open(self, ws, symbols_chunk):
        print('Connection opened')
    
    def create_ws_connection(self, symbols_chunk, stop_event):
        stream_names = [f"{symbol.lower()}@bookTicker" for symbol in symbols_chunk]
        websocket_url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(stream_names)}"

        ws = websocket.WebSocketApp(websocket_url,
                                    on_open=lambda ws: self.on_open(ws, symbols_chunk),
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

        ws_thread = threading.Thread(target=lambda: ws.run_forever())
        ws_thread.start()
        ws_thread.join()

        stop_event.wait()

        ws.close()
        ws_thread.join()

    def chunk_symbols(self, symbols_list, chunk_size=10):
        for i in range(0, len(symbols_list), chunk_size):
            yield symbols_list[i:i + chunk_size]

    def run(self, stop_event):
        self.symbols = self.update_symbols()  
        threads = []

        for symbols_chunk in self.chunk_symbols(self.symbols, 10):
            thread = threading.Thread(target=self.create_ws_connection, args=(symbols_chunk, stop_event))
            thread.start()
            threads.append(thread)

        stop_event.wait()

        for thread in threads:
            thread.join()
        self.message_queue.put(None)  
        self.publisher_thread.join()

def is_valid_pair(pair, inverse_pair, pairs):
    """Check if the pair or its inverse exists."""
    return pair in pairs or inverse_pair in pairs

def find_triangular_arbitrage_paths(pairs, tickers):
    currencies = set(tickers)
    paths = []

    for start in currencies:
        for middle in currencies - {start}:
            for end in currencies - {start, middle}:
                # Direct paths
                direct_path = f"{start}{middle}", f"{middle}{end}", f"{end}{start}"
                # Inverse paths
                inverse_path = f"{middle}{start}", f"{end}{middle}", f"{start}{end}"

                if is_valid_pair(direct_path[0], inverse_path[0], pairs) and \
                   is_valid_pair(direct_path[1], inverse_path[1], pairs) and \
                   is_valid_pair(direct_path[2], inverse_path[2], pairs):
                    paths.append([start, middle, end])

    # Create DataFrame
    df_paths = pd.DataFrame(paths, columns=['Start', 'Middle', 'End'])
    return df_paths 

#df_triangular_paths = find_triangular_arbitrage_paths(pairs, tickers)