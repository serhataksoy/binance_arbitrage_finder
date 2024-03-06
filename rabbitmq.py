import pika
import json
from utils import *
import redis
from math import floor

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def get_binance_symbols():
    url = "https://api.binance.com/api/v1/exchangeInfo"
    tickers = set()
    response = requests.get(url)
    data = response.json()
    symbols = data['symbols']
    for symbol in symbols:
        tickers.add(symbol['baseAsset'])
        tickers.add(symbol['quoteAsset'])
    return list(tickers)

tickers = get_binance_symbols()

def store_in_redis(symbol, bid_price, ask_price, timestamp):
    '''
    Sent data to redis
    '''
    redis_client.hset(symbol, mapping={'bid': bid_price, 'ask': ask_price, 'timestamp': timestamp})
    redis_client.expire(symbol, 1) #There is no need to store old data.

def callback(ch, method, properties, body):
    data = json.loads(body)
    print("Received market data:", data)
    symbol = data['symbol']
    bid_price = data['bid']
    ask_price = data['ask']
    timestamp = data['timestamp']

    inverse_symbol = parse_and_invert_symbol(symbol, tickers) 
    bid_price_inverse = 1/bid_price
    ask_price_inverse = 1/ask_price


    # Store new bid and ask in Redis
    store_in_redis(symbol, bid_price, ask_price, floor(timestamp))
    store_in_redis(inverse_symbol, bid_price_inverse, ask_price_inverse, floor(timestamp))

def parse_and_invert_symbol(symbol, known_currencies):
    '''
    price of AB = 1/price of BA
    '''
    for currency in known_currencies:
        if symbol.endswith(currency):
            base_currency = symbol[:-len(currency)]
            inverse_symbol = currency + base_currency
            return inverse_symbol
    
def main():

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='market_data')

    channel.basic_consume(queue='market_data',
                          auto_ack=True,
                          on_message_callback=callback)

    print('Waiting for market data. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print('Interrupted')
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == '__main__':
    main()