import redis
import time
import csv

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

def parse_pairs(start, middle, end):
    """Construct the pair symbols."""
    return start + middle, middle + end, start + end

def fetch_and_filter_paths(csv_path):
    """Fetch the prices and filter out paths with any missing data."""
    valid_paths = [] 

    with open(csv_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            start, middle, end = row['Start'], row['Middle'], row['End']
            pair1, pair2, pair3 = parse_pairs(start, middle, end)

            prices = {
                'ask1': redis_client.hget(pair1, 'ask'),
                'bid1': redis_client.hget(pair1, 'bid'),
                'ask2': redis_client.hget(pair2, 'ask'),
                'bid2': redis_client.hget(pair2, 'bid'),
                'ask3': redis_client.hget(pair3, 'ask'),
                'bid3': redis_client.hget(pair3, 'bid')
            }

            # If any price data is missing, skip this path
            if all(prices.values()):
                valid_paths.append({
                    'path': (start, middle, end),
                    'prices': {k: float(v) for k, v in prices.items() if v is not None}
                })

    return valid_paths

def check_arbitrage_opportunity(path_data):
    """Check for an arbitrage opportunity in the given path."""
    start, middle, end = path_data['path']
    prices = path_data['prices']

    # Check the arbitrage condition for the ask prices
    if prices['ask1'] * prices['ask2'] * (1 / prices['ask3']) < 1:
        print(f"Arbitrage opportunity detected in ask prices for {start}{middle}:{prices['ask1']}, {middle}{end}:{prices['ask2']}, {start}{end}:{prices['ask3']} ==> {(prices['ask1']*prices['ask2']*1/prices['ask3'])}")
        return True

    # Check the arbitrage condition for the bid prices
    if prices['bid1'] * prices['bid2'] * (1 / prices['bid3']) > 1:
        print(f"Arbitrage opportunity detected in bid prices for {start}{middle}:{prices['bid1']}, {middle}{end}:{prices['bid2']}, {start}{end}:{prices['bid3']} ==> {(prices['bid1']*prices['bid2']*1/prices['bid3'])}")
        return True

    return False

def main(pairs_path):
    try:
        while True:
            valid_paths = fetch_and_filter_paths(pairs_path)
            arbitrage_found = any(check_arbitrage_opportunity(path) for path in valid_paths)
            
            if arbitrage_found:
                print("Arbitrage detected!")
            else:
                print("No arbitrage opportunity at this time.")

            time.sleep(1)  
    except KeyboardInterrupt:
        print('Interrupted by the user')
    finally:
        print('Stopping arbitrage detection.')

if __name__ == '__main__':
    pairs_path = "paths.csv"  
    main(pairs_path)
