from utils import *

def main():
    
    stop_event = threading.Event()
    rabbitmq_parameters = pika.ConnectionParameters('localhost')
    scraper = BinanceDataScraper(rabbitmq_parameters=rabbitmq_parameters)

    scraper_thread = threading.Thread(target=scraper.run, args=(stop_event,))
    scraper_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        stop_event.set()
        scraper_thread.join()

    

if __name__ == "__main__":
    main()