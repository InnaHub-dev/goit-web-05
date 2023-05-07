import aiohttp
import asyncio
import argparse
import functools
import logging
import time
import pytz
from datetime import datetime, timedelta
from typing import Callable, Any

CURRENCIES = ["USD", "EUR"]
API = "https://api.privatbank.ua/p24api/exchange_rates?json&date="

parser = argparse.ArgumentParser(
    prog="Exchange rates fetcher",
    description="Gets the exhange rates from PrivatBank",
)
parser.add_argument(
    "num_days",
    metavar="-d",
    nargs="?",
    type=int,
    default="1",
    help="period for which the exchange rate is shown (ending with the current day)",
)

args = parser.parse_args()
num_days = args.num_days


def time_decorator_async():
    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            start_time = time.time()
            result = await func(*args, **kwargs)
            end_time = time.time()
            print(f"{func.__name__} took {end_time - start_time} seconds to run")
            return result

        return wrapped

    return wrapper


def generate_dates(num_days):
    time_zone_kyiv = pytz.timezone("Europe/Kyiv")
    time_kyiv = datetime.now(time_zone_kyiv)
    date_list = []
    for i in range(num_days):
        date_list.append(time_kyiv.strftime("%d.%m.%Y"))
        time_kyiv -= timedelta(days=1)
    print(date_list)
    return date_list


def create_rates_dict(date_list):
    rates = {}
    for date in date_list:
        rates.update({date: {}})
    return rates


def generate_urls(date_list, api):
    apis = []
    for day in date_list:
        apis.append(api + day)
    return apis


@time_decorator_async()
async def fetch_request(session, url):
    async with session.get(url) as response:
        try:
            logging.info("Getting json")
            if response.status == 200:
                result = await response.json()
                return result
            else:
                logging.error(f"Error status: {response.status} for {url}")
                return None
        except aiohttp.ClientConnectionError as e:
            logging.error(f"Connection error {url}: {e}")
        except aiohttp.ContentTypeError as e:
            logging.error(f"Wrong content type, couldn't decode {url}: {e}")
        except aiohttp.ClientResponseError as e:
            logging.error(f"Error status {e.status} for {url}", {e})


def get_list_indexes(CURRENCIES, entry):
    """This function finds the list indexes of the required currencies, so that the program doesn't have to loop through the entire list every time."""
    indexes = {}
    for currency in CURRENCIES:
        for idx, list_item in enumerate(entry.get("exchangeRate")):
            if list_item.get("currency") == currency:
                indexes.update({currency: idx})
    return indexes


def update_rates(date, currency, index, rates, exchange_rate_list) -> None:
    rates[date].update(
        {
            currency: {
                "sale": exchange_rate_list[index]["saleRate"],
                "purchase": exchange_rate_list[index]["purchaseRate"],
            }
        }
    )


@time_decorator_async()
async def get_json_producer(urls: list, queue):
    async with aiohttp.ClientSession() as session:
        for url in urls:
            r = await fetch_request(session, url)
            if r:
                await queue.put(r)
                logging.info("task was put into queue")
    print("producer finished")


@time_decorator_async()
async def parse_json_consumer(rates, queue):
    indexes = 0
    while True:
        print("fetching new entry...")
        entry = await queue.get()
        exchange_rate_list = entry.get("exchangeRate")
        date = entry.get("date")
        if indexes == 0:
            indexes = get_list_indexes(CURRENCIES, entry)
        for currency, index in indexes.items():
            update_rates(date, currency, index, rates, exchange_rate_list)
        queue.task_done()
        logging.info(f"task was processed")


@time_decorator_async()
async def main():
    date_list = generate_dates(num_days)
    rates_dict = create_rates_dict(date_list)
    urls = generate_urls(date_list, API)

    queue = asyncio.Queue()

    consumers = []
    for _ in range(3):
        consumer = asyncio.create_task(parse_json_consumer(rates_dict, queue))
        consumers.append(consumer)
    await get_json_producer(urls, queue)
    await queue.join()

    for c in consumers:
        c.cancel()

    return rates_dict


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler("mylog.log"), logging.StreamHandler()],
    )

    logger = logging.getLogger(__name__)
    rates = asyncio.run(main())
    print(rates)
