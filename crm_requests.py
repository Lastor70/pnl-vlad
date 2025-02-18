import pandas as pd
import aiohttp
import asyncio
import logging
import requests

logging.basicConfig(level=logging.INFO)

# Константи
TIMEOUT_SECONDS = 60
MAX_CONCURRENT_REQUESTS = 10

# Семафор для обмеження одночасних запитів
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Функція для виконання запиту
async def req(session, url, params):
    async with session.get(url, params=params, timeout=TIMEOUT_SECONDS) as response:
        return await response.json()

# Функція для асинхронної обробки сторінок з тайм-аутом, ретраями і семафором
async def fetch_page(session, url, params, page, retries=5, initial_delay=1):
    """Асинхронний запит для отримання даних зі сторінки з семафором і ретраями."""
    params['page'] = page
    delay = initial_delay
    for attempt in range(retries):
        async with semaphore:  # Обмеження одночасних запитів
            try:
                response = await req(session, url, params)
                if response['success']:
                    return response
            except Exception as e:
                logging.error(f"Error fetching page {page}: {e}")
        
        await asyncio.sleep(delay)
        delay = min(delay * 2, 10)  # Експоненційна затримка до 10 секунд

    logging.warning(f"Failed to fetch page {page} after {retries} attempts")
    return None

# Головна функція для збору всіх сторінок із API
async def gather_orders(api_key, start_date, end_date, request_type, batch_size=10):
    total_pages, url, params = fetch_orders_params(api_key, start_date, end_date, request_type)

    df = pd.DataFrame(range(1, total_pages + 1), columns=['p']).assign(result=None)

    async with aiohttp.ClientSession() as session:
        for batch_start in range(0, total_pages, batch_size):
            batch_end = min(batch_start + batch_size, total_pages)
            tasks = [
                fetch_page(session, url, params, page)
                for page in range(batch_start + 1, batch_end + 1)
            ]

            results = await asyncio.gather(*tasks)
            for idx, result in enumerate(results):
                df.at[batch_start + idx, 'result'] = [result] if result else None

            logging.info(f"Processed pages {batch_start + 1} to {batch_end}")

            # Затримка на 1 секунду для обмеження 10 запитів в секунду
            await asyncio.sleep(1)

    # Об'єднання всіх результатів у один DataFrame
    all_orders = []
    for result_list in df['result']:
        if result_list and isinstance(result_list, list):
            result = result_list[0]
            if 'success' in result and result['success']:
                all_orders.extend(result['orders'])

    return pd.DataFrame(all_orders)

# Функція для запуску асинхронного процесу
def get_orders(api_key, start_date, end_date, request_type):
    return asyncio.run(gather_orders(api_key, start_date, end_date, request_type))

# Функція для отримання параметрів запиту та кількості сторінок
def fetch_orders_params(api_key, start_date, end_date, request_type):
    url = 'https://uzshopping.retailcrm.ru/api/v5/orders'

    if request_type == 'main':
        params = {
            'apiKey': api_key,
            'filter[createdAtFrom]': start_date,
            'filter[createdAtTo]': end_date,
            'limit': 100,
        }
    else:
        params = {
            'apiKey': api_key,
            'filter[statusUpdatedAtFrom]': start_date,
            'filter[statusUpdatedAtTo]': end_date,
            'limit': 100,
        }

    r = requests.get(url, params=params)
    if r.status_code != 200:
        raise Exception(f"Error fetching orders: {r.json().get('error', 'Unknown error')}")

    total_pages = r.json()['pagination']['totalPageCount']
    logging.info(f"Total pages: {total_pages}")  
    return total_pages, url, params
