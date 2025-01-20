import requests
import pandas as pd

def fetch_stocks():
    url = 'https://uzshopping.retailcrm.ru/api/v5/store/offers'
    api_key = '2UzU5byg8DgspnSNahOdKaaTGyB7elQ6'
    params = {
        'apiKey': api_key,
        'site': 'vlad_ss',
        'limit': 100  # Додаємо ліміт для пагінації
    }

    # Початковий API запит для отримання кількості сторінок
    r = requests.get(url, params=params)
    if r.status_code != 200:
        raise Exception(f"Error fetching data: {r.status_code} - {r.text}")

    total_pages = r.json()['pagination']['totalPageCount']

    # Синхронне отримання всіх даних з усіх сторінок
    all_results = []
    for page in range(1, total_pages + 1):
        params['page'] = page
        r = requests.get(url, params=params)
        if r.status_code == 200:
            result = r.json()
            if 'offers' in result:
                all_results.extend(result['offers'])
        else:
            print(f"Error fetching page {page}: {r.status_code} - {r.text}")

    # Нормалізація даних у DataFrame
    df = pd.json_normalize(all_results, errors='ignore')

    # Вибірка потрібних стовпців
    result = df[['article', 'quantity']]

    # Фільтрація за шаблоном
    pattern = r'^[a-zA-Z]{2}-[a-zA-Z]{2}-\d{4}$'
    result = result[result['article'].str.match(pattern, na=False)]

    # Групування та підсумок
    stocks = result.groupby('article').sum().reset_index()

    return stocks