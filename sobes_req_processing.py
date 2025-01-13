import pandas as pd
import requests

def process_sobes_data(df, vykup_statuses, df_sobes):
    df_2 = df_sobes[['externalId', 'purchasePrice']].dropna(subset=['externalId'])

    df_3 = df_2.copy()
    df_3['externalId'] = df_3['externalId'].apply(lambda x: '-'.join(x.split('-')[:3]))
    df_3 = df_3.rename(columns={'purchasePrice': 'Себес (OID) из СРМ'})
    df_3 = df_3.drop_duplicates(subset='externalId')
    df_3.rename(columns={'externalId': 'offer_id(товара)'}, inplace=True)

    mask_delivery = df['Назва товару'].str.contains('оставка', case=False, na=False)
    df_total_in_order = df[~mask_delivery]

    df_total_in_order = df_total_in_order[df_total_in_order['Статус'].isin(vykup_statuses)]
    df_total_in_order = df_total_in_order.merge(df_2, left_on='Product_id', right_on='externalId', how='left')
    df_total_in_order = df_total_in_order.rename(columns={'purchasePrice': 'sebes'})

    # Group and calculate totals
    df_total_in_order = df_total_in_order.groupby('offer_id(заказа)').agg({
        'Загальна сума': 'sum',
        'sebes': 'sum',
    }).reset_index()

    df_total_in_order = df_total_in_order.rename(columns={
        'Загальна сума': 'Выручка по всем товарам без доставки (все товары)',
        'sebes': 'Себес товаров',
        'offer_id(заказа)': 'offer_id(товара)'
    })
    df_total_in_order['Выручка по всем товарам без доставки (все товары)'] *= 1000

    return df_total_in_order, df_3

def fetch_sobes_data_from_api():
    url = 'https://uzshopping.retailcrm.ru/api/v5/store/inventories'
    api_key = 'JS08oUkdygh4tyMotd28sjlZerMTEemr'
    params = {
        'apiKey': api_key,
        'limit': 100
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
        # print(page)
        if r.status_code == 200:
            result = r.json()
            if 'offers' in result:
                all_results.extend(result['offers'])
        else:
            print(f"Error fetching page {page}: {r.status_code} - {r.text}")

    return pd.json_normalize(all_results, errors='ignore')