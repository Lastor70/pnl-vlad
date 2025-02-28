import pandas as pd
import re
from caching import fetch_stocks_cache, get_sobes_data
from sobes_req_processing import process_sobes_data
import numpy as np
import openpyxl

from stocks_processing import fetch_stocks

def add_columns(df):
    df_grouped = df.groupby(['Номер замовлення', 'offer_id(заказа)']).agg({'Загальна сума': 'sum',
                                                                          'Кількість товару': 'sum',
                                                                          'Назва товару': 'first',
                                                                          # 'Ціна товару': 'first',
                                                                          }).reset_index()
    return df_grouped


def add_match_column(df, tovar_id, zakaz_id):
    df.reset_index(drop=True, inplace=True)
    df['Match'] = (df[tovar_id] == df[zakaz_id]).astype(int)

    return df

def get_lead(row, df_payment):
    for index, data in df_payment.iterrows():
        if row['Средний чек апрува без доставки'] <= data['Сумма по товарам(вкл.)']:
            return data['Лид до $']
    max_lead_value = df_payment['Лид до $'].iloc[-1]
    return max_lead_value if row['Средний чек апрува без доставки'] > df_payment['Сумма по товарам(вкл.)'].max() else None

def merge_all_data(leads, clear_leads, appruv, refund, vykup, vozvrat, in_delivery, ref_sum):
    merged_data = leads.merge(clear_leads, on='offer_id(заказа)', how='left') \
                      .merge(appruv, on='offer_id(заказа)', how='left') \
                      .merge(refund, on='offer_id(заказа)', how='left') \
                      .merge(vykup, on='offer_id(заказа)', how='left') \
                      .merge(vozvrat, on='offer_id(заказа)', how='left') \
                      .merge(in_delivery, on='offer_id(заказа)', how='left') \
                      .merge(ref_sum, on='offer_id(заказа)', how='left')
                   
    return merged_data

def count_unique_orders(df, column_name):
    unique_orders_counts = df.groupby('offer_id(заказа)')['Номер замовлення'].nunique().reset_index(name=column_name)
    return unique_orders_counts

def calculate_orders_w_dops(df,merged):
  df = df.dropna(subset=['Назва товару'])
  df = df[~df['Назва товару'].str.contains('оставка')]
  aggregated_df = df.groupby('Номер замовлення').agg({
      'call-center': lambda x: ','.join(x.dropna().astype(str)),
      'Match': lambda x: ','.join(x.astype(str)),
      'offer_id(заказа)': 'first'
  }).reset_index()

  aggregated_df['orders_with_dops_count'] = aggregated_df.apply(
      lambda row: 1 if '0' in row['Match'] or row['call-center'] else 0, axis=1
  )
  df_counts = aggregated_df.groupby('offer_id(заказа)').agg({
      'orders_with_dops_count': 'sum'
  }).reset_index()

  df_counts = pd.merge(df_counts, merged, on='offer_id(заказа)', how='left')

  df_counts['% заказов с допами в апрувах'] = (
      df_counts['orders_with_dops_count'] / df_counts['Кількість аппрувів'].replace(0, np.nan)
  )

  df_counts = df_counts.rename(columns={'orders_with_dops_count': 'Заказов с допами в апрувах'})
  df_counts = df_counts[['offer_id(заказа)', 'Заказов с допами в апрувах', '% заказов с допами в апрувах']]
#   print(df_counts)
  return df_counts


def get_appruv_coefficient(approval_percent, df_appruv_range):
    df_appruv_range['Threshold'] = df_appruv_range['Диапазон апрува'].str.extract(r'(\d+)', expand=False).astype(float)

    sorted_df = df_appruv_range.sort_values(by='Threshold', ascending=False)

    for threshold, coefficient in zip(sorted_df['Threshold'], sorted_df['Бонус/Вычет от чистой выплаты']):
        if approval_percent >= threshold: 
            return coefficient

    return None

def process_orders_data(df, api_key, df_payment, df_appruv_range, df_grouped):
    """Обробляє отримані замовлення та форматує DataFrame."""
    
    mask = ['number', 'status', 'createdAt', 'customFields', 'items']
    df2 = df[mask]

    def get_item_data(items, key):
        data = []
        for item in items:
            if isinstance(item, dict) and 'offer' in item and key in item['offer']:
                data.append(item['offer'][key])
            else:
                data.append(None)
        return data

    df_items_expanded = df2.explode('items')

    df_items_expanded['price'] = df_items_expanded['items'].apply(lambda x: x['prices'][0]['price'] if isinstance(x, dict) and 'prices' in x and x['prices'] else None)
    df_items_expanded['quantity'] = df_items_expanded['items'].apply(lambda x: x['prices'][0]['quantity'] if isinstance(x, dict) and 'prices' in x and x['prices'] else None)
    df_items_expanded['externalId'] = df_items_expanded['items'].apply(lambda x: get_item_data([x], 'externalId')[0] if isinstance(x, dict) else None)
    df_items_expanded['comment'] = df_items_expanded['items'].apply(lambda x: x.get('comment') if isinstance(x, dict) else None)
    df_items_expanded['name'] = df_items_expanded['items'].apply(lambda x: x['offer']['name'] if isinstance(x, dict) and 'offer' in x and 'name' in x['offer'] else None)
    df_items_expanded['item_buyer_id'] = df_items_expanded.apply(lambda x: x['customFields']['buyer_id'] if 'buyer_id' in x['customFields'] else None, axis=1)
    df_items_expanded['item_offer_id'] = df_items_expanded.apply(lambda x: x['customFields']['offer_id'] if 'offer_id' in x['customFields'] else None, axis=1)
    df_items_expanded = df_items_expanded.rename(columns={
        'number': 'Номер замовлення',
        'status': 'Статус',
        'createdAt': 'Дата создания',
        'externalId': 'Product_id',
        'name': 'Назва товару',
        'quantity': 'Кількість товару',
        'price': 'Ціна товару',
        'item_offer_id': 'offer_id(заказа)',
        'item_buyer_id': 'buyer_id',
        'comment': 'call-center'
    })
    df_items_expanded.drop(['customFields', 'items'], axis = 1)
    df = df_items_expanded
    df['offer_id(товара)'] = df['Product_id'].apply(lambda x: '-'.join(x.split('-')[:3]) if isinstance(x, str) else None)
    df['Загальна сума'] = df['Ціна товару'] * df['Кількість товару']
    
    desired_column_order = ['Номер замовлення', 'Статус', 'offer_id(товара)', 'Product_id', 'Назва товару', 'Кількість товару', 'Ціна товару', 'Загальна сума', 'offer_id(заказа)', 'buyer_id','call-center']
    df = df.reindex(columns=desired_column_order)
    
    #додаємо other
    df['offer_id(заказа)'] = df.apply(
    lambda row: 'other' if pd.isna(row['offer_id(заказа)']) or not re.match(r'^[a-zA-Z]{2}-', str(row['offer_id(заказа)'])) else row['offer_id(заказа)'],
    axis=1)
    # print(df[df['offer_id(заказа)']=='other'])
    # статуси
    vykup_statuses = ['complete', 'payoff', 'dostavlen-predvaritelno','refund-req', 'refund-done', 'exchange', 'exchange-done']
    appruv_statuses = ['preorder',
    'urgent',
    'dop-prozvon',
    'call-emu',
    'approve-cod', 'approve-prepay', 'approve-prepay-done', 'podtverzhden-advert', 'podtverzhden-samovyvoz-yandex', 'otpravit-pozzhe', 'approve-muqimi',
    'send-to-assembling', 'assembling', 'client-confirmed', 'assembling-complete', 'pending', 'customer-wait', 'sborka',
    'send-to-delivery', 'delivering', 'redirect', 'pickup-ready', 'given', 'dop-prozvon-podtverzhden',
    'complete', 'payoff', 'dostavlen-predvaritelno',
    'return',
    'vozvrat-predvaritelno',
    'plan-vozvrat',
    'refund-req', 'refund-done', 'exchange', 'exchange-done']

    #себеси
    df_sobes = get_sobes_data(api_key)
    df_sobes_main, df_3 = process_sobes_data(df, vykup_statuses, df_sobes)


    dataset = add_match_column(df, 'offer_id(товара)', 'offer_id(заказа)')

    #тута всі закази без дублів та тестів      
    new = dataset[~dataset['Статус'].isin(['testy','duplicate'])]
    #тута табличка яка містить кількість лідів
    leads = count_unique_orders(new, 'Кількість лідів')
    #тута всі закази без треша, дублів, тестів. І по цьому рахуємо чисті ліди
    new = new[~new['Статус'].isin(['trash'])]
    #тута табличка яка містить кількість чистих лідів
    clear_leads = count_unique_orders(new, 'Кількість чистих лідів')
    #тута усі аппруви
    new = new[new['Статус'].isin(appruv_statuses)]
    #тута кількість аппрувів
    appruv = count_unique_orders(new, 'Кількість аппрувів')
    #refund
    ref_temp = dataset[dataset['Статус'].isin(['refund-req', 'refund-done', 'exchange', 'exchange-done'])]
    refund = count_unique_orders(ref_temp, 'Refund')

    ref_sum = ref_temp.groupby(['offer_id(заказа)']).agg({'Загальна сума': 'sum',
                                                                            # 'Назва товару': 'first',
                                                                            # 'Ціна товару': 'first',
                                                                            }).reset_index()

    ref_sum = ref_sum.rename(columns={'Загальна сума': 'Refund SUM'})

    #vykup
    vykup_temp = dataset[dataset['Статус'].isin(vykup_statuses)]
    vykup = count_unique_orders(vykup_temp, 'Выкуп')

    #возврат
    vozvrat_temp = dataset[dataset['Статус'].isin(['return','vozvrat-predvaritelno', 'plan-vozvrat'])]
    vozvrat = count_unique_orders(vozvrat_temp, 'Возврат')

    #доставляются
    in_delivery_temp = new[~new['Статус'].isin(['preorder','send-to-assembling', 'assembling', 'client-confirmed', 'assembling-complete', 'pending', 'customer-wait', 'sborka',])]
    in_delivery = count_unique_orders(in_delivery_temp, 'Доставляются')

    merged = merge_all_data(leads, clear_leads, appruv, refund, vykup, vozvrat, in_delivery, ref_sum)
    
    def process_prodano_main(df):
        df_count_offer_id = df[df['Match']==1].groupby(['offer_id(заказа)']).agg({'Кількість товару': 'sum'}).reset_index().rename(columns={'Кількість товару':'Продано товаров шт. (OID)'})
        df_count_all = df[df['Назва товару'].notna() & (~df['Назва товару'].str.contains('оставка', na=False))].groupby(['offer_id(заказа)']).agg({'Кількість товару': 'sum'}).reset_index().rename(columns={'Кількість товару':'Продано товаров всего'})
        df_sum_offer_id = df[df['Match']==1].groupby(['offer_id(заказа)']).agg({'Загальна сума': 'sum'}).reset_index().rename(columns={'Загальна сума':'Выручка по OID без доставки (от этого значения 5% баеру)'})
        df_sum_all = df[df['Назва товару'].notna() & (~df['Назва товару'].str.contains('оставка', na=False))].groupby(['offer_id(заказа)']).agg({'Загальна сума': 'sum'}).reset_index().rename(columns={'Загальна сума':'Выручка по всем товарам без доставки (все товары)'})
        df_sum_all_w_delivery = df.groupby(['offer_id(заказа)']).agg({'Загальна сума': 'sum'}).reset_index().rename(columns={'Загальна сума':'Итоговая выручка с дост. в СУМ'})
        return df_count_offer_id,df_count_all,df_sum_offer_id,df_sum_all,df_sum_all_w_delivery

    prodano_oid,prodano_all,profit_oid,profit_all,df_sum_all_w_delivery = process_prodano_main(vykup_temp)

    appruv_avg_sum = (
        new[
            (new['Назва товару'].notna() & ~new['Назва товару'].str.contains('оставка', na=False)) |
            (new['Назва товару'].isna())  # Додана умова через логічне "АБО"
        ]
        .groupby(['Номер замовлення'])  # Групуємо за номером замовлення
        .agg({'Загальна сума': 'sum'})  # Сума для кожного замовлення (без доставки)
        .reset_index()
        .merge(new[['Номер замовлення', 'offer_id(заказа)']], on='Номер замовлення', how='left')  # Додаємо offer_id(заказа)
        .drop_duplicates(subset='Номер замовлення')  # Видаляємо дублікати offer_id(заказа)

        .groupby('offer_id(заказа)')  # Групуємо за offer_id(заказа)
        .agg({'Загальна сума': 'mean'})  # Середнє значення загальних сум
        .reset_index()
        .rename(columns={'Загальна сума': 'Средний чек апрува без доставки'})
    )
    # print(new[new['offer_id(заказа)']=='tv-ss-0018'][['Загальна сума','Номер замовлення']])
    # print(appruv_avg_sum)
    df_dops_in_appruvs = calculate_orders_w_dops(new,merged)  

    ##########   MERGE
    merged['offer_id(заказа)'] = merged['offer_id(заказа)'].astype(str)
    prodano_oid['offer_id(заказа)'] = prodano_oid['offer_id(заказа)'].astype(str)
    df_3['offer_id(товара)'] = df_3['offer_id(товара)'].astype(str)
    df_sobes_main['offer_id(товара)'] = df_sobes_main['offer_id(товара)'].astype(str)

    merged_final = merged.merge(prodano_oid, on='offer_id(заказа)', how='left', suffixes=('', '_prodano'))
    merged_final = merged_final.merge(df_3, left_on='offer_id(заказа)', right_on='offer_id(товара)', how='left', suffixes=('', '_df3'))
    merged_final = merged_final.merge(df_sobes_main, left_on='offer_id(заказа)', right_on='offer_id(товара)', how='left', suffixes=('', '_sobes'))
    merged_final = merged_final.merge(prodano_all, on='offer_id(заказа)', how='left')
    merged_final = merged_final.merge(df_dops_in_appruvs, on='offer_id(заказа)', how='left')
    merged_final = merged_final.merge(profit_oid, on='offer_id(заказа)', how='left')
    merged_final = merged_final.merge(profit_all, on='offer_id(заказа)', how='left')
    merged_final = merged_final.merge(df_sum_all_w_delivery, on='offer_id(заказа)', how='left')
    merged_final = merged_final.merge(appruv_avg_sum, on='offer_id(заказа)', how='left')    
    merged_final = merged_final.merge(df_grouped,left_on='offer_id(заказа)', right_on='offer_id')

    merged_final['% Аппрува'] = merged_final['Кількість аппрувів'] / merged_final['Кількість лідів'] * 100
    merged_final['Коэф. Апрува'] = merged_final['% Аппрува'].apply(
        lambda x: get_appruv_coefficient(x, df_appruv_range)
    )    
    merged_final['Лид до $'] = merged_final.apply(lambda row: get_lead(row, df_payment), axis=1)


    merged_final['Лид до $'] = merged_final['Лид до $'].str.replace(',', '.')
    merged_final['Коэф. Апрува'] = merged_final['Коэф. Апрува'].str.replace(',', '.')

    merged_final['Лид до $'] = pd.to_numeric(merged_final['Лид до $'], errors='coerce')
    merged_final['Коэф. Апрува'] = pd.to_numeric(merged_final['Коэф. Апрува'], errors='coerce')

    merged_final['Лид до $'] = merged_final['Лид до $'] * merged_final['Коэф. Апрува']
    
    merged_final['Итоговая выручка с дост. в СУМ'] = merged_final['Итоговая выручка с дост. в СУМ'] * 1000
    merged_final['Refund SUM'] = merged_final['Refund SUM'] * 1000
    merged_final['Выручка по OID без доставки (от этого значения 5% баеру)'] = merged_final['Выручка по OID без доставки (от этого значения 5% баеру)'] * 1000
    merged_final['Выручка по всем товарам без доставки (все товары)_y'] = merged_final['Выручка по всем товарам без доставки (все товары)_y'] * 1000

    #stocks
    stocks = fetch_stocks_cache(api_key)

    # print(merged_final['offer_id(заказа)'].unique())
    
    names = df.dropna(subset=['Назва товару'])
    names = names[~names['Назва товару'].str.contains('оставка') & names['Match'] == 1]
    names = names.groupby('offer_id(заказа)').agg({'Назва товару': 'first'})
    
    merged_final = merged_final.merge(names, how='left', on='offer_id(заказа)')
    pattern = r'^[a-zA-Z]{2}-[a-zA-Z]{2}-\d{4}$'
    df_categories = merged_final[~merged_final['offer_id(заказа)'].str.contains(pattern, regex=True, na=False)]
    df_non_categories = merged_final[merged_final['offer_id(заказа)'].str.contains(pattern, regex=True, na=False)]
    
    df_non_categories = df_non_categories.merge(stocks, left_on='offer_id(заказа)', right_on='article')
    return df_non_categories,df_categories