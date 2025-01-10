import pandas as pd
import requests
import asyncio
import aiohttp

def get_ad_accounts(access_token):
    url = 'https://graph.facebook.com/v18.0/me/adaccounts'
    fields = 'id,name'#,amount_spent,spend_cap'
    limit = 100
    data = []
    page_count = 0

    while url:
        try:
            params = {
                'fields': fields,
                'limit': limit,
                'access_token': access_token
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            response_data = response.json()

            if 'data' in response_data:
                for account in response_data['data']:
                    account['User Token'] = access_token
                data.extend(response_data['data'])

            url = response_data.get('paging', {}).get('next')
            page_count += 1
            print(f"Отримано сторінку {page_count}. Кількість отриманих записів: {len(data)}")

        except requests.exceptions.RequestException as e:
            print(f"Помилка запиту для токена {access_token}: {e}")
            break

    print(f"Всього отримано {len(data)} записів")
    return data

def get_all_accounts(tokens):
    all_accounts = []
    for token in tokens:
        if token != '0':
            accounts = get_ad_accounts(token)
            all_accounts.extend(accounts)
    return pd.DataFrame(all_accounts)

async def fetch_campaigns(ad_account_id, access_token):
    url = f"https://graph.facebook.com/v18.0/{ad_account_id}/campaigns"
    fields = 'id,name'
    limit = 100
    data = []
    page_count = 0

    async with aiohttp.ClientSession() as session:
        while url:
            params = {
                'fields': fields,
                'limit': limit,
                'access_token': access_token
            }

            try:
                async with session.get(url, params=params) as response:
                    response_data = await response.json()

                    if 'data' in response_data:
                        for campaign in response_data['data']:
                            campaign_data = {
                                'Campaign ID': campaign['id'],
                                'Campaign Name': campaign['name'],
                                'User Token': access_token  # Зберегти токен для цієї кампанії
                            }
                            data.append(campaign_data)

                    url = response_data.get('paging', {}).get('next')
                    page_count += 1

            except aiohttp.ClientError as e:
                print(f"Помилка запиту для облікового запису {ad_account_id}: {e}")
                break

    return data

async def fetch_all_campaigns(ad_accounts_df):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, row in ad_accounts_df.iterrows():
            ad_account_id = row['id']
            access_token = row['User Token']
            tasks.append(fetch_campaigns(ad_account_id, access_token))

        all_data = await asyncio.gather(*tasks)
        return [item for sublist in all_data for item in sublist]

def get_all_campaigns_data(ad_accounts_df):
    return pd.DataFrame(asyncio.run(fetch_all_campaigns(ad_accounts_df)))

async def get_campaign_data(session, campaign_id, access_token, start_date, end_date, offer_id):
    url = f'https://graph.facebook.com/v12.0/{campaign_id}/insights'
    params = {
        'level': 'campaign',
        'fields': 'spend,actions',
        'action_attribution_windows': '1d_click',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'access_token': access_token
    }
    try:
        async with session.get(url, params=params) as response:
            data = await response.json()
            if 'data' in data and data['data']:
                spend = data['data'][0].get('spend', 0)
                actions = data['data'][0].get('actions', [])
                if 'cs-' in offer_id:
                    leads = sum(int(action['value']) for action in actions if action['action_type'] == 'offsite_conversion.fb_pixel_complete_registration')
                else:
                    leads = sum(int(action['value']) for action in actions if action['action_type'] == 'offsite_conversion.fb_pixel_initiate_checkout')
                return spend, leads
            else:
                return None, None
    except aiohttp.ClientResponseError as e:
        print(f"Помилка при отриманні даних з API: {e}")
        return None, None

async def get_campaign_data_for_filtered_df(filtered_df, start_date, end_date):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in filtered_df.iterrows():
            tasks.append(get_campaign_data(session, row['Campaign ID'], row['User Token'], start_date, end_date, row['offer_id']))

        results = await asyncio.gather(*tasks)

        filtered_df['spend'] = [result[0] for result in results]
        filtered_df['leads'] = [result[1] for result in results]

        return filtered_df

def fetch_facebook_data(df_tokens, start_date_str, end_date_str):
    tokens = df_tokens['User Token'].explode().tolist() if 'User Token' in df_tokens else []
    
    if tokens:
        ad_accounts_df = get_all_accounts(tokens)
        campaigns_data = get_all_campaigns_data(ad_accounts_df)
        campaigns_data['offer_id'] = campaigns_data['Campaign Name'].apply(lambda x: x.split('|')[2].strip() if len(x.split('|')) > 2 else None)
        campaigns_data = campaigns_data.dropna(subset=['offer_id'])
        df_campaign_data = asyncio.run(get_campaign_data_for_filtered_df(campaigns_data, start_date_str, end_date_str))
        print(111)
        df_grouped = group_data_by_offer_id(df_campaign_data)
        return df_grouped
    return None

def group_data_by_offer_id(df):
    try:
        df['spend'] = pd.to_numeric(df['spend'], errors='coerce')
        df.drop_duplicates(['Campaign ID'],inplace=True)
        # print(df[df['offer_id'] == 'ss-ss-0167'])
        df_grouped = df.groupby('offer_id').agg({'spend': 'sum', 'leads': 'sum'}).reset_index()
        df_grouped['offer_id'] = df_grouped['offer_id'].str.replace('\ufeff', '', regex=False)
        # print(df_grouped[df_grouped['offer_id'].str.contains('tv-mb-0003')]['offer_id'].iloc[0])
        
        return df_grouped
    except Exception as e:
        print(f"Проблема з групуванням даних: {e}")
