import pandas as pd
import requests

def get_status_groups(api_key):
    url = 'https://uzshopping.retailcrm.ru/api/v5/reference/status-groups'
    params = {
        'apiKey': api_key,
        'limit':100
    }
    r = requests.get(url, params=params)
    json_data = r.json()
    status_groups = json_data["statusGroups"]

    data = []

    for group_name, group_data in status_groups.items():
        statuses = ", ".join(group_data["statuses"]) 
        data.append([group_name, statuses])

    df = pd.DataFrame(data, columns=["statusGroup", "statuses"])
    df_to = df[df['statusGroup'].isin(['approve','assembling','delivery','complete','refund'])]
    
    appruv_statuses = [
    'urgent',
    'dop-prozvon',
    'call-emu',
    'perepodtverdit-net-tovara', 'perepodtverdit-net-xml-koda',
    'return',
    'vozvrat-predvaritelno',
    'plan-vozvrat',
    'perepodtverdit-net-xml-koda','perepodtverdit-net-tovara']

    for status_list in df_to['statuses']:
        appruv_statuses.extend(status_list.split(', '))

    return appruv_statuses