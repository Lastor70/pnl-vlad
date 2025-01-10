import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

def authenticate_google_sheets(secret_name):
    """Аутентифікація в Google Sheets API за допомогою облікових даних."""
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(secret_name, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc

def fetch_google_sheet_data(spreadsheet_id, sheet_name, secret_name):
    """Отримання даних з Google Sheets для токенів."""
    gc = authenticate_google_sheets(secret_name)
    spreadsheet = gc.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(sheet_name)
    
    data = worksheet.get_all_values()
    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)
    df['User Token'] = df['User Token'].replace('', '0')
    df = df[df['User Token'].str.len() >= 10]
    
    return df

def fetch_sheet_as_dataframe(spreadsheet, sheet_name):
    """Отримання даних з Google Sheets у вигляді DataFrame."""
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()
    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)
    return df

def process_dataframe(df, start_column, end_column):
    """Обробка DataFrame: видалення порожніх рядків та налаштування заголовків."""
    df = df.iloc[1:, start_column:end_column].dropna(how='all')
    df.columns = df.iloc[1]
    df = df[2:]
    return df

def fetch_and_process_all_sheets(gc, spreadsheet_id):
    """Отримання і обробка всіх таблиць з оферами."""
    spreadsheet = gc.open_by_key(spreadsheet_id)

    # Отримання всіх необхідних таблиць
    df_offers = fetch_sheet_as_dataframe(spreadsheet, 'Справочник офферов')
    df_offers_tv = fetch_sheet_as_dataframe(spreadsheet, 'Офферы TV')
    df_offers_nr = fetch_sheet_as_dataframe(spreadsheet, 'Офферы NR')
    
    # Обробка таблиць
    df_sasha = process_dataframe(df_offers, 1, 7)
    df_maks = process_dataframe(df_offers, 8, 14)
    df_ilya = process_dataframe(df_offers, 15, 21)
    df_dima = process_dataframe(df_offers, 22, 28)
    df_oleg = process_dataframe(df_offers, 29, 35)
    df_vlad = process_dataframe(df_offers, 36, 42)
    df_pasha = process_dataframe(df_offers, 43, 49)


    df_sasha_tv = process_dataframe(df_offers_tv, 1, 7)
    df_maks_tv = process_dataframe(df_offers_tv, 8, 14)
    df_ilya_tv = process_dataframe(df_offers_tv, 15, 21)
    df_dima_tv = process_dataframe(df_offers_tv, 22, 28)
    df_pasha_tv = process_dataframe(df_offers_tv, 29, 35)
    df_vlad_tv = process_dataframe(df_offers_tv, 36, 42)

    df_sasha_nr = process_dataframe(df_offers_nr, 1, 7)
    df_maks_nr = process_dataframe(df_offers_nr, 8, 14)
    df_ilya_nr = process_dataframe(df_offers_nr, 15, 21)
    df_dima_nr = process_dataframe(df_offers_nr, 22, 28)
    df_oleg_nr = process_dataframe(df_offers_nr, 29, 35)


    # Об'єднання всіх DataFrame
    dfs = [
        df_sasha, df_maks, df_ilya, df_dima, df_oleg, df_vlad,df_pasha,
        df_sasha_tv, df_maks_tv, df_ilya_tv, df_dima_tv, df_pasha_tv, df_vlad_tv,
        df_sasha_nr, df_maks_nr, df_ilya_nr, df_dima_nr, df_oleg_nr
    ]
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Обробка даних
    combined_df['Коэф. Слож.'] = combined_df['Коэф. Слож.'].str.replace(',', '.')
    combined_df['Коэф. Слож.'] = pd.to_numeric(combined_df['Коэф. Слож.'])
    combined_df.drop_duplicates(subset='ID Оффера', keep='first', inplace=True)

    return combined_df
