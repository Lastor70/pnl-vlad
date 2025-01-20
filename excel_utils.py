import openpyxl

def save_data_to_excel(merged_ss, start_date, end_date,df_categories):
    file_path = 'data/template-p&l-3.0.xlsx'  
    wb1 = openpyxl.load_workbook(file_path)
    sh_paste = wb1['Лист1']
    sh_catalog = wb1['Catalog']

    column_mapping = {
        'Назва товару': 'A',
        'offer_id(заказа)': 'B',
        'Кількість лідів': 'C',
        'Кількість чистих лідів': 'D',
        'Кількість аппрувів': 'F',
        'Доставляются': 'I',
        'Возврат': 'K',
        'Выкуп': 'L',
        'Refund': 'N',
        'Продано товаров шт. (OID)': 'P',
        'quantity': 'Q',
        'Себес (OID) из СРМ': 'R',
        'Продано товаров всего': 'S',
        '% заказов с допами в апрувах': 'U',
        'Выручка по OID без доставки (от этого значения 5% баеру)': 'V',
        'Выручка по всем товарам без доставки (все товары)_y': 'W',
        'Итоговая выручка с дост. в СУМ': 'X',
        'Средний чек апрува без доставки': 'Z',
        'Коэф. Апрува': 'AA',
        'Лид до $': 'AB',
        'spend': 'AC',
        'Refund SUM': 'AJ',        
        'Себес товаров': 'AL',                                                                                 
    }

    def paste_data(df, mapping, sheet):
        for df_column, excel_column in mapping.items():
            if df_column in df.columns:  # Перевірка на наявність стовпця в DataFrame
                column_data = df[df_column]
                for row_idx, value in enumerate(column_data, start=1):
                    cell = sheet[f"{excel_column}{row_idx+6}"]
                    cell.value = value
                    cell._style = sheet[f"{excel_column}7"]._style

    map_cash = {
        'offer_id': 'AB',
        'Рекл.спенд.': 'AC',
        'Лидов из ads': 'AD'
    }



    if not df_categories.empty:
        paste_data(df_categories, column_mapping, sh_catalog)
    # if not catalog_cash.empty:
    #     paste_data(catalog_cash, map_cash, sh_catalog)    

    paste_data(merged_ss, column_mapping, sh_paste)

    filename = f'PNL за {start_date}-{end_date}.xlsx'
    wb1.save(filename)

    return filename  # Повертаємо шлях до збереженого файлу
