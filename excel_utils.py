import openpyxl

def save_data_to_excel(merged_ss, start_date, end_date,df_categories):
    file_path = 'data/template-p&l-4.0.xlsx'  
    wb1 = openpyxl.load_workbook(file_path)
    sh_paste = wb1['Лист1']
    sh_catalog = wb1['Catalog']

    column_mapping = {
        'Назва товару': 'A',
        'category': 'B',
        'offer_id(заказа)': 'D',
        'Кількість лідів': 'E',
        'Кількість чистих лідів': 'F',
        'Кількість аппрувів': 'H',
        'Доставляются': 'K',
        'Возврат': 'M',
        'Выкуп': 'N',
        'Refund': 'P',
        'Продано товаров шт. (OID)': 'R',
        'quantity': 'S',
        'Себес (OID) из СРМ': 'T',
        'Продано товаров всего': 'U',
        '% заказов с допами в апрувах': 'W',
        # 'Выручка по OID без доставки (от этого значения 5% баеру)': 'V',
        'Выручка по всем товарам без доставки (все товары)_y': 'X',
        'Итоговая выручка с дост. в СУМ': 'Z',
        'Средний чек апрува без доставки': 'AB',
        # 'Коэф. Апрува': 'AA',
        # 'Лид до $': 'AB',
        'spend': 'AE',
        'Refund SUM': 'AN',        
        'Себес товаров': 'AP',                                                                                 
    }

    def copy_formatting(src_sheet, dest_sheet):
        for row in src_sheet.iter_rows():
            for cell in row:
                dest_cell = dest_sheet[cell.coordinate]
                dest_cell._style = cell._style 

    def paste_data_with_formatting(df, mapping, sheet):
        for df_column, excel_column in mapping.items():
            if df_column in df.columns:
                column_data = df[df_column]
                for row_idx, value in enumerate(column_data, start=1):
                    cell = sheet[f"{excel_column}{row_idx+6}"]
                    cell.value = value

    map_cash = {
        'offer_id': 'BJ',
        'Рекл.спенд.': 'BK',
        'Лидов из ads': 'BL'
    }

    copy_formatting(sh_paste, sh_paste)
    copy_formatting(sh_catalog, sh_catalog)

    if not df_categories.empty:
        paste_data_with_formatting(df_categories, column_mapping, sh_catalog)
    paste_data_with_formatting(merged_ss, column_mapping, sh_paste)

    filename = f'PNL за {start_date}-{end_date}.xlsx'
    wb1.save(filename)

    return filename 
