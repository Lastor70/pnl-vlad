import openpyxl

def save_data_to_excel(merged_ss, start_date, end_date,df_categories,df_spend_wo_leads):
    file_path = 'data/template-p&l-5.0.xlsx'  
    wb1 = openpyxl.load_workbook(file_path)
    sh_paste = wb1['Лист1']
    sh_catalog = wb1['Catalog']

    column_mapping = {
        'Назва товару': 'A',
        'category': 'B',
        'buyer': 'C',
        'order_method': 'D',
        'offer_id(заказа)': 'E',
        'Кількість лідів': 'F',
        'Кількість чистих лідів': 'G',
        '% trash': 'H',
        'Кількість аппрувів': 'I',
        '% preorders': 'L',
        'Доставляются': 'M',
        'Возврат': 'O',
        'Выкуп': 'P',
        'Refund': 'R',
        'Продано товаров шт. (OID)': 'T',
        'quantity': 'U',
        'Себес (OID) из СРМ': 'V',
        'Продано товаров всего': 'W',
        '% заказов с допами в апрувах': 'Y',
        'sum_air_upsell': 'Z',
        # 'Выручка по OID без доставки (от этого значения 5% баеру)': 'V',
        'Выручка по всем товарам без доставки (все товары)_y': 'AA',
        'Итоговая выручка с дост. в СУМ': 'AC',
        'Средний чек апрува без доставки': 'AE',
        # 'Коэф. Апрува': 'AA',
        # 'Лид до $': 'AB',
        'spend': 'AH',
        'Refund SUM': 'AQ',        
        'Себес товаров': 'AS',                                                                                 
    }

    column_mapping_for_all = {
        'Назва товару': 'A',
        'category': 'B',
        'buyer': 'C',
        'order_method': 'D',
        'offer_id_cut': 'E',
        'Кількість лідів': 'F',
        'Кількість чистих лідів': 'G',
        '% trash': 'H',
        'Кількість аппрувів': 'I',
        '% preorders': 'L',
        'Доставляются': 'M',
        'Возврат': 'O',
        'Выкуп': 'P',
        'Refund': 'R',
        'Продано товаров шт. (OID)': 'T',
        'quantity': 'U',
        'Себес (OID) из СРМ': 'V',
        'Продано товаров всего': 'W',
        '% заказов с допами в апрувах': 'Y',
        'sum_air_upsell': 'Z',
        # 'Выручка по OID без доставки (от этого значения 5% баеру)': 'V',
        'Выручка по всем товарам без доставки (все товары)_y': 'AA',
        'Итоговая выручка с дост. в СУМ': 'AC',
        'Средний чек апрува без доставки': 'AE',
        # 'Коэф. Апрува': 'AA',
        # 'Лид до $': 'AB',
        'spend': 'AH',
        'Refund SUM': 'AQ',        
        'Себес товаров': 'AS',                                                                                 
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
        'buyer_id': 'BL',
        'offer_id': 'BM',
        'spend': 'BN',
        'leads': 'BO'
    }

    copy_formatting(sh_paste, sh_paste)
    copy_formatting(sh_catalog, sh_catalog)

    if not df_categories.empty:
        paste_data_with_formatting(df_categories, column_mapping_for_all, sh_catalog)

    if not df_spend_wo_leads.empty:
        paste_data_with_formatting(df_spend_wo_leads, map_cash, sh_paste)

    paste_data_with_formatting(merged_ss, column_mapping, sh_paste)

    filename = f'PNL за {start_date}-{end_date}.xlsx'
    wb1.save(filename)

    return filename 
