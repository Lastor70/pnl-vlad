import openpyxl

def save_data_to_excel(catalog_w_leads, car_space_merged, catalog_cash, merged_ss, result_df, total_vykup, b, start_date, end_date):
    file_path = 'data/template-p&l-3.0.xlsx'  
    wb1 = openpyxl.load_workbook(file_path)
    sh_paste = wb1['Лист1']
    # sh_catalog = wb1['Catalog']

    column_mapping = {
        'Название оффера': 'B',
        'offer_id(заказа)': 'C',
        'Кількість лідів': 'D',
        'Кількість чистих лідів': 'E',
        'Кількість аппрувів': 'G',
        'Средняя сумма в апрувах': 'J',
        'Лид до $': 'L',
        'Коэф. Апрува': 'M',
        'spend': 'N',
        'leads': 'O',
    }

    def paste_data(df, mapping, sheet):
        for df_column, excel_column in mapping.items():
            if df_column in df.columns:  # Перевірка на наявність стовпця в DataFrame
                column_data = df[df_column]
                for row_idx, value in enumerate(column_data, start=1):
                    cell = sheet[f"{excel_column}{row_idx+3}"]
                    cell.value = value
                    cell._style = sheet[f"{excel_column}4"]._style

    map_cash = {
        'offer_id': 'AB',
        'Рекл.спенд.': 'AC',
        'Лидов из ads': 'AD'
    }


    # if not catalog_w_leads.empty:
    #     paste_data(catalog_w_leads, column_mapping, sh_catalog)

    paste_data(merged_ss, column_mapping, sh_paste)

    filename = f'PNL за {start_date}-{end_date}.xlsx'
    wb1.save(filename)

    return filename  # Повертаємо шлях до збереженого файлу
