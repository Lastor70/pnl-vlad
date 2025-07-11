import streamlit as st
from datetime import datetime, timedelta
from caching import *
# from catalog_processing import process_catalog
from data_processing_main_req import process_orders_data
from excel_utils import save_data_to_excel

st.set_page_config(page_title="Рассчет PNL", page_icon="📈")
st.title("Рассчет PNL")
st.header('Фильтр по датам')


def check_password():
    """Показує поле введення пароля, поки не введено правильний. Зберігає статус у session_state."""
    def password_entered():
        if st.session_state["password"] == st.secrets["auth_password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # очищення для безпеки
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("🔒 Введіть пароль", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("🔒 Введіть пароль", type="password", on_change=password_entered, key="password")
        st.error("❌ Невірний пароль")
        return False
    else:
        return True

if not check_password():
    st.stop()

api_key = st.secrets["api_key"]
google_sheets_creds = st.secrets["gcp_service_account"]

# отримання даних справочніка гуглшит
spreadsheet_id_offers = '15GvP6wElztDSQKqk5kxnB37dKxKi3nTyEsTbBF1vqW4'
combined_df = fetch_offers_data(spreadsheet_id_offers, dict(google_sheets_creds))
# отримання даних виплат
sheet_name_payment = 'Выплата (new) копия'
df_payment, df_appruv_range,df_buyers_name = fetch_payment_data(spreadsheet_id_offers, sheet_name_payment, dict(google_sheets_creds))


current_date = datetime.now()
first_day_of_month = current_date.replace(day=1)

start_date = st.date_input('Начальная дата', value=first_day_of_month)
end_date = st.date_input('Конечная дата', value=current_date)

if end_date < start_date:
    st.error('Конечная дата не может быть раньше начальной даты')

start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# отримання токенів з гуглшит
spreadsheet_id_tokens = '1Q8eFscYd9dsl6QTzLiRQqKXMg3HFuZgwjd9kg0fOMdQ'
sheet_name_tokens = 'Лист1'
df_tokens = fetch_tokens_data(spreadsheet_id_tokens, sheet_name_tokens, dict(google_sheets_creds))

# print(df_tokens)

# Кнопка для вигрузки та обробки даних
if st.button("Выгрузить и обработать данные"):
    progress_bar = st.progress(0)

    # Отримання даних з ФБ
    df_grouped = cached_fetch_facebook_data(df_tokens, start_date_str, end_date_str)
    st.session_state['df_grouped'] = df_grouped
    progress_bar.progress(15)
    
    # Отримання замовлень з CRM
    request_type = 'main'
    df_orders = fetch_orders_data(api_key, start_date_str, end_date_str, request_type)
    progress_bar.progress(80)
    # st.write(df_grouped)
    

    # Обробка замовлень
    processed_orders,df_categories,df_spend_wo_leads,buyers = process_orders_data(df_orders, api_key, df_payment, df_appruv_range, df_grouped)
    progress_bar.progress(95)
    
    st.session_state.update({
        'processed_orders': processed_orders,
        'df_categories': df_categories,
        'df_spend_wo_leads': df_spend_wo_leads,
        'buyers': buyers,
        # 'df_sobes_main': df_sobes_main,
    #     'spend_wo_leads': spend_wo_leads,
    #     'df_orders': df_orders,
        # 'df': df,
    })
    st.write(processed_orders)



    # st.session_state.update({
    #     'car_space_merged': car_space_merged,
    #     'catalog_w_leads': catalog_w_leads,
    #     'catalog_cash': catalog_cash
    # })
    # progress_bar.progress(80)


    # st.write(processed_orders)

    filename = save_data_to_excel(
        processed_orders, 
        start_date_str, 
        end_date_str,
        df_categories,
        df_spend_wo_leads,
        buyers
    )
    
    with open(filename, "rb") as f:
        st.download_button(
            "Скачать Excel файл",
            f,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )