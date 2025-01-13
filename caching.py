import streamlit as st
from google_sheets import fetch_google_sheet_data, authenticate_google_sheets
from crm_requests import get_orders
from google_sheets import fetch_and_process_all_sheets
from process_payment import fetch_and_process_payment_sheet
from facebook_api import fetch_facebook_data
from sobes_req_processing import fetch_sobes_data_from_api

# @st.cache_data
def fetch_tokens_data(spreadsheet_id, sheet_name, creds_dict):
    return fetch_google_sheet_data(spreadsheet_id, sheet_name, creds_dict)

@st.cache_data(ttl=3600)
def get_sobes_data():
    return fetch_sobes_data_from_api()

@st.cache_data(ttl=3600)
def fetch_orders_data(api_key, start_date, end_date, request_type):
    return get_orders(api_key, start_date, end_date, request_type)

@st.cache_data(ttl=3600)
def fetch_vykups_data(api_key, start_date, end_date, buyer, request_type):
    return get_orders(api_key, start_date, end_date, buyer, request_type)

@st.cache_data(ttl=3600)
def fetch_offers_data(spreadsheet_id, creds_dict):
    gc = authenticate_google_sheets(creds_dict)  # Аутентифікація тут
    return fetch_and_process_all_sheets(gc, spreadsheet_id)

@st.cache_data(ttl=3600)
def fetch_payment_data(spreadsheet_id, sheet_name, creds_dict):
    gc = authenticate_google_sheets(creds_dict)  # Аутентифікація тут
    return fetch_and_process_payment_sheet(gc, spreadsheet_id, sheet_name)

@st.cache_data(ttl=3600)
def cached_fetch_facebook_data(df_tokens, start_date_str, end_date_str):
    return fetch_facebook_data(df_tokens, start_date_str, end_date_str)
