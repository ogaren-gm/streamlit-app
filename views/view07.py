import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import importlib
from datetime import datetime, timedelta
import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import JsCode
import io
from google.oauth2.service_account import Credentials
import gspread
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
import math
# from streamlit_extras.let_it_rain import rain

def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정
    # ──────────────────────────────────
    st.markdown(
        """
        <style>
        .reportview-container .main .block-container {
            max-width: 100% !important;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.subheader('테스트')
    # ──────────────────────────────────


    # rain(
    #     emoji="🍊",
    #     font_size=30,
    #     falling_speed=4,
    #     animation_length=3,
    # )
    
    
    # 예시 데이터 생성
    data = {
        'A': [1, 2, 3, 4],
        'B': [5, 6, 7, 8],
        'C': [9, 10, 11, 12],
        'Category': ['X', 'Y', 'Z', 'W']
    }
    df = pd.DataFrame(data)


    sum_row = df.select_dtypes(include='number').sum()

    total_row = df.select_dtypes(exclude='number').apply(lambda x: 'Total')

    sum_row_df = pd.DataFrame([sum_row]).assign(**total_row)

    df_with_sum = pd.concat([sum_row_df, df], ignore_index=True)

    # st.dataframe으로 합계 행을 포함한 데이터프레임을 출력
    st.dataframe(df_with_sum, hide_index=True)

