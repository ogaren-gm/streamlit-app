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




    # # 예시 데이터 생성
    # data = {
    #     'A': [1, 2, 3, 4],
    #     'B': [5, 6, 7, 8],
    #     'C': [9, 10, 11, 12]
    # }
    # df = pd.DataFrame(data)

    # # 합계 행 추가 (각 열에 대해 합계를 계산)
    # sum_row = df.sum(numeric_only=True)
    # sum_row['A'] = sum_row['A']
    # sum_row['B'] = sum_row['B']
    # sum_row['C'] = sum_row['C']

    # # 합계 행을 새로운 DataFrame으로 변환
    # sum_row_df = pd.DataFrame([sum_row])

    # # 합계 행을 "합계"로 레이블 변경 (원하는 경우)
    # sum_row_df.iloc[0, 0] = "합계"

    # # 합계 행을 제외한 DataFrame을 정렬
    # df_without_sum = df.sort_values(by='A', ascending=True)  # 예시로 'A' 기준으로 정렬

    # # 정렬된 데이터와 합계 행 결합
    # df_with_sum = pd.concat([df_without_sum, sum_row_df], ignore_index=True)

    # # st.dataframe으로 출력
    # st.dataframe(df_with_sum, hide_index=True)





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

