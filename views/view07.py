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
    # st.set_page_config(layout="wide", page_title="SLPR | 언드 대시보드")
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
    
    # ───────────── 샘플 데이터 ─────────────
    data = {
        ("기본정보", "날짜"): ["2025-08-01", "2025-08-02", "2025-08-03"],
        ("기본정보", "채널명"): ["Youtube", "Instagram", "Naver"],
        ("Engagement", "조회수"): [1200, 980, 1500],
        ("Engagement", "좋아요수"): [240, 310, 400],
        ("GA Actions", "세션수"): [560, 630, 720],
        ("GA Actions", "회원가입"): [12, 15, 20],
    }

    # ───────────── MultiIndex 적용 ─────────────
    df = pd.DataFrame(data)
    df.columns = pd.MultiIndex.from_tuples(df.columns, names=["그룹", "지표"])

    # ───────────── 스타일링 (천단위 콤마, 퍼센트 등) ─────────────
    styled = df.style.format(thousands=",")

    # ───────────── Streamlit 표시 ─────────────
    # The line `# st.dataframe(styled, use_container_width=True)` is a commented-out line of code in
    # Python using Streamlit.
    st.dataframe(styled, use_container_width=True)
    


    df = pd.DataFrame({
        "A": [10, 20, 5],
        "B": [3000, 12000, 8000],
        "C": [0.12, 0.45, 0.08]
    })

    styled = (
        df.style
        .format({"A": "{:,}", "B": "{:,}", "C": "{:.0%}"})
        # B 컬럼만 연한 히트맵 (YlOrRd)
        .background_gradient(
            subset=["B"], cmap="YlOrRd",
            vmin=df["B"].min(), vmax=df["B"].max(),
            low=0.92, high=0.0   # 값 클수록 더 연해짐(화이트에 가까움)
        )
        # C(퍼센트)도 별도로 아주 연하게??
        .background_gradient(
            subset=["C"], cmap="Greens",
            vmin=0.0, vmax=1.0,
            low=0.0, high=0.5
        )
    )

    st.dataframe(styled)  # 또는: st.markdown(styled.to_html(), unsafe_allow_html=True)

