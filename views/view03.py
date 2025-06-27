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
import plotly.graph_objects as go

def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정 (반드시 최상단)
    # ──────────────────────────────────
    st.set_page_config(layout="wide", page_title="SLPR 대시보드")
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
    st.subheader("매체 대시보드")
    st.divider()
    
    st.markdown("매체별 GA이벤트 세션수")
