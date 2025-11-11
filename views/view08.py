# 서희_최신수정일_25-09-17

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
import json
import sys
import modules.style
importlib.reload(sys.modules['modules.style'])
from modules.style import style_format, style_cmap


def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정
    # ──────────────────────────────────

    st.markdown(
        """
        <style>
            /* 전체 컨테이너의 패딩 조정 */
            .block-container {
                max-width: 100% !important;
                padding-top: 1rem;   /* 위쪽 여백 */
                padding-bottom: 8rem;
                padding-left: 5rem; 
                padding-right: 4rem; 
            }
        </style>
        """,
        unsafe_allow_html=True
    )  

    st.subheader('OVERVIEW')
    st.markdown(
        """
        <div style="
            color:#6c757d;        
            font-size:14px;       
            line-height:1.5;      
        ">
        이 대시보드는 ...<br>
        여기서는 ...
        </div>
        """,
        unsafe_allow_html=True
    )

    st.divider()
    
    
    # ────────────────────────────────────────────────────────────────
    # 사이드바 필터 설정
    # ────────────────────────────────────────────────────────────────    
    st.sidebar.header("Filter")
    
    today = datetime.now().date()
    default_end = today - timedelta(days=1)
    default_start = today - timedelta(days=16)
    start_date, end_date = st.sidebar.date_input(
        "기간 선택",
        value=[default_start, default_end],
        max_value=default_end
    )
    cs = start_date.strftime("%Y%m%d")
    ce = end_date.strftime("%Y%m%d")


    @st.cache_data(ttl=3600)
    def load_data(cs: str, ce: str) -> pd.DataFrame:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]

        try: 
            creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
        except: # 배포용 (secrets.toml)
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str):  # 혹시 문자열(JSON)로 저장했을 경우
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1crJTg3VD0I_AKRHhCaAiuLp-39Qr0mJWlPWP7oNgVGA/edit?gid=1867500702#gid=1867500702')
        
        # 시트 불러오기 
        df   = pd.DataFrame(sh.worksheet('raw_test').get_all_records())


        # ---- 날짜 컬럼 정규화 ----
        if 'date' not in df.columns:
            if '날짜' in df.columns:
                df = df.rename(columns={'날짜': 'date'})
            else:
                raise ValueError("raw_test 시트에 'date' 컬럼이 없습니다.")
        
        df['date']   = pd.to_datetime(df['date'])

        # ---- 구간 필터 (포함 범위) ----
        start = pd.to_datetime(cs, format='%Y%m%d')
        end   = pd.to_datetime(ce, format='%Y%m%d')
        df = df[(df['date'] >= start) & (df['date'] <= end)].reset_index(drop=True)

        return df

    # ────────────────────────────────────────────────────────────────
    # 데이터 불러오기
    # ────────────────────────────────────────────────────────────────
    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df = load_data(cs, ce)


    # ────────────────────────────────────────────────────────────────
    # 데이터 전처리
    # ────────────────────────────────────────────────────────────────
    # 필요한 데이터 컬럼만 뽑기
    DF = df[['date', 'metric', 'value']]

    # 선택 가능한 지표 목록 (원하는 8개가 자동 구성됨)
    metric_opts = DF['metric'].dropna().drop_duplicates().tolist()
    if not metric_opts:
        st.warning("선택할 metric이 없습니다.")
        st.stop()
    

    # ──────────────────────────────────
    # 1) 시각화
    # ──────────────────────────────────
    st.session_state.setdefault("active_metric", metric_opts[0] if metric_opts else "매출")
    for i, default_m in enumerate(metric_opts[:3] + metric_opts[3:3]):
        st.session_state.setdefault(f"card_metric_{i}", default_m if metric_opts else "매출")

    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown("<h5 style='margin:0'>OVERVIEW</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ카드 상단 드롭다운에서 지표를 고르고, 카드를 클릭하면 아래에 해당 지표 추이")

    # 버튼 스타일
    st.markdown("""
    <style>
    /* 버튼 자체: 좌측 정렬 & 좌측으로 플렉스 정렬 */
    .stElementContainer[class*="st-key-btn_"] > div > button {
    text-align: left !important;
    justify-content: flex-start !important;
    }

    /* 라벨 텍스트(p 또는 span)에 2rem + 좌측정렬 */
    .stElementContainer[class*="st-key-btn_"] > div > button > div > p,
    .stElementContainer[class*="st-key-btn_"] > div > button > div > span {
    font-size: 1.5rem !important;
    font-weight: 400 !important;
    line-height: 1.1 !important;
    margin: 1.5rem 0.2rem !important;
    text-align: left !important;
    }
    </style>
    """, unsafe_allow_html=True)


    # 카드
    num_cards = 6
    cols = st.columns(num_cards)
    for i, col in enumerate(cols):
        with col:
            sel_key = f"card_metric_{i}"
            current = st.session_state.get(sel_key, metric_opts[min(i, len(metric_opts)-1)])
            chosen = st.selectbox(
                label="지표",
                options=metric_opts,
                index=metric_opts.index(current) if current in metric_opts else 0,
                key=f"sel_{i}",
                label_visibility="collapsed",
                help="이 카드가 표시할 지표",
            )
            st.session_state[sel_key] = chosen

            value_sum = int(DF.loc[DF["metric"] == chosen, "value"].sum())           
            clicked = st.button(
                f"{value_sum:,}",
                key=f"btn_{i}",
                use_container_width=True,
            )
            if clicked:
                st.session_state.active_metric = chosen


    # 그래프
    active = st.session_state.active_metric
    # st.markdown(f"<div class='subtle'>활성 지표: <b>{active}</b></div>", unsafe_allow_html=True)

    df_line = DF[DF["metric"] == active].sort_values("date")
    fig = px.line(
        df_line,
        x="date",
        y="value",
        markers=True,
        title=f"{active} 추이",
        labels={"date": "date", "value": active},
    )
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()