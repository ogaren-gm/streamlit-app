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
    st.set_page_config(layout="wide", page_title="SLPR | 언드 대시보드")
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
    st.subheader('언드 대시보드 (구축중)')
    st.markdown("""
    이 대시보드는 ... 입니다.  
    여기서는 ... 있습니다.
    """)
    st.markdown(
        '<a href="https://www.notion.so/SLPR-241521e07c7680df86eecf5c5f8da4af#241521e07c768094ab81e56cd47e5164" target="_blank">'
        '지표설명 & 가이드</a>',
        unsafe_allow_html=True
    )
    st.divider()
    

    # ────────────────────────────────────────────────────────────────
    # 사이드바 필터 설정
    # ────────────────────────────────────────────────────────────────    
    @st.cache_data(ttl=3600)
    def load_data():
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(
            "C:/_code/auth/sleeper-461005-c74c5cd91818.json",
            scopes=scope
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1Li4YzwsxI7rB3Q2Z0gkuGIyANTaxFrVzgsKE-RAAdME/edit?gid=2078920702#gid=2078920702')

        PPL_LIST   = pd.DataFrame(sh.worksheet('PPL_LIST').get_all_records())
        PPL_DATA   = pd.DataFrame(sh.worksheet('PPL_DATA').get_all_records())
        PPL_ACTION = pd.DataFrame(sh.worksheet('PPL_ACTION').get_all_records())
        query     = pd.DataFrame(sh.worksheet('query').get_all_records())
        
        # # 3) tb_sleeper_psi
        # bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        # df_psi = bq.get_data("tb_sleeper_psi")
        # df_psi["event_date"] = pd.to_datetime(df_psi["event_date"], format="%Y%m%d")
        
        return PPL_LIST, PPL_DATA, PPL_ACTION, query


    with st.spinner("데이터를 불러오는 중입니다. ⏳"):
        PPL_LIST, PPL_DATA, PPL_ACTION, query = load_data()


    # 날짜 컬럼 타입 변환
    # ppl_data['날짜']   = pd.to_datetime(ppl_data['날짜'])
    # ppl_action['날짜'] = pd.to_datetime(ppl_action['날짜'])
    query['날짜']     = pd.to_datetime(query['날짜'])
    
    
    # ────────────────────────────────────────────────────────────────
    # 1번 영역
    # ────────────────────────────────────────────────────────────────
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)

    # 1번 영역
    st.markdown("<h5 style='margin:0'>집행 채널 목록</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)
    
    df = PPL_LIST
    cols_per_row = 3
    rows = math.ceil(len(df) / cols_per_row)

    for i in range(rows):
        # gap="small" 으로 컬럼 간격 최소화
        cols = st.columns(cols_per_row, gap="medium")
        for j, col in enumerate(cols):
            idx = i * cols_per_row + j
            if idx >= len(df):
                break
            row = df.iloc[idx]
            with col:
                # 카드 박스 스타일
                st.markdown(
                    f"""
                    <div style="
                    border:1px solid #e1e1e1;
                    border-radius:6px;
                    padding:16px 20px;
                    margin-bottom:8px;
                    box-shadow: 0px 1px 3px rgba(0,0,0,0.1);
                    ">
                    <strong style="font-size:1.1em;">{row['채널명']}</strong><br>
                    <small style="color:#555;">{row['업로드 날짜']}</small>
                    <div style="display:flex; justify-content:space-between; font-size:0.9em;">
                        <div style="margin:6px 0;">
                        <div style="color:#333;">집행 금액 <strong>{int(row['금액']):,}원</strong></div>
                        </div>
                        <div>
                        {"🔗 <a href='" + row['컨텐츠 URL'] + "' target='_blank'>컨텐츠 보기</a>" 
                        if row.get('컨텐츠 URL') else "🔗 링크 없음"}
                        </div>
                    </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    
    
    # ────────────────────────────────────────────────────────────────
    # 2번 영역
    # ────────────────────────────────────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>채널별 인게이지먼트 및 액션</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)

    _df_merged = pd.merge(PPL_DATA, PPL_ACTION, on=['날짜', 'utm_camp', 'utm_content'], how='outer')
    df_merged = pd.merge(_df_merged, PPL_LIST, on=['utm_camp', 'utm_content'], how='left')
    df_merged_t = df_merged[[
                            "날짜",
                            "채널명",
                            "Cost",
                            "조회수",
                            "좋아요수",
                            "댓글수",
                            "브랜드언급량",
                            "링크 클릭수", 
                            "session_count",
                            "avg_session_duration_sec",
                            "view_item_sessions",
                            "scroll_50_sessions",
                            "view_item_list_sessions",
                            "product_option_price_sessions",
                            "find_showroom_sessions",
                            "add_to_cart_sessions",
                            "sign_up_sessions",
                            "showroom_10s_sessions",
                            "showroom_leads_sessions",
                            "SearchVolume_contribution"
                        ]]
    # 채널별 데이터프레임 분리
    df_verymj    =  df_merged_t[df_merged_t["채널명"] == "베리엠제이"].copy()
    df_taeyomine =  df_merged_t[df_merged_t["채널명"] == "태요미네"].copy()

    tab1, tab2, tab3 = st.tabs(["전체", "베리엠제이", "태요미네"])
    with tab1:  st.dataframe(df_merged_t)
    with tab2:  st.dataframe(df_verymj)
    with tab3:  st.dataframe(df_taeyomine)
    


    # ────────────────────────────────────────────────────────────────
    # 3번 영역
    # ────────────────────────────────────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>네이버 브랜드 쿼리량</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)

    df = query  

    # 필터 영역
    ft1, ft2, ft3 = st.columns([1, 0.6, 2])
    with ft1: 
        chart_type = st.radio(
            "시각화 유형 선택", 
            ["누적 막대", "누적 영역", "꺾은선"], 
            horizontal=True, 
            index=0
        )
    with ft2:
        date_unit = st.radio(
            "날짜 단위 선택",
            ["일별", "주별"],
            horizontal=True,
            index=0
        )
    with ft3:
        keywords = df['키워드'].unique().tolist()
        sel_keywords = st.multiselect(
            "키워드 선택", 
            keywords, 
            default=['슬립퍼', '슬립퍼매트리스', '슬립퍼프레임', '슬립퍼침대']
        )
        df_f = df[df['키워드'].isin(sel_keywords)]
        
    st.markdown(" ")


    # 탭 영역
    tab_labels = ["RSV", "검색량",  "절대화비율", "보정비율"]
    tabs = st.tabs(tab_labels)
    col_map = {
        "RSV": "RSV",
        "검색량": "검색량",
        "절대화비율": "절대화 비율",
        "보정비율": "보정 비율",
    }

    for i, label in enumerate(tab_labels):
        with tabs[i]:
            y_col = col_map[label]

            # --- 단위별 groupby 및 보간 ---
            if date_unit == "일별":
                x_col = "날짜"
                # ① y_col 을 숫자로 변환 (문자열→NaN→0)
                df_f[y_col] = pd.to_numeric(df_f[y_col], errors="coerce").fillna(0)
                # ② '날짜'·'키워드'별 합계 집계
                plot_df = (
                    df_f
                    .groupby([x_col, "키워드"], as_index=False)[y_col]
                    .sum()
                )
                all_x = pd.date_range(plot_df[x_col].min(), plot_df[x_col].max())
            else:  # 주별
                x_col = "week"
                aggfunc = "sum" if label not in ["절대화비율", "보정비율"] else "mean"
                plot_df = (
                    df_f.groupby([x_col, '키워드'], as_index=False)[y_col].agg(aggfunc)
                )
                all_x = plot_df[x_col].sort_values().unique()

            # ③ MultiIndex 생성 및 재색인
            all_keywords = plot_df['키워드'].unique()
            idx = pd.MultiIndex.from_product([all_x, all_keywords], names=[x_col, "키워드"])
            plot_df = (
                plot_df
                .set_index([x_col, '키워드'])[y_col]
                .reindex(idx, fill_value=0)
                .reset_index()
            )

            # --- 차트 유형별 시각화 ---
            if chart_type == "누적 막대":
                fig = px.bar(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="키워드",
                    barmode="relative",
                    labels={x_col: "날짜" if date_unit == "일별" else "주차", y_col: label, "키워드": "키워드"},
                )
                fig.update_traces(opacity=0.6)

            elif chart_type == "누적 영역":
                fig = px.area(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="키워드",
                    groupnorm="",
                    labels={x_col: "날짜" if date_unit == "일별" else "주차", y_col: label, "키워드": "키워드"},
                )
                fig.update_traces(opacity=0.3)

            elif chart_type == "꺾은선":
                fig = px.line(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="키워드",
                    markers=True,
                    labels={x_col: "날짜" if date_unit == "일별" else "주차", y_col: label, "키워드": "키워드"},
                )
                fig.update_traces(opacity=0.6)
            else:
                fig = None

            if fig:
                st.plotly_chart(fig, use_container_width=True)


    # ────────────────────────────────────────────────────────────────
    # 4번 영역
    # ────────────────────────────────────────────────────────────────
    # 4번 영역
    st.header(" ")
    st.markdown("<h5 style='margin:0'>터치포인트 (기획중)</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)




if __name__ == '__main__':
    main()
