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
    st.subheader('언드 대시보드')
    st.markdown("""
    이 대시보드는 **PPL 채널별 성과 및 기여**를 확인할 수 있는 대시보드입니다.  
    여기서는 각 채널별 **참여 지표**(조회수, 좋아요 등)와 PLP 랜딩 이후 **사용자 행동**을 살펴볼 수 있으며, 전체 검색량 대비 **채널별 쿼리 기여량**을 파악할 수 있습니다.
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
        query      = pd.DataFrame(sh.worksheet('query').get_all_records())
        query_sum  = pd.DataFrame(sh.worksheet('query_sum').get_all_records())
        
        # # 3) tb_sleeper_psi
        # bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        # df_psi = bq.get_data("tb_sleeper_psi")
        # df_psi["event_date"] = pd.to_datetime(df_psi["event_date"], format="%Y%m%d")
        
        return PPL_LIST, PPL_DATA, PPL_ACTION, query, query_sum


    with st.spinner("데이터를 불러오는 중입니다. ⏳"):
        PPL_LIST, PPL_DATA, PPL_ACTION, query, query_sum = load_data()


    # 날짜 컬럼 타입 변환
    # ppl_data['날짜']   = pd.to_datetime(ppl_data['날짜'])
    # ppl_action['날짜'] = pd.to_datetime(ppl_action['날짜'])
    query['날짜']     = pd.to_datetime(query['날짜'])
    

    # ────────────────────────────────────────────────────────────────
    # 시각화
    # ────────────────────────────────────────────────────────────────
    
    def render_aggrid__engag(
        df: pd.DataFrame,
        height: int = 410,
        use_parent: bool = True
        ) -> None:
        """
        use_parent: False / True
        """
        df2 = df.copy()
        
        # (주의) 누락됱 컬럼히 당연히 있을수 있음, 그래서 fillna만 해주는게 아니라 컬럼 자리를 만들어서 fillna 해야함.
        expected_cols = [
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
                        "view_item_list_sessions",
                        "view_item_sessions",
                        "scroll_50_sessions",
                        "product_option_price_sessions",
                        "find_showroom_sessions",
                        "add_to_cart_sessions",
                        "sign_up_sessions",
                        "showroom_10s_sessions",
                        "showroom_leads_sessions",
                        # "SearchVolume_contribution"
        ]
        for col in expected_cols:
            df2[col] = df2.get(col, 0)
        df2.fillna(0, inplace=True)     # (기존과 동일) 값이 없는 경우 일단 0으로 치환
        
        # 전처리 영역 (파생지표 생성) - CPA
        # (생략)
        
        # 전처리 영역 (파생지표 생성) - CVR
        df2['view_item_list_CVR']         = (df2['view_item_list_sessions']     / df2['session_count']          * 100).round(2)
        df2['view_item_CVR']              = (df2['view_item_sessions']              / df2['view_item_list_sessions']          * 100).round(2)
        df2['scroll_50_CVR']              = (df2['scroll_50_sessions']              / df2['view_item_list_sessions']          * 100).round(2)
        df2['product_option_price_CVR']   = (df2['product_option_price_sessions']   / df2['view_item_list_sessions']          * 100).round(2)
        df2['find_nearby_showroom_CVR']   = (df2['find_showroom_sessions']          / df2['view_item_list_sessions']          * 100).round(2)
        df2['add_to_cart_CVR']            = (df2['add_to_cart_sessions']            / df2['view_item_list_sessions']          * 100).round(2)
        df2['sign_up_CVR']                = (df2['sign_up_sessions']                / df2['view_item_list_sessions']          * 100).round(2)
        df2['showroom_10s_CVR']           = (df2['showroom_10s_sessions']           / df2['view_item_list_sessions']          * 100).round(2)
        df2['showroom_leads_CVR']         = (df2['showroom_leads_sessions']         / df2['view_item_list_sessions']          * 100).round(2)

        # 컬럼순서 지정
        # (생략)

        # (필수함수) make_num_child
        def make_num_child(header, field, fmt_digits=0, suffix=''):
            return {
                "headerName": header, "field": field,
                "type": ["numericColumn","customNumericFormat"],
                "valueFormatter": JsCode(
                    f"function(params){{"
                    f"  return params.value!=null?"
                    f"params.value.toLocaleString(undefined,{{minimumFractionDigits:{fmt_digits},maximumFractionDigits:{fmt_digits}}})+'{suffix}':'';"
                    f"}}"
                ),
                "cellStyle": JsCode("params=>({textAlign:'right'})")
            }
        
        # (필수함수) add_summary
        def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]): #'sum'|'avg'|'mid'
            summary: dict[str, float] = {}
            for col, op in agg_map.items():
                if op == 'sum':
                    summary[col] = int(df[col].sum())
                elif op == 'avg':
                    summary[col] = float(df[col].mean())
                elif op == 'mid':
                    summary[col] = float(df[col].median())
                else:
                    summary[col] = "-"  # 에러 발생시, "-"로 표기하고 raise error 하지 않음
            grid_options['pinnedBottomRowData'] = [summary]
            return grid_options
        
        # date_col
        date_col = {
            "headerName": "날짜",
            "field": "날짜",
            "pinned": "left",
            "width": 100,
            "cellStyle": JsCode("params=>({textAlign:'left'})"),
            "sort": "desc"
        }
        # channel_col
        channel_col = {
            "headerName": "채널",
            "field": "채널명",
            "pinned": "left",
            "width": 100,
            "cellStyle": JsCode("params=>({textAlign:'left'})"),
            "sort": "desc"
        }
        
        flat_cols = [
            date_col,
            channel_col,
            make_num_child("일할비용",     "Cost"),
            make_num_child("조회수",       "조회수"),
            make_num_child("좋아요수",     "좋아요수"),
            make_num_child("댓글수",       "댓글수"),
            make_num_child("브랜드언급량",  "브랜드언급량"),
            make_num_child("링크클릭수",   "링크 클릭수"),
            make_num_child("세션수",                       "session_count"),
            make_num_child("avg_session_duration_sec",    "avg_session_duration_sec"),
            make_num_child("PLP조회",                      "view_item_list_sessions"),
            make_num_child("PLP조회 CVR",                  "view_item_list_CVR", fmt_digits=2, suffix="%"),
            make_num_child("PDP조회",                      "view_item_sessions"),
            make_num_child("PDP조회 CVR",                  "view_item_CVR", fmt_digits=2, suffix="%"),
            make_num_child("PDPscr50",                    "scroll_50_sessions"),
            make_num_child("PDPscr50 CVR",                "scroll_50_CVR", fmt_digits=2, suffix="%"),
            make_num_child("가격표시",                      "product_option_price_sessions"),
            make_num_child("가격표시 CVR",                  "product_option_price_CVR", fmt_digits=2, suffix="%"),
            make_num_child("쇼룸찾기",                      "find_showroom_sessions"),
            make_num_child("쇼룸찾기 CVR",                  "find_nearby_showroom_CVR", fmt_digits=2, suffix="%"),
            make_num_child("장바구니",                      "add_to_cart_sessions"),
            make_num_child("장바구니 CVR",                  "add_to_cart_CVR", fmt_digits=2, suffix="%"),
            make_num_child("회원가입",                      "sign_up_sessions"),
            make_num_child("회원가입 CVR",                  "sign_up_CVR", fmt_digits=2, suffix="%"),
            make_num_child("쇼룸10초",                      "showroom_10s_sessions"),
            make_num_child("쇼룸10초 CVR",                  "showroom_10s_CVR", fmt_digits=2, suffix="%"),
            make_num_child("쇼룸예약",                      "showroom_leads_sessions"),
            make_num_child("쇼룸예약 CVR",                  "showroom_leads_CVR", fmt_digits=2, suffix="%"),
        ]

        # (use_parent) grouped_cols
        grouped_cols = [
            date_col,
            channel_col,
            make_num_child("일할비용", "Cost"),
            # 인게이지먼트
            {
                "headerName": "Engagement",
                "children": [
                    make_num_child("조회수",           "조회수"),
                    make_num_child("좋아요수",         "좋아요수"),
                    make_num_child("댓글수",           "댓글수"),
                    make_num_child("브랜드언급량",      "브랜드언급량"),
                    make_num_child("링크클릭수",        "링크 클릭수"),
                ]
            },
            # GA 후속 액션
            {
                "headerName": "GA Actions",
                "children": [
                    make_num_child("세션수",                       "session_count"),
                    make_num_child("avg_session_duration_sec",    "avg_session_duration_sec"),
                    make_num_child("PLP조회",                      "view_item_list_sessions"),
                    make_num_child("PLP조회 CVR",                  "view_item_list_CVR", fmt_digits=2, suffix="%"),
                    make_num_child("PDP조회",                      "view_item_sessions"),
                    make_num_child("PDP조회 CVR",                  "view_item_CVR", fmt_digits=2, suffix="%"),
                    make_num_child("PDPscr50",                    "scroll_50_sessions"),
                    make_num_child("PDPscr50 CVR",                "scroll_50_CVR", fmt_digits=2, suffix="%"),
                    make_num_child("가격표시",                      "product_option_price_sessions"),
                    make_num_child("가격표시 CVR",                  "product_option_price_CVR", fmt_digits=2, suffix="%"),
                    make_num_child("쇼룸찾기",                      "find_showroom_sessions"),
                    make_num_child("쇼룸찾기 CVR",                  "find_nearby_showroom_CVR", fmt_digits=2, suffix="%"),
                    make_num_child("장바구니",                      "add_to_cart_sessions"),
                    make_num_child("장바구니 CVR",                  "add_to_cart_CVR", fmt_digits=2, suffix="%"),
                    make_num_child("회원가입",                      "sign_up_sessions"),
                    make_num_child("회원가입 CVR",                  "sign_up_CVR", fmt_digits=2, suffix="%"),
                    make_num_child("쇼룸10초",                      "showroom_10s_sessions"),
                    make_num_child("쇼룸10초 CVR",                  "showroom_10s_CVR", fmt_digits=2, suffix="%"),
                    make_num_child("쇼룸예약",                      "showroom_leads_sessions"),
                    make_num_child("쇼룸예약 CVR",                  "showroom_leads_CVR", fmt_digits=2, suffix="%"),
                ]
            },
        ]

        # (use_parent)
        column_defs = grouped_cols if use_parent else flat_cols
    
        # grid_options & 렌더링
        grid_options = {
        "columnDefs": column_defs,
        "defaultColDef": {
            "sortable": True,
            "filter": True,
            "resizable": True,
            "flex": 1,       # flex:1 이면 나머지 공간을 컬럼 개수만큼 균등 분배
            "minWidth": 90,   # 최소 너비
            "wrapHeaderText": True,
            "autoHeaderHeight": True
        },
        "onGridReady": JsCode(
            "function(params){ params.api.sizeColumnsToFit(); }"
        ),
        "headerHeight": 60,
        "groupHeaderHeight": 30,
        }        

        # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
        def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]):
            summary: dict[str, float | str] = {}
            for col, op in agg_map.items():
                val = None
                try:
                    if op == 'sum':
                        val = df[col].sum()
                    elif op == 'avg':
                        val = df[col].mean()
                    elif op == 'mid':
                        val = df[col].median()
                except:
                    val = None

                # NaN / Inf / numpy 타입 → None or native 타입으로 처리
                if val is None or isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    summary[col] = None
                else:
                    # numpy 타입 제거
                    if isinstance(val, (np.integer, np.int64, np.int32)):
                        summary[col] = int(val)
                    elif isinstance(val, (np.floating, np.float64, np.float32)):
                        summary[col] = float(round(val, 2))
                    else:
                        summary[col] = val

            grid_options['pinnedBottomRowData'] = [summary]
            return grid_options
        
        # AgGrid(
        #     df2,
        #     gridOptions=grid_options,
        #     height=height,
        #     fit_columns_on_grid_load=False,  # True면 전체넓이에서 균등분배 
        #     theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
        #     allow_unsafe_jscode=True
        # )


        # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
        grid_options = add_summary(
            grid_options,
            df2,
            {
                '조회수': 'sum',
                '좋아요수': 'sum',
                '댓글수': 'sum',
                '브랜드언급량': 'sum',
                '링크 클릭수': 'sum',
                'session_count': 'sum',
                'avg_session_duration_sec': 'avg',
                'view_item_list_sessions': 'sum',
                'view_item_list_CVR': 'avg',
                'view_item_sessions': 'sum',
                'view_item_CVR': 'avg',
                'scroll_50_sessions': 'sum',
                'scroll_50_CVR': 'avg',
                'product_option_price_sessions': 'sum',
                'product_option_price_CVR': 'avg',
                'find_showroom_sessions': 'sum',
                'find_nearby_showroom_CVR': 'avg',
                'add_to_cart_sessions': 'sum',
                'add_to_cart_CVR': 'avg',
                'sign_up_sessions': 'sum',
                'sign_up_CVR': 'avg',
                'showroom_10s_sessions': 'sum',
                'showroom_10s_CVR': 'avg',
                'showroom_leads_sessions': 'sum',
                'showroom_leads_CVR': 'avg',
            }
        )
        
        AgGrid(
            df2,
            gridOptions=grid_options,
            height=height,
            fit_columns_on_grid_load=False,  # True면 전체넓이에서 균등분배 
            theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
            allow_unsafe_jscode=True
        )
    
    

    def render_aggrid__contri(
        df: pd.DataFrame,
        height: int = 410,
        use_parent: bool = True
        ) -> None:
        """
        use_parent: False / True
        """
        df2 = df.copy()
        
        # (주의) 누락됱 컬럼히 당연히 있을수 있음, 그래서 fillna만 해주는게 아니라 컬럼 자리를 만들어서 fillna 해야함.
        expected_cols = ['날짜', '검색량', '검색량차집합', '검색량차집합_비중', '베리엠제이', '베리엠제이_비중', '태요미네', '태요미네_비중', '노홍철 유튜브', '노홍철 유튜브_비중']
        
        for col in expected_cols:
            df2[col] = df2.get(col, 0)
        df2.fillna(0, inplace=True)     # (기존과 동일) 값이 없는 경우 일단 0으로 치환

        # (필수함수) make_num_child
        def make_num_child(header, field, fmt_digits=0, suffix=''):
            return {
                "headerName": header, "field": field,
                "type": ["numericColumn","customNumericFormat"],
                "valueFormatter": JsCode(
                    f"function(params){{"
                    f"  return params.value!=null?"
                    f"params.value.toLocaleString(undefined,{{minimumFractionDigits:{fmt_digits},maximumFractionDigits:{fmt_digits}}})+'{suffix}':'';"
                    f"}}"
                ),
                "cellStyle": JsCode("params=>({textAlign:'right'})")
            }
        
        # (필수함수) add_summary
        def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]): #'sum'|'avg'|'mid'
            summary: dict[str, float] = {}
            for col, op in agg_map.items():
                if op == 'sum':
                    summary[col] = int(df[col].sum())
                elif op == 'avg':
                    summary[col] = float(df[col].mean())
                elif op == 'mid':
                    summary[col] = float(df[col].median())
                else:
                    summary[col] = "-"  # 에러 발생시, "-"로 표기하고 raise error 하지 않음
            grid_options['pinnedBottomRowData'] = [summary]
            return grid_options
        
        # date_col
        date_col = {
            "headerName": "날짜",
            "field": "날짜",
            "pinned": "left",
            "width": 100,
            "cellStyle": JsCode("params=>({textAlign:'left'})"),
            "sort": "desc"
        }
        
        flat_cols = [
        ]

        # (use_parent) grouped_cols
        grouped_cols = [
            date_col,
            make_num_child("전체 검색량", "검색량"),
            # 검색량 차집합
            {
                "headerName": "일반 검색량",
                "children": [
                    make_num_child("검색량",      "검색량차집합"),
                    make_num_child("비중(%)",     "검색량차집합_비중", fmt_digits=2, suffix="%"),
                ]
            },
            # 베리엠제이
            {
                "headerName": "베리엠제이",
                "children": [
                    make_num_child("검색량",      "베리엠제이"),
                    make_num_child("비중(%)",     "베리엠제이_비중", fmt_digits=2, suffix="%"),
                ]
            },
            # 태요미네
            {
                "headerName": "태요미네",
                "children": [
                    make_num_child("검색량",      "태요미네"),
                    make_num_child("비중(%)",     "태요미네_비중", fmt_digits=2, suffix="%"),
                ]
            },
            # 노홍철 유튜브
            {
                "headerName": "노홍철 유튜브",
                "children": [
                    make_num_child("검색량",      "노홍철 유튜브"),
                    make_num_child("비중(%)",     "노홍철 유튜브_비중", fmt_digits=2, suffix="%"),
                ]
            },
        ]

        # (use_parent)
        column_defs = grouped_cols if use_parent else flat_cols
    
        # grid_options & 렌더링
        grid_options = {
        "columnDefs": column_defs,
        "defaultColDef": {
            "sortable": True,
            "filter": True,
            "resizable": True,
            "flex": 1,       # flex:1 이면 나머지 공간을 컬럼 개수만큼 균등 분배
            "minWidth": 90,   # 최소 너비
            "wrapHeaderText": True,
            "autoHeaderHeight": True
        },
        "onGridReady": JsCode(
            "function(params){ params.api.sizeColumnsToFit(); }"
        ),
        "headerHeight": 30,
        "groupHeaderHeight": 30,
        }        

        # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
        def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]):
            summary: dict[str, float | str] = {}
            for col, op in agg_map.items():
                val = None
                try:
                    if op == 'sum':
                        val = df[col].sum()
                    elif op == 'avg':
                        val = df[col].mean()
                    elif op == 'mid':
                        val = df[col].median()
                except:
                    val = None

                # NaN / Inf / numpy 타입 → None or native 타입으로 처리
                if val is None or isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    summary[col] = None
                else:
                    # numpy 타입 제거
                    if isinstance(val, (np.integer, np.int64, np.int32)):
                        summary[col] = int(val)
                    elif isinstance(val, (np.floating, np.float64, np.float32)):
                        summary[col] = float(round(val, 2))
                    else:
                        summary[col] = val

            grid_options['pinnedBottomRowData'] = [summary]
            return grid_options

        # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
        grid_options = add_summary(
            grid_options,
            df2,
            {
                '검색량': 'sum',
                '검색량차집합': 'sum',
                '베리엠제이': 'sum',
                '태요미네': 'sum',
                '노홍철 유튜브': 'sum',
            }
        )
        
        AgGrid(
            df2,
            gridOptions=grid_options,
            height=height,
            fit_columns_on_grid_load=False,  # True면 전체넓이에서 균등분배 
            theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
            allow_unsafe_jscode=True
        )
    
    def render_stacked_bar(
        df: pd.DataFrame,
        x: str,
        y: str | list[str],
        color: str,
        ) -> None:

        # y가 단일 문자열이면 리스트로 감싸기
        y_cols = [y] if isinstance(y, str) else y

        fig = px.bar(
            df,
            x=x,
            y=y_cols,
            color=color,
            labels={"variable": ""},
            opacity=0.6,
            barmode="stack",
        )
        fig.update_layout(
            bargap=0.1,        # 카테고리 간 간격 (0~1)
            bargroupgap=0.2,   # 같은 카테고리 내 막대 간 간격 (0~1)
            height=400,
            xaxis_title=None,
            yaxis_title=None,
            legend=dict(
                orientation="h",
                y=1.02,
                x=1,
                xanchor="right",
                yanchor="bottom",
                title=None
            )
        )
        fig.update_xaxes(tickformat="%m월 %d일")
        st.plotly_chart(fig, use_container_width=True)

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
    st.markdown(":gray-badge[:material/Info: Info]ㅤ집행 중인 모든 채널의 시작일과 금액을 확인할 수 있습니다.", unsafe_allow_html=True)
    
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
                        <div style="color:#333;">집행금액ㅤ<strong>{int(row['금액']):,}원</strong></div>
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
    st.markdown(":gray-badge[:material/Info: Info]ㅤ날짜별, **인게이지먼트** (참여 및 반응 데이터), **세션수 및 주요 액션별 효율** (GA 데이터)을 표에서 확인할 수 있습니다.", unsafe_allow_html=True)

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
                            "view_item_list_sessions",
                            "view_item_sessions",
                            "scroll_50_sessions",
                            "product_option_price_sessions",
                            "find_showroom_sessions",
                            "add_to_cart_sessions",
                            "sign_up_sessions",
                            "showroom_10s_sessions",
                            "showroom_leads_sessions",
                            # "SearchVolume_contribution"
                        ]]
    # 채널별 데이터프레임 분리
    df_verymj    =  df_merged_t[df_merged_t["채널명"] == "베리엠제이"].copy()
    df_taeyomine =  df_merged_t[df_merged_t["채널명"] == "태요미네"].copy()

    tab1, tab2, tab3 = st.tabs(["전체", "베리엠제이", "태요미네"])
    with tab1:
        render_aggrid__engag(df_merged_t)
    with tab2:
        render_aggrid__engag(df_verymj)
    with tab3:
        render_aggrid__engag(df_taeyomine)
    


    # ────────────────────────────────────────────────────────────────
    # 3번 영역
    # ────────────────────────────────────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>채널별 쿼리 기여량</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ“쿼리 기여량”은 전체 검색량 중에서 **각 PPL 채널이 유도했다고 판단되는 검색 수**를 의미합니다.", unsafe_allow_html=True)


    # 1. 전체 쿼리량 대비 기여 쿼리량
    query_sum                                                                       # 전체 쿼리량 = 날짜, 검색량
    ppl_action2 = PPL_ACTION[['날짜', 'utm_content', 'SearchVolume_contribution']]   # 기여 쿼리량 = 날짜, utm_content, SearchVolume_contribution
    ppl_action3 = pd.merge(ppl_action2, PPL_LIST, on=['utm_content'], how='left')   # utm_content가 너무 복잡하니까 채널명으로 변경
    ppl_action3 = ppl_action3[['날짜', '채널명', 'SearchVolume_contribution']]        # utm_content 안녕~
    ppl_action3 = ppl_action3.pivot_table(index="날짜", columns="채널명", values="SearchVolume_contribution", aggfunc="sum").reset_index() # 멜팅
    df_QueryContribution = ppl_action3.merge(query_sum[['날짜', '검색량']], on='날짜', how='left')  # 데이터 생성
    # 데이터 전처리 1
    cols_to_int = ['베리엠제이', '태요미네', '노홍철 유튜브', '검색량']
    df_QueryContribution[cols_to_int] = df_QueryContribution[cols_to_int].apply(
        lambda s: pd.to_numeric(s, errors='coerce')   # 숫자로 변환, 에러나면 NaN
                    .fillna(0)                        # NaN → 0
                    .astype(int)                      # int 로 캐스팅
    )
    # 신규컬럼 생성 - 검색량차집합
    df_QueryContribution["검색량차집합"] = df_QueryContribution["검색량"] - df_QueryContribution[['베리엠제이','태요미네','노홍철 유튜브']].sum(axis=1)
    # 신규컬럼 생성 - 비중
    cols = ['노홍철 유튜브', '베리엠제이', '태요미네', '검색량차집합']
    for col in cols:
        df_QueryContribution[f"{col}_비중"] = (
            df_QueryContribution[col] / df_QueryContribution['검색량'] * 100
        ).round(2)
    df_QueryContribution[[f"{c}_비중" for c in cols]] = df_QueryContribution[[f"{c}_비중" for c in cols]].fillna(0) # 다시 검색량이 0이었던 곳은 0% 처리
    df_QueryContribution = df_QueryContribution[['날짜', '검색량', '검색량차집합', '검색량차집합_비중', '베리엠제이', '베리엠제이_비중', '태요미네', '태요미네_비중', '노홍철 유튜브', '노홍철 유튜브_비중']]
    df_QueryContribution = df_QueryContribution.sort_values("날짜", ascending=True)
        
    # 렌더링 (그래프, 표)
    df_long = df_QueryContribution.melt(
        id_vars='날짜',
        value_vars=cols,
        var_name='콘텐츠',
        value_name='기여량'
    )
    render_stacked_bar(df_long, x="날짜", y="기여량", color="콘텐츠")
    render_aggrid__contri(df_QueryContribution)
    
    
    # ────────────────────────────────────────────────────────────────
    # 4번 영역
    # ────────────────────────────────────────────────────────────────
    # 4번 영역
    st.header(" ")
    st.markdown("<h5 style='margin:0'>키워드별 쿼리량 (기획중)</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)

    # df = query  

    # # 필터 영역
    # ft1, ft2, ft3 = st.columns([1, 0.6, 2])
    # with ft1: 
    #     chart_type = st.radio(
    #         "시각화 유형 선택", 
    #         ["누적 막대", "누적 영역", "꺾은선"], 
    #         horizontal=True, 
    #         index=0
    #     )
    # with ft2:
    #     date_unit = st.radio(
    #         "날짜 단위 선택",
    #         ["일별", "주별"],
    #         horizontal=True,
    #         index=0
    #     )
    # with ft3:
    #     keywords = df['키워드'].unique().tolist()
    #     sel_keywords = st.multiselect(
    #         "키워드 선택", 
    #         keywords, 
    #         default=['슬립퍼', '슬립퍼매트리스', '슬립퍼프레임', '슬립퍼침대']
    #     )
    #     df_f = df[df['키워드'].isin(sel_keywords)]
        
    # st.markdown(" ")


    # # 탭 영역
    # tab_labels = ["RSV", "검색량",  "절대화비율", "보정비율"]
    # tabs = st.tabs(tab_labels)
    # col_map = {
    #     "RSV": "RSV",
    #     "검색량": "검색량",
    #     "절대화비율": "절대화 비율",
    #     "보정비율": "보정 비율",
    # }

    # for i, label in enumerate(tab_labels):
    #     with tabs[i]:
    #         y_col = col_map[label]

    #         # --- 단위별 groupby 및 보간 ---
    #         if date_unit == "일별":
    #             x_col = "날짜"
    #             # ① y_col 을 숫자로 변환 (문자열→NaN→0)
    #             df_f[y_col] = pd.to_numeric(df_f[y_col], errors="coerce").fillna(0)
    #             # ② '날짜'·'키워드'별 합계 집계
    #             plot_df = (
    #                 df_f
    #                 .groupby([x_col, "키워드"], as_index=False)[y_col]
    #                 .sum()
    #             )
    #             all_x = pd.date_range(plot_df[x_col].min(), plot_df[x_col].max())
    #         else:  # 주별
    #             x_col = "week"
    #             aggfunc = "sum" if label not in ["절대화비율", "보정비율"] else "mean"
    #             plot_df = (
    #                 df_f.groupby([x_col, '키워드'], as_index=False)[y_col].agg(aggfunc)
    #             )
    #             all_x = plot_df[x_col].sort_values().unique()

    #         # ③ MultiIndex 생성 및 재색인
    #         all_keywords = plot_df['키워드'].unique()
    #         idx = pd.MultiIndex.from_product([all_x, all_keywords], names=[x_col, "키워드"])
    #         plot_df = (
    #             plot_df
    #             .set_index([x_col, '키워드'])[y_col]
    #             .reindex(idx, fill_value=0)
    #             .reset_index()
    #         )

    #         # --- 차트 유형별 시각화 ---
    #         if chart_type == "누적 막대":
    #             fig = px.bar(
    #                 plot_df,
    #                 x=x_col,
    #                 y=y_col,
    #                 color="키워드",
    #                 barmode="relative",
    #                 labels={x_col: "날짜" if date_unit == "일별" else "주차", y_col: label, "키워드": "키워드"},
    #             )
    #             fig.update_traces(opacity=0.6)

    #         elif chart_type == "누적 영역":
    #             fig = px.area(
    #                 plot_df,
    #                 x=x_col,
    #                 y=y_col,
    #                 color="키워드",
    #                 groupnorm="",
    #                 labels={x_col: "날짜" if date_unit == "일별" else "주차", y_col: label, "키워드": "키워드"},
    #             )
    #             fig.update_traces(opacity=0.3)

    #         elif chart_type == "꺾은선":
    #             fig = px.line(
    #                 plot_df,
    #                 x=x_col,
    #                 y=y_col,
    #                 color="키워드",
    #                 markers=True,
    #                 labels={x_col: "날짜" if date_unit == "일별" else "주차", y_col: label, "키워드": "키워드"},
    #             )
    #             fig.update_traces(opacity=0.6)
    #         else:
    #             fig = None

    #         if fig:
    #             st.plotly_chart(fig, use_container_width=True)


    # ────────────────────────────────────────────────────────────────
    # 5번 영역
    # ────────────────────────────────────────────────────────────────
    # 5번 영역
    st.header(" ")
    st.markdown("<h5 style='margin:0'>터치포인트 (기획중)</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)




if __name__ == '__main__':
    main()
