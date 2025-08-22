# 서희_최신수정일_25-08-19

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
                padding-top: 4rem;   /* 위쪽 여백 */
                padding-bottom: 8rem;
                padding-left: 5rem; 
                padding-right: 4rem; 
            }
        </style>
        """,
        unsafe_allow_html=True
    )  

    st.subheader('언드·PPL 대시보드')
    st.markdown(
        """
        <div style="
            color:#6c757d;        
            font-size:14px;       
            line-height:1.5;      
        ">
        이 대시보드는 <b>PPL 채널별 성과 및 기여</b>를 확인할 수 있는 대시보드입니다.<br>
        여기서는 <b>채널별 참여 지표</b>와, 
        랜딩 이후의 <b>사용자 행동</b>을 살펴볼 수 있으며, 
        전체 검색량 대비 <b>채널별 쿼리 기여량</b>을 파악할 수 있습니다.
        </div>
        """,
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

        try: 
            creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
        except: # 배포용 (secrets.toml)
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str):  # 혹시 문자열(JSON)로 저장했을 경우
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1Li4YzwsxI7rB3Q2Z0gkuGIyANTaxFrVzgsKE-RAAdME/edit?gid=2078920702#gid=2078920702')
        
        # 데이터 시트별로 불러오자.
        PPL_LIST   = pd.DataFrame(sh.worksheet('PPL_LIST').get_all_records())
        PPL_DATA   = pd.DataFrame(sh.worksheet('PPL_DATA').get_all_records())
        # PPL_ACTION = pd.DataFrame(sh.worksheet('PPL_ACTION').get_all_records())
        # --------------------------------------------------------------
        wsa = sh.worksheet('PPL_ACTION')
        data = wsa.get('A1:P')  # A열~P열까지 전체
        PPL_ACTION = pd.DataFrame(data[1:], columns=data[0])  # 1행=헤더
        # --------------------------------------------------------------
        query_slp      = pd.DataFrame(sh.worksheet('query_슬립퍼').get_all_records())
        query_nor      = pd.DataFrame(sh.worksheet('query_누어').get_all_records())
        query_sum  = pd.DataFrame(sh.worksheet('query_sum').get_all_records())

        
        # # 3) tb_sleeper_psi
        # bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        # df_psi = bq.get_data("tb_sleeper_psi")
        # df_psi["event_date"] = pd.to_datetime(df_psi["event_date"], format="%Y%m%d")
        
        return PPL_LIST, PPL_DATA, PPL_ACTION, query_slp, query_nor, query_sum


    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        PPL_LIST, PPL_DATA, PPL_ACTION, query_slp, query_nor, query_sum = load_data()


    # 날짜 컬럼 타입 변환
    # ppl_data['날짜']   = pd.to_datetime(ppl_data['날짜'])
    # ppl_action['날짜'] = pd.to_datetime(ppl_action['날짜'])
    query_slp['날짜']   = pd.to_datetime(query_slp['날짜'])
    query_nor['날짜']   = pd.to_datetime(query_nor['날짜'])
    
    
    # 모든 데이터프레임이 동일한 파생 지표를 가...지지 않음
    # => "채널별 인게이지먼트 및 액션"용
    def decorate_df_eng(df: pd.DataFrame,
                    select_option: int = 1,) -> None:
        # 키에러방지
        required = ["날짜",
                    "채널명",
                    "Cost",
                    "조회수","좋아요수","댓글수","브랜드언급량","링크 클릭수", 
                    "session_count","avg_session_duration_sec","view_item_list_sessions","view_item_sessions","scroll_50_sessions","product_option_price_sessions","find_showroom_sessions","add_to_cart_sessions","sign_up_sessions","showroom_10s_sessions","showroom_leads_sessions",
                    ]
        for c in required:
            if c not in df.columns:
                df[c] = 0  
        num_cols = ["Cost",
                    "조회수","좋아요수","댓글수","브랜드언급량","링크 클릭수", 
                    "session_count","avg_session_duration_sec","view_item_list_sessions","view_item_sessions","scroll_50_sessions","product_option_price_sessions","find_showroom_sessions","add_to_cart_sessions","sign_up_sessions","showroom_10s_sessions","showroom_leads_sessions",
                    ]
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

        # 파생지표 생성 - CVR
        df['view_item_list_CVR']         = (df['view_item_list_sessions']     / df['session_count']          * 100).round(2)
        df['view_item_CVR']              = (df['view_item_sessions']              / df['view_item_list_sessions']          * 100).round(2)
        df['scroll_50_CVR']              = (df['scroll_50_sessions']              / df['view_item_list_sessions']          * 100).round(2)
        df['product_option_price_CVR']   = (df['product_option_price_sessions']   / df['view_item_list_sessions']          * 100).round(2)
        df['find_nearby_showroom_CVR']   = (df['find_showroom_sessions']          / df['view_item_list_sessions']          * 100).round(2)
        df['add_to_cart_CVR']            = (df['add_to_cart_sessions']            / df['view_item_list_sessions']          * 100).round(2)
        df['sign_up_CVR']                = (df['sign_up_sessions']                / df['view_item_list_sessions']          * 100).round(2)
        df['showroom_10s_CVR']           = (df['showroom_10s_sessions']           / df['view_item_list_sessions']          * 100).round(2)
        df['showroom_leads_CVR']         = (df['showroom_leads_sessions']         / df['view_item_list_sessions']          * 100).round(2)
        # 파생지표 생성 - CPA
        df['view_item_list_CPA']         = (df['Cost']     /  df['view_item_list_sessions']          * 100).round(0)
        df['view_item_CPA']              = (df['Cost']     /  df['view_item_sessions']               * 100).round(0)
        df['scroll_50_CPA']              = (df['Cost']     /  df['scroll_50_sessions']               * 100).round(0)
        df['product_option_price_CPA']   = (df['Cost']     /  df['product_option_price_sessions']    * 100).round(0)
        df['find_nearby_showroom_CPA']   = (df['Cost']     /  df['find_showroom_sessions']           * 100).round(0)
        df['add_to_cart_CPA']            = (df['Cost']     /  df['add_to_cart_sessions']             * 100).round(0)
        df['sign_up_CPA']                = (df['Cost']     /  df['sign_up_sessions']                 * 100).round(0)
        df['showroom_10s_CPA']           = (df['Cost']     /  df['showroom_10s_sessions']            * 100).round(0)
        df['showroom_leads_CPA']         = (df['Cost']     /  df['showroom_leads_sessions']          * 100).round(0)
        
        # 컬럼 순서 지정
        if select_option == 1: # min
            df = df[["날짜",
                    "채널명",
                    "Cost",
                    "조회수","좋아요수","댓글수","브랜드언급량","링크 클릭수", 
                    "session_count","avg_session_duration_sec",
                    "view_item_list_sessions",
                    "view_item_sessions",
                    "scroll_50_sessions",
                    "product_option_price_sessions",
                    "find_showroom_sessions",
                    "add_to_cart_sessions",
                    "sign_up_sessions",
                    "showroom_10s_sessions",
                    "showroom_leads_sessions"
                    ]]
        elif select_option == 2: # CVR
            df = df[["날짜",
                    "채널명",
                    "Cost",
                    "조회수","좋아요수","댓글수","브랜드언급량","링크 클릭수", 
                    "session_count","avg_session_duration_sec",
                    "view_item_list_sessions","view_item_list_CVR",
                    "view_item_sessions","view_item_CVR",
                    "scroll_50_sessions","scroll_50_CVR",
                    "product_option_price_sessions","product_option_price_CVR",
                    "find_showroom_sessions","find_nearby_showroom_CVR",
                    "add_to_cart_sessions","add_to_cart_CVR",
                    "sign_up_sessions","sign_up_CVR",
                    "showroom_10s_sessions","showroom_10s_CVR",
                    "showroom_leads_sessions","showroom_leads_CVR"
                    ]]
        elif select_option == 3: # CPA
            df = df[["날짜",
                    "채널명",
                    "Cost",
                    "조회수","좋아요수","댓글수","브랜드언급량","링크 클릭수", 
                    "session_count","avg_session_duration_sec",
                    "view_item_list_sessions","view_item_list_CPA",
                    "view_item_sessions","view_item_CPA",
                    "scroll_50_sessions","scroll_50_CPA",
                    "product_option_price_sessions","product_option_price_CPA",
                    "find_showroom_sessions","find_nearby_showroom_CPA",
                    "add_to_cart_sessions","add_to_cart_CPA",
                    "sign_up_sessions","sign_up_CPA",
                    "showroom_10s_sessions","showroom_10s_CPA",
                    "showroom_leads_sessions","showroom_leads_CPA"
                    ]]
        elif select_option == 4: # max
            df = df[["날짜",
                    "채널명",
                    "Cost",
                    "조회수","좋아요수","댓글수","브랜드언급량","링크 클릭수", 
                    "session_count","avg_session_duration_sec",
                    "view_item_list_sessions","view_item_list_CVR", "view_item_list_CPA",
                    "view_item_sessions","view_item_CVR","view_item_CPA",
                    "scroll_50_sessions","scroll_50_CVR","scroll_50_CPA",
                    "product_option_price_sessions","product_option_price_CVR","product_option_price_CPA",
                    "find_showroom_sessions","find_nearby_showroom_CVR","find_nearby_showroom_CPA",
                    "add_to_cart_sessions","add_to_cart_CVR","add_to_cart_CPA",
                    "sign_up_sessions","sign_up_CVR","sign_up_CPA",
                    "showroom_10s_sessions","showroom_10s_CVR","showroom_10s_CPA",
                    "showroom_leads_sessions","showroom_leads_CVR","showroom_leads_CPA"
                    ]]

        # 자료형 워싱 (event_date 아님)
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
        num_cols = df.select_dtypes(include=['number']).columns
        df[num_cols] = (df[num_cols].replace([np.inf, -np.inf], np.nan).fillna(0))    
        
        # 컬럼 이름 변경 - 멀티 인덱스
        if select_option == 1: # min 
            df.columns = pd.MultiIndex.from_tuples([
                ("기본정보",      "날짜"),             # 
                ("기본정보",      "채널명"),            # 
                ("COST",         "일할비용"),          # 
                ("ENGAGEMENT",   "조회수"),            # 
                ("ENGAGEMENT",   "좋아요수"),          # 
                ("ENGAGEMENT",   "댓글수"),            # 
                ("ENGAGEMENT",   "브랜드언급량"),       # 
                ("ENGAGEMENT",   "링크클릭수"),         # 
                ("GA",    "유입 세션수"),       # session_count
                ("GA",    "평균세션시간(초)"),   # avg_session_duration_sec
                ("PLP조회",    "Acutal"),          # view_item_list_sessions
                ("PDP조회",    "Acutal"),          # view_item_sessions
                ("PDPscr50",    "Acutal"),         # scroll_50_sessions
                ("가격표시",    "Acutal"),          # product_option_price_sessions
                ("쇼룸찾기",    "Acutal"),          # find_showroom_sessions
                ("장바구니",    "Acutal"),          # add_to_cart_sessions
                ("회원가입",    "Acutal"),          # sign_up_sessions
                ("쇼룸10초",    "Acutal"),          # showroom_10s_sessions
                ("쇼룸예약",    "Acutal"),          # showroom_leads_sessions
            ], names=["그룹","지표"])  # 상단 레벨 이름(옵션)   
        elif select_option == 2: # CVR
            df.columns = pd.MultiIndex.from_tuples([
                ("기본정보",      "날짜"),             # 
                ("기본정보",      "채널명"),            # 
                ("COST",         "일할비용"),          # 
                ("ENGAGEMENT",   "조회수"),            # 
                ("ENGAGEMENT",   "좋아요수"),          # 
                ("ENGAGEMENT",   "댓글수"),            # 
                ("ENGAGEMENT",   "브랜드언급량"),       # 
                ("ENGAGEMENT",   "링크클릭수"),         # 
                ("GA",    "유입 세션수"),       # session_count
                ("GA",    "평균세션시간(초)"),   # avg_session_duration_sec
                ("PLP조회",    "Acutal"),          # view_item_list_sessions
                ("PLP조회",    "CVR"),     
                ("PDP조회",    "Acutal"),          # view_item_sessions
                ("PDP조회",    "CVR"),  
                ("PDPscr50",    "Acutal"),         # scroll_50_sessions
                ("PDPscr50",    "CVR"),    
                ("가격표시",    "Acutal"),          # product_option_price_sessions
                ("가격표시",    "CVR"),   
                ("쇼룸찾기",    "Acutal"),          # find_showroom_sessions
                ("쇼룸찾기",    "CVR"), 
                ("장바구니",    "Acutal"),          # add_to_cart_sessions
                ("장바구니",    "CVR"),
                ("회원가입",    "Acutal"),          # sign_up_sessions
                ("회원가입",    "CVR"),  
                ("쇼룸10초",    "Acutal"),          # showroom_10s_sessions
                ("쇼룸10초",    "CVR"),  
                ("쇼룸예약",    "Acutal"),          # showroom_leads_sessions
                ("쇼룸예약",    "CVR"),
            ], names=["그룹","지표"])  # 상단 레벨 이름(옵션)     
        elif select_option == 3: # CPA
            df.columns = pd.MultiIndex.from_tuples([
                ("기본정보",      "날짜"),             # 
                ("기본정보",      "채널명"),            # 
                ("COST",         "일할비용"),          # 
                ("ENGAGEMENT",   "조회수"),            # 
                ("ENGAGEMENT",   "좋아요수"),          # 
                ("ENGAGEMENT",   "댓글수"),            # 
                ("ENGAGEMENT",   "브랜드언급량"),       # 
                ("ENGAGEMENT",   "링크클릭수"),         # 
                ("GA",    "유입 세션수"),       # session_count
                ("GA",    "평균세션시간(초)"),   # avg_session_duration_sec
                ("PLP조회",    "Acutal"),          # view_item_list_sessions
                ("PLP조회",    "CPA"),     
                ("PDP조회",    "Acutal"),          # view_item_sessions
                ("PDP조회",    "CPA"),     
                ("PDPscr50",    "Acutal"),         # scroll_50_sessions
                ("PDPscr50",    "CPA"),     
                ("가격표시",    "Acutal"),          # product_option_price_sessions
                ("가격표시",    "CPA"),    
                ("쇼룸찾기",    "Acutal"),          # find_showroom_sessions
                ("쇼룸찾기",    "CPA"), 
                ("장바구니",    "Acutal"),          # add_to_cart_sessions
                ("장바구니",    "CPA"), 
                ("회원가입",    "Acutal"),          # sign_up_sessions
                ("회원가입",    "CPA"),  
                ("쇼룸10초",    "Acutal"),          # showroom_10s_sessions
                ("쇼룸10초",    "CPA"),  
                ("쇼룸예약",    "Acutal"),          # showroom_leads_sessions
                ("쇼룸예약",    "CPA"), 
            ], names=["그룹","지표"])  # 상단 레벨 이름(옵션) 
        elif select_option == 4: # max
            df.columns = pd.MultiIndex.from_tuples([
                ("기본정보",      "날짜"),             # 
                ("기본정보",      "채널명"),            # 
                ("COST",         "일할비용"),          # 
                ("ENGAGEMENT",   "조회수"),            # 
                ("ENGAGEMENT",   "좋아요수"),          # 
                ("ENGAGEMENT",   "댓글수"),            # 
                ("ENGAGEMENT",   "브랜드언급량"),       # 
                ("ENGAGEMENT",   "링크클릭수"),         # 
                ("GA",    "유입 세션수"),       # session_count
                ("GA",    "평균세션시간(초)"),   # avg_session_duration_sec
                ("PLP조회",    "Acutal"),          # view_item_list_sessions
                ("PLP조회",    "CVR"),     
                ("PLP조회",    "CPA"),     
                ("PDP조회",    "Acutal"),          # view_item_sessions
                ("PDP조회",    "CVR"),  
                ("PDP조회",    "CPA"),     
                ("PDPscr50",    "Acutal"),         # scroll_50_sessions
                ("PDPscr50",    "CVR"),    
                ("PDPscr50",    "CPA"),     
                ("가격표시",    "Acutal"),          # product_option_price_sessions
                ("가격표시",    "CVR"),   
                ("가격표시",    "CPA"),    
                ("쇼룸찾기",    "Acutal"),          # find_showroom_sessions
                ("쇼룸찾기",    "CVR"), 
                ("쇼룸찾기",    "CPA"), 
                ("장바구니",    "Acutal"),          # add_to_cart_sessions
                ("장바구니",    "CVR"),
                ("장바구니",    "CPA"), 
                ("회원가입",    "Acutal"),          # sign_up_sessions
                ("회원가입",    "CVR"),  
                ("회원가입",    "CPA"),  
                ("쇼룸10초",    "Acutal"),          # showroom_10s_sessions
                ("쇼룸10초",    "CVR"),  
                ("쇼룸10초",    "CPA"),  
                ("쇼룸예약",    "Acutal"),          # showroom_leads_sessions
                ("쇼룸예약",    "CVR"),
                ("쇼룸예약",    "CPA"), 
            ], names=["그룹","지표"])  # 상단 레벨 이름(옵션)
        
        return df

    def render_style_eng(target_df, select_option):
        styled = style_format(
            decorate_df_eng(target_df, select_option=opt),
            decimals_map={
                ("COST",         "일할비용"):0,          # 
                ("ENGAGEMENT",   "조회수"):0,            # 
                ("ENGAGEMENT",   "좋아요수"):0,          # 
                ("ENGAGEMENT",   "댓글수"):0,            # 
                ("ENGAGEMENT",   "브랜드언급량"):0,       # 
                ("ENGAGEMENT",   "링크클릭수"):0,         # 
                ("GA",    "유입 세션수"):0,       # session_count
                ("GA",    "평균세션시간(초)"):0,   # avg_session_duration_sec
                ("PLP조회",    "Acutal"):0,          # view_item_list_sessions
                ("PLP조회",    "CVR"):1,     
                ("PLP조회",    "CPA"):0,     
                ("PDP조회",    "Acutal"):0,          # view_item_sessions
                ("PDP조회",    "CVR"):1,  
                ("PDP조회",    "CPA"):0,     
                ("PDPscr50",    "Acutal"):0,         # scroll_50_sessions
                ("PDPscr50",    "CVR"):1,    
                ("PDPscr50",    "CPA"):0,     
                ("가격표시",    "Acutal"):0,          # product_option_price_sessions
                ("가격표시",    "CVR"):1,   
                ("가격표시",    "CPA"):0,    
                ("쇼룸찾기",    "Acutal"):0,          # find_showroom_sessions
                ("쇼룸찾기",    "CVR"):1, 
                ("쇼룸찾기",    "CPA"):0, 
                ("장바구니",    "Acutal"):0,          # add_to_cart_sessions
                ("장바구니",    "CVR"):1,
                ("장바구니",    "CPA"):0, 
                ("회원가입",    "Acutal"):0,          # sign_up_sessions
                ("회원가입",    "CVR"):1,  
                ("회원가입",    "CPA"):0,  
                ("쇼룸10초",    "Acutal"):0,          # showroom_10s_sessions
                ("쇼룸10초",    "CVR"):1,  
                ("쇼룸10초",    "CPA"):0,  
                ("쇼룸예약",    "Acutal"):0,          # showroom_leads_sessions
                ("쇼룸예약",    "CVR"):1,
                ("쇼룸예약",    "CPA"):0, 
            },
            suffix_map={
                ("PLP조회",    "CVR"):" %",     
                ("PDP조회",    "CVR"):" %",  
                ("PDPscr50",    "CVR"):" %",    
                ("가격표시",    "CVR"):" %",   
                ("쇼룸찾기",    "CVR"):" %", 
                ("장바구니",    "CVR"):" %",
                ("회원가입",    "CVR"):" %",  
                ("쇼룸10초",    "CVR"):" %",  
                ("쇼룸예약",    "CVR"):" %",
        }
        )

        st.dataframe(styled, use_container_width=True, row_height=30, hide_index=True)

    # => "채널별 쿼리 기여량"용
    def decorate_df_ctb(df: pd.DataFrame,
                    brand: str = 'sleeper') -> None:
        if brand == "sleeper":
            # 키에러 방지
            required = ['날짜', '검색량', '기본 검색량', '기본 검색량_비중',
                        '태요미네', '태요미네_비중', '노홍철 유튜브', '노홍철 유튜브_비중', '아울디자인', '아울디자인_비중', '알쓸물치', '알쓸물치_비중']            
            for c in required:
                if c not in df.columns:
                    df[c] = 0
            num_cols = ['검색량', '기본 검색량', '기본 검색량_비중',
                        '태요미네', '태요미네_비중', '노홍철 유튜브', '노홍철 유튜브_비중', '아울디자인', '아울디자인_비중', '알쓸물치', '알쓸물치_비중'] 
            df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

            # 컬럼 순서 지정
            df = df[['날짜', '검색량', '기본 검색량', '기본 검색량_비중',
                    '노홍철 유튜브', '노홍철 유튜브_비중', '태요미네', '태요미네_비중',  '아울디자인', '아울디자인_비중', '알쓸물치', '알쓸물치_비중']]
            
            # 자료형 워싱
            df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            num_cols = df.select_dtypes(include=['number']).columns
            df[num_cols] = (df[num_cols].replace([np.inf, -np.inf], np.nan).fillna(0))  
            
            # 컬럼 이름 변경 - 멀티 인덱스
            df.columns = pd.MultiIndex.from_tuples([
                ("기본정보",      "날짜"),        
                ("기본정보",        "전체 검색량"),      
                ("기본 검색량",        "검색량"),         
                ("기본 검색량",        "비중(%)"),  
                ("노홍철 유튜브",        "검색량"),         
                ("노홍철 유튜브",        "비중(%)"), 
                ("태요미네",        "검색량"),         
                ("태요미네",        "비중(%)"),  
                ("아울디자인",        "검색량"),         
                ("아울디자인",        "비중(%)"), 
                ("알쓸물치",        "검색량"),         
                ("알쓸물치",        "비중(%)"),
            ], names=["그룹","지표"])  # 상단 레벨 이름(옵션)  

        elif brand == "nooer":
            # 키에러 방지
            required = ['날짜', '검색량', '기본 검색량', '기본 검색량_비중',
                        '베리엠제이', '베리엠제이_비중']            
            for c in required:
                if c not in df.columns:
                    df[c] = 0
            num_cols = ['검색량', '기본 검색량', '기본 검색량_비중',
                        '베리엠제이', '베리엠제이_비중'] 
            df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

            # 컬럼 순서 지정
            df = df[['날짜', '검색량', '기본 검색량', '기본 검색량_비중',
                    '베리엠제이', '베리엠제이_비중']]
            
            # 자료형 워싱
            df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
            num_cols = df.select_dtypes(include=['number']).columns
            df[num_cols] = (df[num_cols].replace([np.inf, -np.inf], np.nan).fillna(0))  
            
            # 컬럼 이름 변경 - 멀티 인덱스
            df.columns = pd.MultiIndex.from_tuples([
                ("기본정보",      "날짜"),        
                ("기본정보",        "전체 검색량"),      
                ("기본 검색량",        "검색량"),         
                ("기본 검색량",        "비중(%)"),  
                ("베리엠제이",        "검색량"),         
                ("베리엠제이",        "비중(%)"),
            ], names=["그룹","지표"])  # 상단 레벨 이름(옵션) 
        
        return df

    def render_style_ctb(target_df, brand):
        styled = style_format(
            decorate_df_ctb(target_df, brand),
            decimals_map={
                ("기본정보",        "전체 검색량"): 0,
                ("기본 검색량",        "비중(%)"): 1,  
                ("노홍철 유튜브",        "비중(%)"): 1,
                ("태요미네",        "비중(%)"): 1,
                ("아울디자인",        "비중(%)"): 1,
                ("알쓸물치",        "비중(%)"): 1,
                ("베리엠제이",        "비중(%)"): 1,
            },
            suffix_map={
                ("기본 검색량",        "비중(%)"): " %",
                ("기본 검색량",        "비중(%)"): " %",  
                ("노홍철 유튜브",        "비중(%)"): " %",
                ("태요미네",        "비중(%)"): " %",
                ("아울디자인",        "비중(%)"): " %",
                ("알쓸물치",        "비중(%)"): " %",
                ("베리엠제이",        "비중(%)"): " %",
        }
        )
        st.dataframe(styled, use_container_width=True, row_height=30, hide_index=True)

    
    # def render_stacked_bar(
    #     df: pd.DataFrame,
    #     x: str,
    #     y: str | list[str],
    #     color: str,
    #     ) -> None:

    #     # y가 단일 문자열이면 리스트로 감싸기
    #     y_cols = [y] if isinstance(y, str) else y

    #     fig = px.bar(
    #         df,
    #         x=x,
    #         y=y_cols,
    #         color=color,
    #         labels={"variable": ""},
    #         opacity=0.6,
    #         barmode="relative",
    #     )
    #     fig.update_layout(
    #         barmode="relative",
    #         bargap=0.1,        # 카테고리 간 간격 (0~1)
    #         bargroupgap=0.2,   # 같은 카테고리 내 막대 간 간격 (0~1)
    #         height=400,
    #         xaxis_title=None,
    #         yaxis_title=None,
    #         legend=dict(
    #             orientation="h",
    #             y=1.02,
    #             x=1,
    #             xanchor="right",
    #             yanchor="bottom",
    #             title=None
    #         )
    #     )
    #     fig.update_xaxes(tickformat="%m월 %d일")
    #     st.plotly_chart(fig, use_container_width=True)


    def render_stacked_bar(df: pd.DataFrame, x: str, y: str | list[str], color: str | None) -> None:
        # 숫자형 보정
        def _to_numeric(cols):
            for c in cols:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        if isinstance(y, (list, tuple)):   # wide-form 들어오면
            _to_numeric(list(y))
            if color is not None and color in df.columns:
                # y-list + color가 같이 오면 long으로 변환해 확실히 누적
                long_df = df.melt(id_vars=[x, color], value_vars=list(y),
                                var_name="__series__", value_name="__value__")
                fig = px.bar(long_df, x=x, y="__value__", color="__series__", opacity=0.6)
            else:
                fig = px.bar(df, x=x, y=list(y), opacity=0.6)
        else:                               # y가 단일이면 long-form
            _to_numeric([y])
            fig = px.bar(df, x=x, y=y, color=color, opacity=0.6)

        # 핵심: 진짜로 누적시키기
        fig.update_layout(barmode="relative")
        fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))

        fig.update_layout(
            bargap=0.1,
            bargroupgap=0.2,
            height=400,
            xaxis_title=None,
            yaxis_title=None,
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom", title=None),
        )
        fig.update_xaxes(tickformat="%m월 %d일")
        st.plotly_chart(fig, use_container_width=True)















    # def render_aggrid__engag(
    #     df: pd.DataFrame,
    #     height: int = 410,
    #     use_parent: bool = True,
    #     select_option: int = 1,
    #     ) -> None:
    #     """
    #     use_parent: False / True
    #     """
    #     df2 = df.copy()
        
    #     # (주의) 누락됱 컬럼히 당연히 있을수 있음, 그래서 fillna만 해주는게 아니라 컬럼 자리를 만들어서 fillna 해야함.
    #     expected_cols = [
    #                     "날짜",
    #                     "채널명",
    #                     "Cost",
    #                     "조회수",
    #                     "좋아요수",
    #                     "댓글수",
    #                     "브랜드언급량",
    #                     "링크 클릭수", 
    #                     "session_count",
    #                     "avg_session_duration_sec",
    #                     "view_item_list_sessions",
    #                     "view_item_sessions",
    #                     "scroll_50_sessions",
    #                     "product_option_price_sessions",
    #                     "find_showroom_sessions",
    #                     "add_to_cart_sessions",
    #                     "sign_up_sessions",
    #                     "showroom_10s_sessions",
    #                     "showroom_leads_sessions",
    #     ]
    #     for col in expected_cols:
    #         df2[col] = df2.get(col, 0)
    #     df2.fillna(0, inplace=True)     # (기존과 동일) 값이 없는 경우 일단 0으로 치환
        

    #     # 전처리 영역 (파생지표 생성) - CVR
    #     df2['view_item_list_CVR']         = (df2['view_item_list_sessions']     / df2['session_count']          * 100).round(2)
    #     df2['view_item_CVR']              = (df2['view_item_sessions']              / df2['view_item_list_sessions']          * 100).round(2)
    #     df2['scroll_50_CVR']              = (df2['scroll_50_sessions']              / df2['view_item_list_sessions']          * 100).round(2)
    #     df2['product_option_price_CVR']   = (df2['product_option_price_sessions']   / df2['view_item_list_sessions']          * 100).round(2)
    #     df2['find_nearby_showroom_CVR']   = (df2['find_showroom_sessions']          / df2['view_item_list_sessions']          * 100).round(2)
    #     df2['add_to_cart_CVR']            = (df2['add_to_cart_sessions']            / df2['view_item_list_sessions']          * 100).round(2)
    #     df2['sign_up_CVR']                = (df2['sign_up_sessions']                / df2['view_item_list_sessions']          * 100).round(2)
    #     df2['showroom_10s_CVR']           = (df2['showroom_10s_sessions']           / df2['view_item_list_sessions']          * 100).round(2)
    #     df2['showroom_leads_CVR']         = (df2['showroom_leads_sessions']         / df2['view_item_list_sessions']          * 100).round(2)

    #     # 전처리 영역 (파생지표 생성) - CPA
    #     df2['view_item_list_CPA']         = (df2['Cost']     /  df2['view_item_list_sessions']          * 100).round(0)
    #     df2['view_item_CPA']              = (df2['Cost']     /  df2['view_item_sessions']               * 100).round(0)
    #     df2['scroll_50_CPA']              = (df2['Cost']     /  df2['scroll_50_sessions']               * 100).round(0)
    #     df2['product_option_price_CPA']   = (df2['Cost']     /  df2['product_option_price_sessions']    * 100).round(0)
    #     df2['find_nearby_showroom_CPA']   = (df2['Cost']     /  df2['find_showroom_sessions']           * 100).round(0)
    #     df2['add_to_cart_CPA']            = (df2['Cost']     /  df2['add_to_cart_sessions']             * 100).round(0)
    #     df2['sign_up_CPA']                = (df2['Cost']     /  df2['sign_up_sessions']                 * 100).round(0)
    #     df2['showroom_10s_CPA']           = (df2['Cost']     /  df2['showroom_10s_sessions']            * 100).round(0)
    #     df2['showroom_leads_CPA']         = (df2['Cost']     /  df2['showroom_leads_sessions']          * 100).round(0)
        
    #     # 컬럼순서 지정
    #     # (생략)

    #     # (필수함수) make_num_child
    #     def make_num_child(header, field, fmt_digits=0, suffix=''):
    #         return {
    #             "headerName": header, "field": field,
    #             "type": ["numericColumn","customNumericFormat"],
    #             "valueFormatter": JsCode(
    #                 f"function(params){{"
    #                 f"  return params.value!=null?"
    #                 f"params.value.toLocaleString(undefined,{{minimumFractionDigits:{fmt_digits},maximumFractionDigits:{fmt_digits}}})+'{suffix}':'';"
    #                 f"}}"
    #             ),
    #             "cellStyle": JsCode("params=>({textAlign:'right'})")
    #         }
        
    #     # (필수함수) add_summary
    #     def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]): #'sum'|'avg'|'mid'
    #         summary: dict[str, float] = {}
    #         for col, op in agg_map.items():
    #             if op == 'sum':
    #                 summary[col] = int(df[col].sum())
    #             elif op == 'avg':
    #                 summary[col] = float(df[col].mean())
    #             elif op == 'mid':
    #                 summary[col] = float(df[col].median())
    #             else:
    #                 summary[col] = "-"  # 에러 발생시, "-"로 표기하고 raise error 하지 않음
    #         grid_options['pinnedBottomRowData'] = [summary]
    #         return grid_options
        
    #     # date_col
    #     date_col = {
    #         "headerName": "날짜",
    #         "field": "날짜",
    #         "pinned": "left",
    #         "width": 100,
    #         "cellStyle": JsCode("params=>({textAlign:'left'})"),
    #         # "sort": "desc"
    #     }
    #     # channel_col
    #     channel_col = {
    #         "headerName": "채널",
    #         "field": "채널명",
    #         "pinned": "left",
    #         "width": 100,
    #         "cellStyle": JsCode("params=>({textAlign:'left'})"),
    #         # "sort": "desc"
    #     }
        
    #     flat_cols = [
    #         date_col,
    #         channel_col,
    #         make_num_child("일할비용",     "Cost"),
    #         make_num_child("조회수",       "조회수"),
    #         make_num_child("좋아요수",     "좋아요수"),
    #         make_num_child("댓글수",       "댓글수"),
    #         make_num_child("브랜드언급량",  "브랜드언급량"),
    #         make_num_child("링크클릭수",   "링크 클릭수"),
    #         make_num_child("세션수",                       "session_count"),
    #         make_num_child("평균세션시간(초)",    "avg_session_duration_sec"),
    #         make_num_child("PLP조회",                      "view_item_list_sessions"),
    #         make_num_child("PLP조회 CVR",                  "view_item_list_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("PDP조회",                      "view_item_sessions"),
    #         make_num_child("PDP조회 CVR",                  "view_item_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("PDPscr50",                    "scroll_50_sessions"),
    #         make_num_child("PDPscr50 CVR",                "scroll_50_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("가격표시",                      "product_option_price_sessions"),
    #         make_num_child("가격표시 CVR",                  "product_option_price_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("쇼룸찾기",                      "find_showroom_sessions"),
    #         make_num_child("쇼룸찾기 CVR",                  "find_nearby_showroom_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("장바구니",                      "add_to_cart_sessions"),
    #         make_num_child("장바구니 CVR",                  "add_to_cart_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("회원가입",                      "sign_up_sessions"),
    #         make_num_child("회원가입 CVR",                  "sign_up_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("쇼룸10초",                      "showroom_10s_sessions"),
    #         make_num_child("쇼룸10초 CVR",                  "showroom_10s_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("쇼룸예약",                      "showroom_leads_sessions"),
    #         make_num_child("쇼룸예약 CVR",                  "showroom_leads_CVR", fmt_digits=2, suffix="%"),
    #     ]

    #     # (use_parent) grouped_cols
    #     grouped_cols = [
    #         date_col,
    #         channel_col,
    #         make_num_child("일할비용", "Cost"),
    #         # 인게이지먼트
    #         {
    #             "headerName": "Engagement",
    #             "children": [
    #                 make_num_child("조회수",           "조회수"),
    #                 make_num_child("좋아요수",         "좋아요수"),
    #                 make_num_child("댓글수",           "댓글수"),
    #                 make_num_child("브랜드언급량",      "브랜드언급량"),
    #                 make_num_child("링크클릭수",        "링크 클릭수"),
    #             ]
    #         },
    #         # GA 후속 액션
    #         {
    #             "headerName": "GA Actions",
    #             "children": [
    #                 make_num_child("세션수",                       "session_count"),
    #                 make_num_child("평균세션시간(초)",    "avg_session_duration_sec"),
    #                 make_num_child("PLP조회",                      "view_item_list_sessions"),
    #                 make_num_child("PDP조회",                      "view_item_sessions"),
    #                 make_num_child("PDPscr50",                    "scroll_50_sessions"),
    #                 make_num_child("가격표시",                      "product_option_price_sessions"),
    #                 make_num_child("쇼룸찾기",                      "find_showroom_sessions"),
    #                 make_num_child("장바구니",                      "add_to_cart_sessions"),
    #                 make_num_child("회원가입",                      "sign_up_sessions"),
    #                 make_num_child("쇼룸10초",                      "showroom_10s_sessions"),
    #                 make_num_child("쇼룸예약",                      "showroom_leads_sessions"),
    #             ]
    #         },
    #     ]

    #     grouped_cols_CVR = [
    #         date_col,
    #         channel_col,
    #         make_num_child("일할비용", "Cost"),
    #         # 인게이지먼트
    #         {
    #             "headerName": "Engagement",
    #             "children": [
    #                 make_num_child("조회수",           "조회수"),
    #                 make_num_child("좋아요수",         "좋아요수"),
    #                 make_num_child("댓글수",           "댓글수"),
    #                 make_num_child("브랜드언급량",      "브랜드언급량"),
    #                 make_num_child("링크클릭수",        "링크 클릭수"),
    #             ]
    #         },
    #         # GA 후속 액션
    #         {
    #             "headerName": "GA Actions",
    #             "children": [
    #                 make_num_child("세션수",                       "session_count"),
    #                 make_num_child("평균세션시간(초)",    "avg_session_duration_sec"),
    #                 make_num_child("PLP조회",                      "view_item_list_sessions"),
    #                 make_num_child("PLP조회 CVR",                  "view_item_list_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("PDP조회",                      "view_item_sessions"),
    #                 make_num_child("PDP조회 CVR",                  "view_item_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("PDPscr50",                    "scroll_50_sessions"),
    #                 make_num_child("PDPscr50 CVR",                "scroll_50_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("가격표시",                      "product_option_price_sessions"),
    #                 make_num_child("가격표시 CVR",                  "product_option_price_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("쇼룸찾기",                      "find_showroom_sessions"),
    #                 make_num_child("쇼룸찾기 CVR",                  "find_nearby_showroom_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("장바구니",                      "add_to_cart_sessions"),
    #                 make_num_child("장바구니 CVR",                  "add_to_cart_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("회원가입",                      "sign_up_sessions"),
    #                 make_num_child("회원가입 CVR",                  "sign_up_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("쇼룸10초",                      "showroom_10s_sessions"),
    #                 make_num_child("쇼룸10초 CVR",                  "showroom_10s_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("쇼룸예약",                      "showroom_leads_sessions"),
    #                 make_num_child("쇼룸예약 CVR",                  "showroom_leads_CVR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #     ]

    #     grouped_cols_CPA = [
    #         date_col,
    #         channel_col,
    #         make_num_child("일할비용", "Cost"),
    #         # 인게이지먼트
    #         {
    #             "headerName": "Engagement",
    #             "children": [
    #                 make_num_child("조회수",           "조회수"),
    #                 make_num_child("좋아요수",         "좋아요수"),
    #                 make_num_child("댓글수",           "댓글수"),
    #                 make_num_child("브랜드언급량",      "브랜드언급량"),
    #                 make_num_child("링크클릭수",        "링크 클릭수"),
    #             ]
    #         },
    #         # GA 후속 액션
    #         {
    #             "headerName": "GA Actions",
    #             "children": [
    #                 make_num_child("세션수",                       "session_count"),
    #                 make_num_child("평균세션시간(초)",    "avg_session_duration_sec"),
    #                 make_num_child("PLP조회",                      "view_item_list_sessions"),
    #                 make_num_child("PLP조회 CPA",                  "view_item_list_CPA", fmt_digits=0),
    #                 make_num_child("PDP조회",                      "view_item_sessions"),
    #                 make_num_child("PDP조회 CPA",                  "view_item_CPA", fmt_digits=0),
    #                 make_num_child("PDPscr50",                    "scroll_50_sessions"),
    #                 make_num_child("PDPscr50 CPA",                "scroll_50_CPA", fmt_digits=0),
    #                 make_num_child("가격표시",                      "product_option_price_sessions"),
    #                 make_num_child("가격표시 CPA",                  "product_option_price_CPA", fmt_digits=0),
    #                 make_num_child("쇼룸찾기",                      "find_showroom_sessions"),
    #                 make_num_child("쇼룸찾기 CPA",                  "find_nearby_showroom_CPA", fmt_digits=0),
    #                 make_num_child("장바구니",                      "add_to_cart_sessions"),
    #                 make_num_child("장바구니 CPA",                  "add_to_cart_CPA", fmt_digits=0),
    #                 make_num_child("회원가입",                      "sign_up_sessions"),
    #                 make_num_child("회원가입 CPA",                  "sign_up_CPA", fmt_digits=0),
    #                 make_num_child("쇼룸10초",                      "showroom_10s_sessions"),
    #                 make_num_child("쇼룸10초 CPA",                  "showroom_10s_CPA", fmt_digits=0),
    #                 make_num_child("쇼룸예약",                      "showroom_leads_sessions"),
    #                 make_num_child("쇼룸예약 CPA",                  "showroom_leads_CPA", fmt_digits=0),
    #             ]
    #         },
    #     ]

    #     grouped_cols_CVRCPA = [
    #         date_col,
    #         channel_col,
    #         make_num_child("일할비용", "Cost"),
    #         # 인게이지먼트
    #         {
    #             "headerName": "Engagement",
    #             "children": [
    #                 make_num_child("조회수",           "조회수"),
    #                 make_num_child("좋아요수",         "좋아요수"),
    #                 make_num_child("댓글수",           "댓글수"),
    #                 make_num_child("브랜드언급량",      "브랜드언급량"),
    #                 make_num_child("링크클릭수",        "링크 클릭수"),
    #             ]
    #         },
    #         # GA 후속 액션
    #         {
    #             "headerName": "GA Actions",
    #             "children": [
    #                 make_num_child("세션수",                       "session_count"),
    #                 make_num_child("평균세션시간(초)",    "avg_session_duration_sec"),
    #                 make_num_child("PLP조회",                      "view_item_list_sessions"),
    #                 make_num_child("PLP조회 CVR",                  "view_item_list_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("PLP조회 CPA",                  "view_item_list_CPA", fmt_digits=0),
    #                 make_num_child("PDP조회",                      "view_item_sessions"),
    #                 make_num_child("PDP조회 CVR",                  "view_item_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("PDP조회 CPA",                  "view_item_CPA", fmt_digits=0),
    #                 make_num_child("PDPscr50",                    "scroll_50_sessions"),
    #                 make_num_child("PDPscr50 CVR",                "scroll_50_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("PDPscr50 CPA",                "scroll_50_CPA", fmt_digits=0),
    #                 make_num_child("가격표시",                      "product_option_price_sessions"),
    #                 make_num_child("가격표시 CVR",                  "product_option_price_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("가격표시 CPA",                  "product_option_price_CPA", fmt_digits=0),
    #                 make_num_child("쇼룸찾기",                      "find_showroom_sessions"),
    #                 make_num_child("쇼룸찾기 CVR",                  "find_nearby_showroom_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("쇼룸찾기 CPA",                  "find_nearby_showroom_CPA", fmt_digits=0),
    #                 make_num_child("장바구니",                      "add_to_cart_sessions"),
    #                 make_num_child("장바구니 CVR",                  "add_to_cart_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("장바구니 CPA",                  "add_to_cart_CPA", fmt_digits=0),
    #                 make_num_child("회원가입",                      "sign_up_sessions"),
    #                 make_num_child("회원가입 CVR",                  "sign_up_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("회원가입 CPA",                  "sign_up_CPA", fmt_digits=0),
    #                 make_num_child("쇼룸10초",                      "showroom_10s_sessions"),
    #                 make_num_child("쇼룸10초 CVR",                  "showroom_10s_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("쇼룸10초 CPA",                  "showroom_10s_CPA", fmt_digits=0),
    #                 make_num_child("쇼룸예약",                      "showroom_leads_sessions"),
    #                 make_num_child("쇼룸예약 CVR",                  "showroom_leads_CVR", fmt_digits=2, suffix="%"),
    #                 make_num_child("쇼룸예약 CPA",                  "showroom_leads_CPA", fmt_digits=0),
    #             ]
    #         },
    #     ]

    #     # (use_parent)
    #     if use_parent:
    #         if select_option == 1:
    #             column_defs = grouped_cols
    #         elif select_option == 2:
    #             column_defs = grouped_cols_CVR
    #         elif select_option == 3:
    #             column_defs = grouped_cols_CPA
    #         elif select_option == 4:
    #             column_defs = grouped_cols_CVRCPA
    #     else:
    #         column_defs = flat_cols
    
    #     # grid_options & 렌더링
    #     grid_options = {
    #     "columnDefs": column_defs,
    #     "defaultColDef": {
    #         "sortable": True,
    #         "filter": True,
    #         "resizable": True,
    #         "flex": 1,       # flex:1 이면 나머지 공간을 컬럼 개수만큼 균등 분배
    #         "minWidth": 90,   # 최소 너비
    #         "wrapHeaderText": True,
    #         "autoHeaderHeight": True
    #     },
    #     "onGridReady": JsCode(
    #         "function(params){ params.api.sizeColumnsToFit(); }"
    #     ),
    #     "headerHeight": 60,
    #     "groupHeaderHeight": 30,
    #     }        

    #     AgGrid(
    #         df2,
    #         gridOptions=grid_options,
    #         height=height,
    #         fit_columns_on_grid_load=False,  # True면 전체넓이에서 균등분배 
    #         theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
    #         allow_unsafe_jscode=True
    #     )


    #     # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #     def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]):
    #         summary: dict[str, float | str] = {}
    #         for col, op in agg_map.items():
    #             val = None
    #             try:
    #                 if op == 'sum':
    #                     val = df[col].sum()
    #                 elif op == 'avg':
    #                     val = df[col].mean()
    #                 elif op == 'mid':
    #                     val = df[col].median()
    #                 elif op == 'max':
    #                     val = df[col].max()
    #             except:
    #                 val = None

    #             # NaN / Inf / numpy 타입 → None or native 타입으로 처리
    #             if val is None or isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
    #                 summary[col] = None
    #             else:
    #                 # numpy 타입 제거
    #                 if isinstance(val, (np.integer, np.int64, np.int32)):
    #                     summary[col] = int(val)
    #                 elif isinstance(val, (np.floating, np.float64, np.float32)):
    #                     summary[col] = float(round(val, 2))
    #                 else:
    #                     summary[col] = val

    #         grid_options['pinnedBottomRowData'] = [summary]
    #         return grid_options
        
    #     # # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #     # grid_options = add_summary(
    #     #     grid_options,
    #     #     df2,
    #     #     {
    #     #         '조회수': 'max',
    #     #         '좋아요수': 'max',
    #     #         '댓글수': 'max',
    #     #         '브랜드언급량': 'max',
    #     #         '링크 클릭수': 'max',
    #     #         'session_count': 'sum',
    #     #         'avg_session_duration_sec': 'avg',
    #     #         'view_item_list_sessions': 'sum',
    #     #         'view_item_list_CVR': 'avg',
    #     #         'view_item_sessions': 'sum',
    #     #         'view_item_CVR': 'avg',
    #     #         'scroll_50_sessions': 'sum',
    #     #         'scroll_50_CVR': 'avg',
    #     #         'product_option_price_sessions': 'sum',
    #     #         'product_option_price_CVR': 'avg',
    #     #         'find_showroom_sessions': 'sum',
    #     #         'find_nearby_showroom_CVR': 'avg',
    #     #         'add_to_cart_sessions': 'sum',
    #     #         'add_to_cart_CVR': 'avg',
    #     #         'sign_up_sessions': 'sum',
    #     #         'sign_up_CVR': 'avg',
    #     #         'showroom_10s_sessions': 'sum',
    #     #         'showroom_10s_CVR': 'avg',
    #     #         'showroom_leads_sessions': 'sum',
    #     #         'showroom_leads_CVR': 'avg',
    #     #     }
    #     # )
        

        
    #     # AgGrid(
    #     #     df2,
    #     #     gridOptions=grid_options,
    #     #     height=height,
    #     #     fit_columns_on_grid_load=False,  # True면 전체넓이에서 균등분배 
    #     #     theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
    #     #     allow_unsafe_jscode=True
    #     # )

    # def render_aggrid__contri(
    #     df: pd.DataFrame,
    #     height: int = 323,
    #     use_parent: bool = True,
    #     brand: str = 'sleeper'
    #     ) -> None:
    #     """
    #     use_parent: False / True
    #     """
    #     df2 = df.copy()

    #     # (필수함수) make_num_child
    #     def make_num_child(header, field, fmt_digits=0, suffix=''):
    #         return {
    #             "headerName": header, "field": field,
    #             "type": ["numericColumn","customNumericFormat"],
    #             "valueFormatter": JsCode(
    #                 f"function(params){{"
    #                 f"  return params.value!=null?"
    #                 f"params.value.toLocaleString(undefined,{{minimumFractionDigits:{fmt_digits},maximumFractionDigits:{fmt_digits}}})+'{suffix}':'';"
    #                 f"}}"
    #             ),
    #             "cellStyle": JsCode("params=>({textAlign:'right'})")
    #         }
        
    #     # (필수함수) add_summary
    #     def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]): #'sum'|'avg'|'mid'
    #         summary: dict[str, float] = {}
    #         for col, op in agg_map.items():
    #             if op == 'sum':
    #                 summary[col] = int(df[col].sum())
    #             elif op == 'avg':
    #                 summary[col] = float(df[col].mean())
    #             elif op == 'mid':
    #                 summary[col] = float(df[col].median())
    #             else:
    #                 summary[col] = "-"  # 에러 발생시, "-"로 표기하고 raise error 하지 않음
    #         grid_options['pinnedBottomRowData'] = [summary]
    #         return grid_options
        
    #     # date_col
    #     date_col = {
    #         "headerName": "날짜",
    #         "field": "날짜",
    #         "pinned": "left",
    #         "width": 100,
    #         "cellStyle": JsCode("params=>({textAlign:'left'})"),
    #         # "sort": "desc"
    #     }
        
    #     flat_cols = [
    #     ]

    #     if brand == "sleeper":
    #         expected_cols = ['날짜', '검색량', '기본 검색량', '기본 검색량_비중',
    #                             '태요미네', '태요미네_비중', '노홍철 유튜브', '노홍철 유튜브_비중', '아울디자인', '아울디자인_비중', '알쓸물치', '알쓸물치_비중']            
            
    #         for col in expected_cols:
    #             df2[col] = df2.get(col, 0)
            
    #         df2.fillna(0, inplace=True)     # (기존과 동일) 값이 없는 경우 일단 0으로 치환


    #         # (use_parent) grouped_cols
    #         grouped_cols = [
    #             date_col,
    #             make_num_child("전체 검색량", "검색량"),
    #             # 검색량 차집합
    #             {
    #                 "headerName": "기본 검색량",
    #                 "children": [
    #                     make_num_child("검색량",      "기본 검색량"),
    #                     make_num_child("비중(%)",     "기본 검색량_비중", fmt_digits=2, suffix="%"),
    #                 ]
    #             },
    #             # 노홍철 유튜브
    #             {
    #                 "headerName": "노홍철 유튜브",
    #                 "children": [
    #                     make_num_child("검색량",      "노홍철 유튜브"),
    #                     make_num_child("비중(%)",     "노홍철 유튜브_비중", fmt_digits=2, suffix="%"),
    #                 ]
    #             },
    #             # 태요미네
    #             {
    #                 "headerName": "태요미네",
    #                 "children": [
    #                     make_num_child("검색량",      "태요미네"),
    #                     make_num_child("비중(%)",     "태요미네_비중", fmt_digits=2, suffix="%"),
    #                 ]
    #             },
    #             # 아울디자인
    #             {
    #                 "headerName": "아울디자인",
    #                 "children": [
    #                     make_num_child("검색량",      "아울디자인"),
    #                     make_num_child("비중(%)",     "아울디자인_비중", fmt_digits=2, suffix="%"),
    #                 ]
    #             },
    #             # 알쓸물치
    #             {
    #                 "headerName": "알쓸물치",
    #                 "children": [
    #                     make_num_child("검색량",      "알쓸물치"),
    #                     make_num_child("비중(%)",     "알쓸물치_비중", fmt_digits=2, suffix="%"),
    #                 ]
    #             },
    #         ]

    #     elif brand == "nooer": 
    #         expected_cols = ['날짜', '검색량', '기본 검색량', '기본 검색량_비중', '베리엠제이', '베리엠제이_비중']            
            
    #         for col in expected_cols:
    #             df2[col] = df2.get(col, 0)
            
    #         df2.fillna(0, inplace=True)     # (기존과 동일) 값이 없는 경우 일단 0으로 치환


    #         # (use_parent) grouped_cols
    #         grouped_cols = [
    #             date_col,
    #             make_num_child("전체 검색량", "검색량"),
    #             # 검색량 차집합
    #             {
    #                 "headerName": "기본 검색량",
    #                 "children": [
    #                     make_num_child("검색량",      "기본 검색량"),
    #                     make_num_child("비중(%)",     "기본 검색량_비중", fmt_digits=2, suffix="%"),
    #                 ]
    #             },
    #             # 베리엠제이
    #             {
    #                 "headerName": "베리엠제이",
    #                 "children": [
    #                     make_num_child("검색량",      "베리엠제이"),
    #                     make_num_child("비중(%)",     "베리엠제이_비중", fmt_digits=2, suffix="%"),
    #                 ]
    #             },
    #         ]
            
    #     # (use_parent)
    #     column_defs = grouped_cols if use_parent else flat_cols
    
    #     # grid_options & 렌더링
    #     grid_options = {
    #     "columnDefs": column_defs,
    #     "defaultColDef": {
    #         "sortable": True,
    #         "filter": True,
    #         "resizable": True,
    #         "flex": 1,       # flex:1 이면 나머지 공간을 컬럼 개수만큼 균등 분배
    #         "minWidth": 90,   # 최소 너비
    #         "wrapHeaderText": True,
    #         "autoHeaderHeight": True
    #     },
    #     "onGridReady": JsCode(
    #         "function(params){ params.api.sizeColumnsToFit(); }"
    #     ),
    #     "headerHeight": 30,
    #     "groupHeaderHeight": 30,
    #     }        

    #     # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #     def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]):
    #         summary: dict[str, float | str] = {}
    #         for col, op in agg_map.items():
    #             val = None
    #             try:
    #                 if op == 'sum':
    #                     val = df[col].sum()
    #                 elif op == 'avg':
    #                     val = df[col].mean()
    #                 elif op == 'mid':
    #                     val = df[col].median()
    #             except:
    #                 val = None

    #             # NaN / Inf / numpy 타입 → None or native 타입으로 처리
    #             if val is None or isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
    #                 summary[col] = None
    #             else:
    #                 # numpy 타입 제거
    #                 if isinstance(val, (np.integer, np.int64, np.int32)):
    #                     summary[col] = int(val)
    #                 elif isinstance(val, (np.floating, np.float64, np.float32)):
    #                     summary[col] = float(round(val, 2))
    #                 else:
    #                     summary[col] = val

    #         grid_options['pinnedBottomRowData'] = [summary]
    #         return grid_options

    #     if brand == "sleeper": 
    #         # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #         grid_options = add_summary(
    #             grid_options,
    #             df2,
    #             {
    #                 '검색량': 'sum',
    #                 '기본 검색량': 'sum',
    #                 '태요미네': 'sum',
    #                 '노홍철 유튜브': 'sum',
    #                 '아울디자인': 'sum',
    #                 '알쓸물치': 'sum',
    #             }
    #         )
    #     elif brand == "nooer":
    #         # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #         grid_options = add_summary(
    #             grid_options,
    #             df2,
    #             {
    #                 '검색량': 'sum',
    #                 '기본 검색량': 'sum',
    #                 '베리엠제이': 'sum',
    #             }
    #         )
        
    #     AgGrid(
    #         df2,
    #         gridOptions=grid_options,
    #         height=height,
    #         fit_columns_on_grid_load=False,  # True면 전체넓이에서 균등분배 
    #         theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
    #         allow_unsafe_jscode=True
    #     )

    # def render_aggrid__kwd(
    #     df: pd.DataFrame,
    #     height: int = 292,
    #     use_parent: bool = False
    #     ) -> None:
    #     """
    #     use_parent: False / True
    #     """
    #     df2 = df.copy()
        
    #     # (주의) 누락됱 컬럼히 당연히 있을수 있음, 그래서 fillna만 해주는게 아니라 컬럼 자리를 만들어서 fillna 해야함.
    #     expected_cols = ['날짜', '키워드', '검색량']
        
    #     for col in expected_cols:
    #         df2[col] = df2.get(col, 0)
    #     df2.fillna(0, inplace=True)     # (기존과 동일) 값이 없는 경우 일단 0으로 치환

    #     # (필수함수) make_num_child
    #     def make_num_child(header, field, fmt_digits=0, suffix=''):
    #         return {
    #             "headerName": header, "field": field,
    #             "type": ["numericColumn","customNumericFormat"],
    #             "valueFormatter": JsCode(
    #                 f"function(params){{"
    #                 f"  return params.value!=null?"
    #                 f"params.value.toLocaleString(undefined,{{minimumFractionDigits:{fmt_digits},maximumFractionDigits:{fmt_digits}}})+'{suffix}':'';"
    #                 f"}}"
    #             ),
    #             "cellStyle": JsCode("params=>({textAlign:'right'})")
    #         }
        
    #     # (필수함수) add_summary
    #     def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]): #'sum'|'avg'|'mid'
    #         summary: dict[str, float] = {}
    #         for col, op in agg_map.items():
    #             if op == 'sum':
    #                 summary[col] = int(df[col].sum())
    #             elif op == 'avg':
    #                 summary[col] = float(df[col].mean())
    #             elif op == 'mid':
    #                 summary[col] = float(df[col].median())
    #             else:
    #                 summary[col] = "-"  # 에러 발생시, "-"로 표기하고 raise error 하지 않음
    #         grid_options['pinnedBottomRowData'] = [summary]
    #         return grid_options
        
    #     # date_col
    #     date_col = {
    #         "headerName": "날짜",
    #         "field": "날짜",
    #         "pinned": "left",
    #         "width": 100,
    #         "cellStyle": JsCode("params=>({textAlign:'left'})"),
    #         # "sort": "desc"
    #     }
    #     kwd_col = {
    #         "headerName": "키워드",
    #         "field": "키워드",
    #         "pinned": "left",
    #         "width": 100,
    #         "cellStyle": JsCode("params=>({textAlign:'left'})"),
    #         # "sort": "desc"
    #     }
        
    #     flat_cols = [
    #         date_col,
    #         kwd_col,
    #         make_num_child("검색량",     "검색량"),
    #     ]

    #     # (use_parent) grouped_cols
    #     grouped_cols = [
    #     ]

    #     # (use_parent)
    #     column_defs = grouped_cols if use_parent else flat_cols
    
    #     # grid_options & 렌더링
    #     grid_options = {
    #     "columnDefs": column_defs,
    #     "defaultColDef": {
    #         "sortable": True,
    #         "filter": True,
    #         "resizable": True,
    #         "flex": 1,       # flex:1 이면 나머지 공간을 컬럼 개수만큼 균등 분배
    #         "minWidth": 90,   # 최소 너비
    #         "wrapHeaderText": True,
    #         "autoHeaderHeight": True
    #     },
    #     "onGridReady": JsCode(
    #         "function(params){ params.api.sizeColumnsToFit(); }"
    #     ),
    #     "headerHeight": 30,
    #     "groupHeaderHeight": 30,
    #     }        

    #     # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #     def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]):
    #         summary: dict[str, float | str] = {}
    #         for col, op in agg_map.items():
    #             val = None
    #             try:
    #                 if op == 'sum':
    #                     val = df[col].sum()
    #                 elif op == 'avg':
    #                     val = df[col].mean()
    #                 elif op == 'mid':
    #                     val = df[col].median()
    #             except:
    #                 val = None

    #             # NaN / Inf / numpy 타입 → None or native 타입으로 처리
    #             if val is None or isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
    #                 summary[col] = None
    #             else:
    #                 # numpy 타입 제거
    #                 if isinstance(val, (np.integer, np.int64, np.int32)):
    #                     summary[col] = int(val)
    #                 elif isinstance(val, (np.floating, np.float64, np.float32)):
    #                     summary[col] = float(round(val, 2))
    #                 else:
    #                     summary[col] = val

    #         grid_options['pinnedBottomRowData'] = [summary]
    #         return grid_options

    #     # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #     grid_options = add_summary(
    #         grid_options,
    #         df2,
    #         {
    #             '검색량': 'sum',
    #         }
    #     )
        
    #     AgGrid(
    #         df2,
    #         gridOptions=grid_options,
    #         height=height,
    #         fit_columns_on_grid_load=False,  # True면 전체넓이에서 균등분배 
    #         theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
    #         allow_unsafe_jscode=True
    #     )



    # ────────────────────────────────────────────────────────────────
    # 채널 목록
    # ────────────────────────────────────────────────────────────────
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)

    # 1번 영역
    st.markdown("<h5 style='margin:0'>채널 목록</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ전체 채널에 대한 집행 정보입니다. <span style='color:#8E9097;'>(최신순 정렬)</span> ", unsafe_allow_html=True)

    df = PPL_LIST
    df = df.sort_values(by="order", ascending=False)
    
    # 브랜드별 데이터프레임 분리
    df_slp = df[df["브랜드"] == "슬립퍼"].copy()
    df_nor = df[df["브랜드"] == "누어"].copy()
    
    tab1, tab2, tab3 = st.tabs(["전체", "슬립퍼", "누어"])
    with tab1:
        cols_per_row = 5
        rows = math.ceil(len(df) / cols_per_row)
        for i in range(rows):
            # gap="small" 으로 컬럼 간격 최소화
            cols = st.columns(cols_per_row, gap="small")
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
                            <div style="color:#333;">Total <strong>{int(row['금액']):,}원</strong></div>
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
    with tab2:
        cols_per_row = 5
        rows = math.ceil(len(df_slp) / cols_per_row)
        for i in range(rows):
            # gap="small" 으로 컬럼 간격 최소화
            cols = st.columns(cols_per_row, gap="small")
            for j, col in enumerate(cols):
                idx = i * cols_per_row + j
                if idx >= len(df_slp):
                    break
                row = df_slp.iloc[idx]
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
                            <div style="color:#333;">Total <strong>{int(row['금액']):,}원</strong></div>
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
    with tab3:
        cols_per_row = 5
        rows = math.ceil(len(df_nor) / cols_per_row)
        for i in range(rows):
            # gap="small" 으로 컬럼 간격 최소화
            cols = st.columns(cols_per_row, gap="small")
            for j, col in enumerate(cols):
                idx = i * cols_per_row + j
                if idx >= len(df_nor):
                    break
                row = df_nor.iloc[idx]
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
                            <div style="color:#333;">Total <strong>{int(row['금액']):,}원</strong></div>
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
    # 채널별 인게이지먼트 및 액션
    # ────────────────────────────────────────────────────────────────
    st.subheader(" ")
    st.subheader(" ")
    st.markdown("<h5 style='margin:0'>채널별 인게이지먼트 및 액션</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ채널별 **반응 데이터**와 **사용자 액션 및 효율 데이터**를 확인할 수 있습니다.", unsafe_allow_html=True)
    with st.popover("지표 설명"):
        st.markdown("""
                    - **CVR** (Conversion Rate) : **전환율** (주문수 ÷ 세션수 × 100)  
                    - **CPA** (Cost Per Action) : **행동당 비용** (광고비 ÷ 전환수)  
                    """)

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
    
    # 자료형 
    numeric_cols = df_merged_t.columns.difference(["날짜", "채널명"])
    df_merged_t[numeric_cols] = df_merged_t[numeric_cols].apply(lambda col: pd.to_numeric(col, errors="coerce").fillna(0))
    df_merged_t[numeric_cols] = df_merged_t[numeric_cols].astype(int)

    # 채널별 데이터프레임 분리
    df_usefulpt  = df_merged_t[df_merged_t["채널명"] == "알쓸물치"].copy()
    df_owldesign  = df_merged_t[df_merged_t["채널명"] == "아울디자인"].copy()
    df_verymj  = df_merged_t[df_merged_t["채널명"] == "베리엠제이"].copy()
    df_taeyomine = df_merged_t[df_merged_t["채널명"] == "태요미네"].copy()
    df_hongchul  = df_merged_t[df_merged_t["채널명"] == "노홍철 유튜브"].copy()


    tab1, tab2, tab3, tab4, tab5 = st.tabs(["노홍철 유튜브", "태요미네", "베리엠제이", "아울디자인", "알쓸물치"])
    
    # check box -> CVR, CPA
    with tab1:
        c1, c2, _ = st.columns([1,1,11])
        add_cvr = c1.checkbox("CVR 추가", key="hongchul_cvr", value=False)
        add_cpa = c2.checkbox("CPA 추가", key="hongchul_cpa", value=False)
        if add_cvr and add_cpa:
            opt = 4
        elif add_cvr:
            opt = 2
        elif add_cpa:
            opt = 3
        else:
            opt = 1
        render_style_eng(df_hongchul, select_option=opt)
        
    
    with tab2:    
        c1, c2, _ = st.columns([1,1,11])
        add_cvr = c1.checkbox("CVR 추가", key="taeyomine_cvr", value=False)
        add_cpa = c2.checkbox("CPA 추가", key="taeyomine_cpa", value=False)
        if add_cvr and add_cpa:
            opt = 4
        elif add_cvr:
            opt = 2
        elif add_cpa:
            opt = 3
        else:
            opt = 1
        render_style_eng(df_taeyomine, select_option=opt)
        
    with tab3: 
        c1, c2, _ = st.columns([1,1,11])
        add_cvr = c1.checkbox("CVR 추가", key="verymj_cvr", value=False)
        add_cpa = c2.checkbox("CPA 추가", key="verymj_cpa", value=False)
        if add_cvr and add_cpa:
            opt = 4
        elif add_cvr:
            opt = 2
        elif add_cpa:
            opt = 3
        else:
            opt = 1
        render_style_eng(df_verymj, select_option=opt)
        
    with tab4: 
        c1, c2, _ = st.columns([1,1,11])
        add_cvr = c1.checkbox("CVR 추가", key="owldesign_cvr", value=False)
        add_cpa = c2.checkbox("CPA 추가", key="owldesign_cpa", value=False)
        if add_cvr and add_cpa:
            opt = 4
        elif add_cvr:
            opt = 2
        elif add_cpa:
            opt = 3
        else:
            opt = 1
        render_style_eng(df_owldesign, select_option=opt)
        
    with tab5: 
        c1, c2, _ = st.columns([1,1,11])
        add_cvr = c1.checkbox("CVR 추가", key="usefulpt_cvr", value=False)
        add_cpa = c2.checkbox("CPA 추가", key="usefulpt_cpa", value=False)
        if add_cvr and add_cpa:
            opt = 4
        elif add_cvr:
            opt = 2
        elif add_cpa:
            opt = 3
        else:
            opt = 1
        render_style_eng(df_usefulpt, select_option=opt)


    # ────────────────────────────────────────────────────────────────
    # 채널별 쿼리 기여량
    # ────────────────────────────────────────────────────────────────
    query_sum_slp  = query_sum[query_sum["브랜드"] == "슬립퍼"].copy()
    query_sum_nor  = query_sum[query_sum["브랜드"] == "누어"].copy()
    
    st.header(" ")
    st.markdown("<h5 style='margin:0'>채널별 쿼리 기여량</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ'쿼리 기여량'은 전체 검색량 중에서 **각 PPL 채널이 유도했다고 판단되는 검색 수**를 의미합니다.", unsafe_allow_html=True)
    
    ppl_action2 = PPL_ACTION[['날짜', 'utm_content', 'SearchVolume_contribution']]   # 기여 쿼리량 { 날짜, utm_content, SearchVolume_contribution }
    ppl_action3 = pd.merge(ppl_action2, PPL_LIST, on=['utm_content'], how='left')   # utm_content가 너무 복잡하니까 채널명으로 변경
    ppl_action3 = ppl_action3[['날짜', '채널명', 'SearchVolume_contribution']]        # utm_content 안녕~
    ppl_action3 = ppl_action3.pivot_table(index="날짜", columns="채널명", values="SearchVolume_contribution", aggfunc="sum").reset_index() # 멜팅
    

    tab1, tab2 = st.tabs(["슬립퍼", "누어"])
    # ---- 슬립퍼
    with tab1:
        df_QueryContribution     = ppl_action3.merge(query_sum_slp[['날짜', '검색량']], on='날짜', how='outer')  # 데이터 생성 
        
        # 데이터 전처리 1
        cols_to_int = ['태요미네', '노홍철 유튜브', '아울디자인', '알쓸물치', '검색량']
        df_QueryContribution[cols_to_int] = df_QueryContribution[cols_to_int].apply(
            lambda s: pd.to_numeric(s, errors='coerce')   # 숫자로 변환, 에러나면 NaN
                        .fillna(0)                        # NaN → 0
                        .astype(int)                      # int 로 캐스팅
        )
        # 신규컬럼 생성 - 기본 검색량
        df_QueryContribution["기본 검색량"] = df_QueryContribution["검색량"] - df_QueryContribution[['태요미네','노홍철 유튜브', '아울디자인', '알쓸물치']].sum(axis=1)
        # 신규컬럼 생성 - 비중
        cols = ['노홍철 유튜브', '태요미네', '아울디자인', '알쓸물치', '기본 검색량']
        for col in cols:
            df_QueryContribution[f"{col}_비중"] = (
                df_QueryContribution[col] / df_QueryContribution['검색량'] * 100
            ).round(2)
        df_QueryContribution[[f"{c}_비중" for c in cols]] = df_QueryContribution[[f"{c}_비중" for c in cols]].fillna(0) # 다시 검색량이 0이었던 곳은 0% 처리
        df_QueryContribution = df_QueryContribution[['날짜', '검색량', '기본 검색량', '기본 검색량_비중',
                                                        '태요미네', '태요미네_비중', '노홍철 유튜브', '노홍철 유튜브_비중','아울디자인', '아울디자인_비중', '알쓸물치', '알쓸물치_비중']]
        df_QueryContribution = df_QueryContribution.sort_values("날짜", ascending=True)
        
        from pandas.tseries.offsets import MonthEnd
        # 1) “날짜” → datetime 변환
        df_QueryContribution["날짜_dt"] = pd.to_datetime(
            df_QueryContribution["날짜"], format="%Y-%m-%d", errors="coerce"
        )

        # 슬라이더 -> 데이터 전체 범위
        start_period = df_QueryContribution["날짜_dt"].min().to_period("M")  # 데이터 최소월
        # end_period = df_QueryContribution["날짜_dt"].max().to_period("M")  # 데이터 최소월
        curr_period  = pd.Timestamp.now().to_period("M")                     # 이번달
        all_periods  = pd.period_range(start=start_period, end=curr_period, freq="M")
        month_options = [p.to_timestamp() for p in all_periods]

        # 데이터 선택 범위 디폴트 -> 지난달 ~ 이번달
        now     = pd.Timestamp.now()
        curr_ts = now.to_period("M").to_timestamp()         # 이번달 첫날
        prev_ts = (now.to_period("M") - 1).to_timestamp()   # 이전월 첫날

        # 슬라이더 렌더링
        st.markdown(" ")
        selected_range = st.select_slider(
            "🚀 기간 선택ㅤ(지난달부터 이번달까지가 기본 선택되어 있습니다)",
            options=month_options,                  # 전체 데이터 기간 옵션
            value=(prev_ts, curr_ts),               # 기본: 이전월→이번달
            format_func=lambda x: x.strftime("%Y-%m"),
            key="slider_01"
        )
        start_sel, end_sel = selected_range

        # 5) 필터링 구간(1일~말일)
        period_start = start_sel
        period_end   = end_sel + MonthEnd(0)

        df_filtered = df_QueryContribution[
            (df_QueryContribution["날짜_dt"] >= period_start) &
            (df_QueryContribution["날짜_dt"] <= period_end)
        ].copy()
        df_filtered["날짜"] = df_filtered["날짜_dt"].dt.strftime("%Y-%m-%d")

        # 6) long 포맷 변환 및 렌더링
        cols    = ['노홍철 유튜브', '태요미네', '아울디자인', '알쓸물치', '기본 검색량']
        df_long = df_filtered.melt(
            id_vars='날짜',
            value_vars=cols,
            var_name='콘텐츠',
            value_name='기여량'
        )
        # 렌더링
        render_stacked_bar(df_long, x="날짜", y="기여량", color="콘텐츠")
        render_style_ctb(df_filtered, brand='sleeper')
    
    # ---- 누어
    with tab2:
        df_QueryContribution_nor = ppl_action3.merge(query_sum_nor[['날짜', '검색량']], on='날짜', how='outer')
        # 데이터 전처리 1
        cols_to_int = ['베리엠제이', '검색량']
        df_QueryContribution_nor[cols_to_int] = df_QueryContribution_nor[cols_to_int].apply(
            lambda s: pd.to_numeric(s, errors='coerce')   # 숫자로 변환, 에러나면 NaN
                        .fillna(0)                        # NaN → 0
                        .astype(int)                      # int 로 캐스팅
        )
        # 신규컬럼 생성 - 기본 검색량
        df_QueryContribution_nor["기본 검색량"] = df_QueryContribution_nor["검색량"] - df_QueryContribution_nor[['베리엠제이']].sum(axis=1)
        # 신규컬럼 생성 - 비중
        cols = ['베리엠제이', '기본 검색량']
        for col in cols:
            df_QueryContribution_nor[f"{col}_비중"] = (
                df_QueryContribution_nor[col] / df_QueryContribution_nor['검색량'] * 100
            ).round(2)
        df_QueryContribution_nor[[f"{c}_비중" for c in cols]] = df_QueryContribution_nor[[f"{c}_비중" for c in cols]].fillna(0) # 다시 검색량이 0이었던 곳은 0% 처리
        df_QueryContribution_nor = df_QueryContribution_nor[['날짜', '검색량', '기본 검색량', '기본 검색량_비중', '베리엠제이', '베리엠제이_비중']]
        df_QueryContribution_nor = df_QueryContribution_nor.sort_values("날짜", ascending=True)
        
        from pandas.tseries.offsets import MonthEnd
        # 1) “날짜” → datetime 변환
        df_QueryContribution_nor["날짜_dt"] = pd.to_datetime(
            df_QueryContribution_nor["날짜"], format="%Y-%m-%d", errors="coerce"
        )

        # 슬라이더 -> 데이터 전체 범위
        start_period = df_QueryContribution_nor["날짜_dt"].min().to_period("M")  # 데이터 최소월
        # end_period = df_QueryContribution["날짜_dt"].max().to_period("M")  # 데이터 최소월
        curr_period  = pd.Timestamp.now().to_period("M")                     # 이번달
        all_periods  = pd.period_range(start=start_period, end=curr_period, freq="M")
        month_options = [p.to_timestamp() for p in all_periods]

        # 데이터 선택 범위 디폴트 -> 지난달 ~ 이번달
        now     = pd.Timestamp.now()
        curr_ts = now.to_period("M").to_timestamp()         # 이번달 첫날
        prev_ts = (now.to_period("M") - 1).to_timestamp()   # 이전월 첫날

        # 슬라이더 렌더링
        st.markdown(" ")
        selected_range = st.select_slider(
            "🚀 기간 선택ㅤ(지난달부터 이번달까지가 기본 선택되어 있습니다)",
            options=month_options,                  # 전체 데이터 기간 옵션
            value=(prev_ts, curr_ts),               # 기본: 이전월→이번달
            format_func=lambda x: x.strftime("%Y-%m"),
            key="slider_02"
        )
        start_sel, end_sel = selected_range

        # 5) 필터링 구간(1일~말일)
        period_start = start_sel
        period_end   = end_sel + MonthEnd(0)

        df_filtered_nor = df_QueryContribution_nor[
            (df_QueryContribution_nor["날짜_dt"] >= period_start) &
            (df_QueryContribution_nor["날짜_dt"] <= period_end)
        ].copy()
        df_filtered_nor["날짜"] = df_filtered_nor["날짜_dt"].dt.strftime("%Y-%m-%d")

        # 6) long 포맷 변환 및 렌더링
        cols    = ['베리엠제이', '기본 검색량']
        df_long = df_filtered_nor.melt(
            id_vars='날짜',
            value_vars=cols,
            var_name='콘텐츠',
            value_name='기여량'
        )
        # 렌더링
        render_stacked_bar(df_long, x="날짜", y="기여량", color="콘텐츠")
        render_style_ctb(df_filtered_nor, brand='nooer')
    
    
    # ────────────────────────────────────────────────────────────────
    # 키워드별 검색량
    # ────────────────────────────────────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>키워드별 검색량</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ주요 **키워드별 검색량**에 대해 증감 추이를 확인할 수 있습니다.", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["슬립퍼", "누어"])
    with tab1: 
        df = query_slp.copy()

        # ──────────── 월 단위 범위 슬라이더 추가 ────────────
        # 1) 날짜 컬럼을 datetime 으로 변환
        df['날짜_dt'] = pd.to_datetime(df['날짜'], format="%Y-%m-%d", errors="coerce")
        df['날짜'] = df['날짜'].dt.strftime("%Y-%m-%d")


        # 2) 전체 데이터 범위의 월 옵션 생성
        start_period  = df['날짜_dt'].min().to_period("M")
        curr_period   = pd.Timestamp.now().to_period("M")
        all_periods   = pd.period_range(start=start_period, end=curr_period, freq="M")
        month_options = [p.to_timestamp() for p in all_periods]

        # 3) 기본값: 이전월 → 이번달
        now     = pd.Timestamp.now()
        curr_ts = now.to_period("M").to_timestamp()
        prev_ts = (now.to_period("M") - 1).to_timestamp()

        # 4) 범위 슬라이더
        st.markdown(" ")
        start_sel, end_sel = st.select_slider(
            "🚀 기간 선택ㅤ(지난달부터 이번달까지가 기본 선택되어 있습니다)",
            options=month_options,
            value=(prev_ts, curr_ts),
            format_func=lambda x: x.strftime("%Y-%m"),
            key="slider_03"
        )

        # 5) 선택 구간의 1일~말일 계산 & 필터링
        period_start = start_sel
        period_end   = end_sel + MonthEnd(0)
        df = df[(df['날짜_dt'] >= period_start) & (df['날짜_dt'] <= period_end)].copy()

        # ───────────────────── 기존 필터 영역 ─────────────────────
        ft1, _p, ft2 = st.columns([3, 0.3, 1])
        with ft1: 
            keywords     = df['키워드'].unique().tolist()
            default_kw   = [kw for kw in keywords if ('슬리퍼' in kw) or ('슬립퍼' in kw)]
            sel_keywords = st.multiselect(
                "키워드 선택", 
                keywords, 
                default=default_kw,
                key="kw_select_03"
            )       
        with _p: pass
        with ft2: 
            chart_type = st.radio(
                "시각화 유형 선택", 
                ["누적 막대", "누적 영역", "꺾은선"], 
                horizontal=True, 
                index=0,
                key="chart_type_03"
            )

        df_f = df[df['키워드'].isin(sel_keywords)].copy()

        # y축 고정
        y_col = "검색량"

        # 1) 숫자형 변환 & 일별 집계
        df_plot = df_f.copy()
        df_plot[y_col] = pd.to_numeric(df_plot[y_col], errors="coerce").fillna(0)
        plot_df = (
            df_plot
            .groupby(["날짜_dt", "키워드"], as_index=False)[y_col]
            .sum()
        )
        if plot_df.empty:
            st.warning("선택된 기간/키워드에 해당하는 데이터가 없습니다.")
        else:
            # 2) 일별 날짜 범위 생성
            min_date = plot_df["날짜_dt"].min()
            max_date = plot_df["날짜_dt"].max()
            all_x    = pd.date_range(min_date, max_date)
            x_col    = "날짜_dt"

            # 3) MultiIndex 재색인으로 누락값 채움
            all_keywords = plot_df['키워드'].unique()
            idx = pd.MultiIndex.from_product([all_x, all_keywords],
                                            names=[x_col, "키워드"])
            plot_df = (
                plot_df
                .set_index([x_col, '키워드'])[y_col]
                .reindex(idx, fill_value=0)
                .reset_index()
            )

            # 4) chart_type 에 따른 시각화
            if chart_type == "누적 막대":
                fig = px.bar(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="키워드",
                    barmode="relative",
                )
                fig.update_layout(barmode="relative")
                fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
                fig.update_traces(opacity=0.6)

            elif chart_type == "누적 영역":
                fig = px.area(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="키워드",
                )
                fig.update_traces(opacity=0.3)

            else:  # 꺾은선
                fig = px.line(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="키워드",
                    markers=True,
                )
                fig.update_traces(opacity=0.6)

            # x축 한글 포맷, 축 제목 숨기기
            try:
                fig.update_xaxes(tickformat="%m월 %d일")
                fig.update_layout(xaxis_title=None, yaxis_title=None)
                st.plotly_chart(fig, use_container_width=True)
            except: pass
            df_f = df_f[['날짜', '키워드', '검색량']]
            st.dataframe(df_f,  row_height=30, hide_index=True)
            
    with tab2: 
        df = query_nor.copy()

        # ──────────── 월 단위 범위 슬라이더 추가 ────────────
        # 1) 날짜 컬럼을 datetime 으로 변환
        df['날짜_dt'] = pd.to_datetime(df['날짜'], format="%Y-%m-%d", errors="coerce")
        df['날짜'] = df['날짜'].dt.strftime("%Y-%m-%d")


        # 2) 전체 데이터 범위의 월 옵션 생성
        start_period  = df['날짜_dt'].min().to_period("M")
        curr_period   = pd.Timestamp.now().to_period("M")
        all_periods   = pd.period_range(start=start_period, end=curr_period, freq="M")
        month_options = [p.to_timestamp() for p in all_periods]

        # 3) 기본값: 이전월 → 이번달
        now     = pd.Timestamp.now()
        curr_ts = now.to_period("M").to_timestamp()
        prev_ts = (now.to_period("M") - 1).to_timestamp()

        # 4) 범위 슬라이더
        st.markdown(" ")
        start_sel, end_sel = st.select_slider(
            "🚀 기간 선택ㅤ(지난달부터 이번달까지가 기본 선택되어 있습니다)",
            options=month_options,
            value=(prev_ts, curr_ts),
            format_func=lambda x: x.strftime("%Y-%m"),
            key="slider_04"
        )

        # 5) 선택 구간의 1일~말일 계산 & 필터링
        period_start = start_sel
        period_end   = end_sel + MonthEnd(0)
        df = df[(df['날짜_dt'] >= period_start) & (df['날짜_dt'] <= period_end)].copy()

        # ───────────────────── 기존 필터 영역 ─────────────────────
        ft1, _p, ft2 = st.columns([3, 0.3, 1])
        with ft1: 
            keywords     = df['키워드'].unique().tolist()
            default_kw   = [kw for kw in keywords if ('누어' in kw) or ('NOOER' in kw)]
            sel_keywords = st.multiselect(
                "키워드 선택", 
                keywords, 
                default=default_kw,
                key="kw_select_04"
            )       
        with _p: pass
        with ft2: 
            chart_type = st.radio(
                "시각화 유형 선택", 
                ["누적 막대", "누적 영역", "꺾은선"], 
                horizontal=True, 
                index=0,
                key="chart_type_04"
            )

        df_f = df[df['키워드'].isin(sel_keywords)].copy()

        # y축 고정
        y_col = "검색량"

        # 1) 숫자형 변환 & 일별 집계
        df_plot = df_f.copy()
        df_plot[y_col] = pd.to_numeric(df_plot[y_col], errors="coerce").fillna(0)
        plot_df = (
            df_plot
            .groupby(["날짜_dt", "키워드"], as_index=False)[y_col]
            .sum()
        )
        if plot_df.empty:
            st.warning("선택된 기간/키워드에 해당하는 데이터가 없습니다.")
        else:
            # 2) 일별 날짜 범위 생성
            min_date = plot_df["날짜_dt"].min()
            max_date = plot_df["날짜_dt"].max()
            all_x    = pd.date_range(min_date, max_date)
            x_col    = "날짜_dt"

            # 3) MultiIndex 재색인으로 누락값 채움
            all_keywords = plot_df['키워드'].unique()
            idx = pd.MultiIndex.from_product([all_x, all_keywords],
                                            names=[x_col, "키워드"])
            plot_df = (
                plot_df
                .set_index([x_col, '키워드'])[y_col]
                .reindex(idx, fill_value=0)
                .reset_index()
            )

            # 4) chart_type 에 따른 시각화
            if chart_type == "누적 막대":
                fig = px.bar(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="키워드",
                    barmode="relative",
                )
                fig.update_layout(barmode="relative")
                fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
                fig.update_traces(opacity=0.6)

            elif chart_type == "누적 영역":
                fig = px.area(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="키워드",
                )
                fig.update_traces(opacity=0.3)

            else:  # 꺾은선
                fig = px.line(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="키워드",
                    markers=True,
                )
                fig.update_traces(opacity=0.6)

            # x축 한글 포맷, 축 제목 숨기기
            fig.update_xaxes(tickformat="%m월 %d일")
            fig.update_layout(xaxis_title=None, yaxis_title=None)
            st.plotly_chart(fig, use_container_width=True)
            df_f = df_f[['날짜', '키워드', '검색량']]
            st.dataframe(df_f, row_height=30,  hide_index=True)


    # ────────────────────────────────────────────────────────────────
    # 5번 영역
    # ────────────────────────────────────────────────────────────────
    # 5번 영역
    st.header(" ")
    st.markdown("<h5 style='margin:0'>터치포인트 (기획중)</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)




if __name__ == '__main__':
    main()
