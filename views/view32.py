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

import modules.ui_common as ui
importlib.reload(ui)
from zoneinfo import ZoneInfo



def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정
    # ──────────────────────────────────
    st.markdown(
        """
        <style>
            .block-container {
                max-width: 100% !important;
                padding-top: 0rem;
                padding-bottom: 8rem;
                padding-left: 5rem;
                padding-right: 4.5rem;
            }
        </style>
        """,
        unsafe_allow_html=True
    )    
    # # 탭 간격 CSS
    # st.markdown("""
    #     <style>
    #         [role="tablist"] [role="tab"] { margin-right: 1rem; }
    #     </style>
    # """, unsafe_allow_html=True)
    
    
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
        GA_EVENT_SUMMARY = pd.DataFrame(sh.worksheet('GA_Event_Summary').get_all_records())
        # --------------------------------------------------------------
        wsa = sh.worksheet('PPL_ACTION')
        data = wsa.get('A1:P')  # A열~P열까지 전체
        PPL_ACTION = pd.DataFrame(data[1:], columns=data[0])  # 1행=헤더
        # --------------------------------------------------------------
        query_slp      = pd.DataFrame(sh.worksheet('query_슬립퍼').get_all_records())
        query_nor      = pd.DataFrame(sh.worksheet('query_누어').get_all_records())
        query_sum      = pd.DataFrame(sh.worksheet('query_sum').get_all_records())
        
        # last_updated_time
        last_updated_time__query = query_slp['날짜'].max()
        last_updated_time__GA = GA_EVENT_SUMMARY['event_date'].max()
        
        
        return PPL_LIST, PPL_DATA, PPL_ACTION, query_slp, query_nor, query_sum, last_updated_time__query, last_updated_time__GA


    # PROGRESS BAR
    import time
    progress_bar = st.progress(0, text="데이터베이스 연결 확인 중입니다...")
    time.sleep(0.2)
    
    for i in range(1, 80, 5):
        progress_bar.progress(i, text=f"데이터를 불러오고 있습니다...{i}%")
        time.sleep(0.1)

    PPL_LIST, PPL_DATA, PPL_ACTION, query_slp, query_nor, query_sum, last_updated_time__query, last_updated_time__GA = load_data()
    
    # 로드 완료 직후, 수치를 대폭 점프시켜 보상감 제공
    progress_bar.progress(95, text="데이터 분석 및 시각화를 구성 증입니다...")
    time.sleep(0.4)
    
    progress_bar.progress(100, text="데이터 로드 완료!")
    time.sleep(0.6)
    progress_bar.empty()


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
        df['view_item_list_CVR']         = (df['view_item_list_sessions']         / df['session_count']          * 100).round(2)
        df['view_item_CVR']              = (df['view_item_sessions']              / df['session_count']          * 100).round(2)
        df['scroll_50_CVR']              = (df['scroll_50_sessions']              / df['session_count']          * 100).round(2)
        df['product_option_price_CVR']   = (df['product_option_price_sessions']   / df['session_count']          * 100).round(2)
        df['find_nearby_showroom_CVR']   = (df['find_showroom_sessions']          / df['session_count']          * 100).round(2)
        df['add_to_cart_CVR']            = (df['add_to_cart_sessions']            / df['session_count']          * 100).round(2)
        df['sign_up_CVR']                = (df['sign_up_sessions']                / df['session_count']          * 100).round(2)
        df['showroom_10s_CVR']           = (df['showroom_10s_sessions']           / df['session_count']          * 100).round(2)
        df['showroom_leads_CVR']         = (df['showroom_leads_sessions']         / df['session_count']          * 100).round(2)  # (25.11.02) view_item_list_CVR 제외하고 view_item_list_sessions가 분모였는데 세션수로 일괄 바꿈
        # 파생지표 생성 - CPA
        df['view_item_list_CPA']         = (df['Cost']/10     /  df['view_item_list_sessions']          * 100).round(0)  # (25.11.02) 행별로 분배가 안되고 있어서 하드코딩으로 변경함 
        df['view_item_CPA']              = (df['Cost']/10     /  df['view_item_sessions']               * 100).round(0)
        df['scroll_50_CPA']              = (df['Cost']/10     /  df['scroll_50_sessions']               * 100).round(0)
        df['product_option_price_CPA']   = (df['Cost']/10     /  df['product_option_price_sessions']    * 100).round(0)
        df['find_nearby_showroom_CPA']   = (df['Cost']/10     /  df['find_showroom_sessions']           * 100).round(0)
        df['add_to_cart_CPA']            = (df['Cost']/10     /  df['add_to_cart_sessions']             * 100).round(0)
        df['sign_up_CPA']                = (df['Cost']/10     /  df['sign_up_sessions']                 * 100).round(0)
        df['showroom_10s_CPA']           = (df['Cost']/10     /  df['showroom_10s_sessions']            * 100).round(0)
        df['showroom_leads_CPA']         = (df['Cost']/10     /  df['showroom_leads_sessions']          * 100).round(0)
        
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
                ("GA",    "평균체류시간(초)"),   # avg_session_duration_sec
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
                ("GA",    "평균체류시간(초)"),   # avg_session_duration_sec
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
                ("GA",    "평균체류시간(초)"),   # avg_session_duration_sec
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
                ("GA",    "평균체류시간(초)"),   # avg_session_duration_sec
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
        styled = ui.style_format(
            decorate_df_eng(target_df, select_option=opt),
            decimals_map={
                ("COST",         "일할비용"):0,          # 
                ("ENGAGEMENT",   "조회수"):0,            # 
                ("ENGAGEMENT",   "좋아요수"):0,          # 
                ("ENGAGEMENT",   "댓글수"):0,            # 
                ("ENGAGEMENT",   "브랜드언급량"):0,       # 
                ("ENGAGEMENT",   "링크클릭수"):0,         # 
                ("GA",    "유입 세션수"):0,       # session_count
                ("GA",    "평균체류시간(초)"):0,   # avg_session_duration_sec
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


    # ──────────────────────────────────
    # 브랜드별 채널명 자동으로 수집. 하드코딩 제거 
    # ──────────────────────────────────
    channel_brand = (
        PPL_LIST.loc[:, ['채널명', '브랜드', 'order']]
        .dropna(subset=['채널명', '브랜드'])
        .assign(order=lambda d: pd.to_numeric(d['order'], errors='coerce'))
        # order가 비어있거나 숫자 변환 실패 시 맨 뒤로 가도록 아주 작은 값으로 대체
        .assign(order=lambda d: d['order'].fillna(float('-inf')))
        # 브랜드 내에서 order 내림차순 정렬
        .sort_values(['브랜드', 'order'], ascending=[True, False])
        # 중복 채널명/브랜드 조합 있으면 첫 행(=가장 큰 order)만 보존
        .drop_duplicates(subset=['브랜드', '채널명'], keep='first')
    )

    CHANNELS_BY_BRAND = {
        b: g['채널명'].tolist()
        for b, g in channel_brand.groupby('브랜드', sort=False)
    }
    

    # (25.09.18 하드코딩 제거) "채널별 쿼리 기여량"용
    def decorate_df_ctb(df: pd.DataFrame, brand: str = 'sleeper') -> pd.DataFrame:
        # 브랜드 라벨 정규화
        brand_map = {'sleeper': '슬립퍼', 'nooer': '누어'}
        brand_kor = brand_map.get(brand, brand)  # 이미 한글이면 그대로

        # 채널 목록 자동 로드 (없으면 빈 리스트)
        channels = CHANNELS_BY_BRAND.get(brand_kor, [])

        # 필요한 기본 컬럼 채우기
        base_required = ['날짜', '검색량', '기본 검색량', '기본 검색량_비중']
        # 채널·비중 동적 required
        dyn_required = [c for ch in channels for c in (ch, f'{ch}_비중')]
        for c in base_required + dyn_required:
            if c not in df.columns:
                df[c] = 0

        # 숫자형 변환
        num_cols = ['검색량', '기본 검색량', '기본 검색량_비중'] + \
                [c for ch in channels for c in (ch, f'{ch}_비중')]
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

        # 컬럼 순서 동적 구성
        ordered = ['날짜', '검색량', '기본 검색량', '기본 검색량_비중'] + \
                [c for pair in [(ch, f'{ch}_비중') for ch in channels] for c in pair]
        df = df[ordered]

        # 날짜/무한대 처리
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce').dt.strftime('%Y-%m-%d')
        num_cols2 = df.select_dtypes(include=['number']).columns
        df[num_cols2] = df[num_cols2].replace([np.inf, -np.inf], np.nan).fillna(0)

        # MultiIndex 헤더 구성
        tuples = [
            ("기본정보", "날짜"),
            ("기본정보", "전체 검색량"),
            ("기본 검색량", "검색량"),
            ("기본 검색량", "비중(%)"),
        ]
        for ch in channels:
            tuples += [(ch, "검색량"), (ch, "비중(%)")]

        df.columns = pd.MultiIndex.from_tuples(tuples, names=["그룹", "지표"])
        return df


    # (25.09.18 하드코딩 제거) 
    def render_style_ctb(target_df, brand):
        # 먼저 데코레이션(여기서 MultiIndex 컬럼 완성됨)
        decorated = decorate_df_ctb(target_df, brand)

        # 채널 그룹명 동적 추출 (기본정보/기본 검색량 제외)
        groups = list(decorated.columns.get_level_values(0).unique())
        channel_groups = [g for g in groups if g not in ["기본정보", "기본 검색량"]]

        # 동적 포맷 맵
        decimals_map = {
            ("기본정보", "전체 검색량"): 0,
            ("기본 검색량", "비중(%)"): 1,
        }
        # 채널 비중은 모두 소수 1자리
        for g in channel_groups:
            decimals_map[(g, "비중(%)")] = 1

        suffix_map = {
            ("기본 검색량", "비중(%)"): " %",
        }
        for g in channel_groups:
            suffix_map[(g, "비중(%)")] = " %"

        styled = ui.style_format(decorated, decimals_map=decimals_map, suffix_map=suffix_map)
        st.dataframe(styled, use_container_width=True, row_height=30, hide_index=True)


    def render_stacked_bar(
        df, x, y, color,
        fixed_label="기본 검색량",
        fixed_color="#D5DAE5",  # 회색 고정, 회색 다신 등장 ㄴㄴ
    ):
        # 숫자형 보정
        def _to_numeric(cols):
            for c in cols:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        # 회색 제외 팔레트
        palette = px.colors.qualitative.Plotly
        color_map = {fixed_label: fixed_color}

        # ← 여기만 변경: fixed_label을 항상 "마지막"으로 보냄
        def _category_order(series_name):
            cats = df[series_name].dropna().astype(str).unique().tolist()
            if fixed_label in cats:
                cats = [c for c in cats if c != fixed_label] + [fixed_label]
            return cats

        if isinstance(y, (list, tuple)):
            _to_numeric(list(y))
            if color is not None and color in df.columns:
                long_df = df.melt(id_vars=[x, color], value_vars=list(y),
                                var_name="__series__", value_name="__value__")
                order = _category_order("__series__")
                fig = px.bar(
                    long_df, x=x, y="__value__", color="__series__", opacity=0.6,
                    color_discrete_map=color_map,
                    color_discrete_sequence=palette,
                    category_orders={"__series__": order},
                )
            else:
                fig = px.bar(df, x=x, y=list(y), opacity=0.6,
                            color_discrete_sequence=palette)
        else:
            _to_numeric([y])
            order = _category_order(color) if color else None
            fig = px.bar(
                df, x=x, y=y, color=color, opacity=0.6,
                color_discrete_map=color_map,
                color_discrete_sequence=palette,
                category_orders=({color: order} if order else None),
            )

        fig.update_layout(barmode="relative")
        fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
        fig.update_layout(
            bargap=0.1, bargroupgap=0.2, height=400,
            xaxis_title=None, yaxis_title=None,
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom", title=None),
        )
        fig.update_xaxes(tickformat="%m월 %d일")
        st.plotly_chart(fig, use_container_width=True)




    # ────────────────────────────────────────────────────────────────
    # 채널 목록 > 조금 컴팩트하게 수정
    # ────────────────────────────────────────────────────────────────

    # 카드 전용 CSS (컴팩트 스타일)
    st.markdown("""
    <style>
    .ppl-grid { gap: 6px !important; }  /* st.columns gap=small 과 조화 */

    .ppl-card {
        border:1px solid #e6e6e6;
        border-radius:8px;
        padding:12px 14px;
        margin-bottom:14px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.06);
        background:#fff;
    }
    .ppl-card .card-top {
        display:flex; align-items:center; justify-content:space-between;
        margin-bottom:1px;
    }
    .ppl-card .title {
        font-size:0.9rem; font-weight:700; color:#222; margin:0; 
        line-height:1.2;
    }
    .ppl-card .meta {
        font-size:0.7rem; color:#666; margin-top:2px;
    }
    .ppl-card .row {
        display:flex; justify-content:space-between; align-items:center;
        margin-top:1px; font-size:0.8rem;
    }
    .ppl-card .total { color:#333; }
    .ppl-card .total b { font-weight:700; }

    /* 브랜드 배지 */
    .ppl-badge {
        display:inline-block; padding:2px 8px; border-radius:999px;
        font-size:0.7rem; line-height:1.6; font-weight:500; color:#fff;
        white-space:nowrap;
    }
    .badge-slp { background:#FF4B4B; } /* 슬립퍼 */
    .badge-nor { background:#5562EA; } /* 누어 */

    /* 링크 */
    .ppl-link a { text-decoration:none; font-size:0.8rem; }
    .ppl-link a:hover { text-decoration:underline; }
    </style>
    """, unsafe_allow_html=True)



    # (25.11.10) 제목 + 설명 + 업데이트 시각 + 캐시초기화 
    # last_updated_time
    # 제목
    st.subheader("언드·PPL 대시보드 (수정중)")


    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()   # 파라미터 제거
        st.rerun()
        
    # 설명
    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="  
                font-size:14px;       
                line-height:1.5;      
            ">
            <b>PPL 채널별 성과</b>와, 
            랜딩 이후의 <b>사용자 행동</b>을 살펴볼 수 있으며, 
            전체 검색량 대비 <b>채널별 쿼리 기여량</b>을 파악할 수 있습니다.
            </div>
            <div style="
                color:#6c757d;        
                font-size:14px;       
                line-height:2.0;      
            ">
            ※ 키워드·쿼리 D-1 데이터는 매일 10시 ~ 11시 사이에 업데이트됩니다.
            </div>
            """,
            unsafe_allow_html=True
        )
        
    with col2:
        # last_updated_time
        
        # -> last_updated_time__query
        if isinstance(last_updated_time__query, str):
            lut_q = datetime.strptime(last_updated_time__query, "%Y-%m-%d")
        else:
            lut_q = last_updated_time__query
        lut_q_date = lut_q.date()
        
        # -> last_updated_time__GA
        if isinstance(last_updated_time__GA, str):
            lut_g = datetime.strptime(last_updated_time__GA, "%Y-%m-%d")
        else:
            lut_g = last_updated_time__GA
        lut_g_date = lut_g.date()
        
        now_kst   = datetime.now(ZoneInfo("Asia/Seoul"))
        today_kst = now_kst.date()
        
        delta_days_q = (today_kst - lut_q_date).days
        delta_days_g = (today_kst - lut_g_date).days
        
        # 기본값
        msg_q    = f"D-{delta_days_q} 업데이트 완료"
        sub_bg_q = "#E6F4EC"
        sub_bd_q = "#91C7A5"
        sub_fg_q = "#237A57"
        
        msg_g   = f"D-{delta_days_g} 업데이트 완료"
        sub_bg_g = "#fff7ed"
        sub_bd_g = "#fdba74"
        sub_fg_g = "#c2410c"
        
        # 렌더링
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
            <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 10px;
                font-size:13px;line-height:1.1;
                color:{sub_fg_g};background:{sub_bg_g};border:1px solid {sub_bd_g};
                border-radius:10px;white-space:nowrap;">
                📢 {msg_g}
            </span>
            <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 10px;
                font-size:13px;line-height:1.1;
                color:{sub_fg_q};background:{sub_bg_q};border:1px solid {sub_bd_q};
                border-radius:10px;white-space:nowrap;">
                📊 {msg_q}
            </span>
            <a href="?refresh=1" title="캐시 초기화" style="text-decoration:none;vertical-align:middle;">
                <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 8px;
                font-size:13px;line-height:1;
                color:#475569;background:#f8fafc;border:1px solid #e2e8f0;
                border-radius:10px;white-space:nowrap;">
                🗑️ 캐시 초기화
                </span>
            </a>
            </div>
            """,
            unsafe_allow_html=True
        )
    st.divider()

    # ──────────────────────────────────
    # 1) 채널 목록
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>채널 목록</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ전체 채널에 대한 집행 정보입니다. <span style='color:#8E9097;'>(최신 6개 정렬)</span> ", unsafe_allow_html=True)

    # 원본 DF 정렬
    df = PPL_LIST.copy()
    df = df.sort_values(by="order", ascending=False)

    # 브랜드별 분리
    df_slp = df[df["브랜드"] == "슬립퍼"].copy()
    df_nor = df[df["브랜드"] == "누어"].copy()


    # _render_card_grid
    def _render_card_grid(
        df_src: pd.DataFrame,
        cols_per_row: int = 6,
        key: str = "all",
        initial: int = 6,
        step: int = 12
    ):
        """브랜드 배지 포함 카드 그리드 + 더보기/접기"""
        if df_src is None or len(df_src) == 0:
            st.info("표시할 채널이 없습니다.")
            return

        # 탭/그룹별 노출 개수 상태
        state_key = f"ppl_view_{key}"
        if state_key not in st.session_state:
            st.session_state[state_key] = initial

        view_n = st.session_state[state_key]
        total = len(df_src)
        df_view = df_src.iloc[:view_n]

        # 카드 렌더 (기존 로직 재사용)
        rows = math.ceil(len(df_view) / cols_per_row)
        for i in range(rows):
            cols = st.columns(cols_per_row, gap="small")
            for j, col in enumerate(cols):
                idx = i * cols_per_row + j
                if idx >= len(df_view): break
                row = df_view.iloc[idx]

                brand = str(row.get("브랜드", "")).strip()
                badge_class = "badge-slp" if brand == "슬립퍼" else ("badge-nor" if brand == "누어" else "badge-slp")

                amount_raw = row.get("금액")
                money = "-"
                if pd.notna(amount_raw):
                    try:
                        money = f"{int(float(amount_raw)):,}원"
                    except Exception:
                        money = str(amount_raw)

                url = row.get("컨텐츠 URL")
                link_html = f"🔗 <a href='{url}' target='_blank'>컨텐츠 보기</a>" if url else "🔗 링크 없음"

                upload_date = row.get("업로드 날짜")
                upload_date_str = str(upload_date) if pd.notna(upload_date) else ""

                with col:
                    st.markdown(
                        f"""
                        <div class="ppl-card">
                        <div class="card-top">
                            <h4 class="title">{row.get('채널명','')}</h4>
                            <span class="ppl-badge {badge_class}">{brand if brand else '브랜드'}</span>
                        </div>
                        <div class="meta">{upload_date_str}</div>
                        <div class="row">
                            <div class="total">Total <b>{money}</b></div>
                            <div class="ppl-link">{link_html}</div>
                        </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        # --- 더보기 / 접기 컨트롤 (간소화 & 잔여 버튼 제거) ---
        if total > view_n:
            if st.button(f"더보기  ({view_n}/{total})", key=f"more_{key}", use_container_width=True):
                st.session_state[state_key] = min(total, view_n + step)
                st.rerun()
        elif total > initial:
            if st.button("접기", key=f"less_{key}", use_container_width=True):
                st.session_state[state_key] = initial
                st.rerun()

    tab1, tab2, tab3 = st.tabs(["전체", "슬립퍼", "누어"])
    with tab1:
        _render_card_grid(df,     cols_per_row=6, key="all")   # 전체
    with tab2:
        _render_card_grid(df_slp, cols_per_row=6, key="slp")   # 슬립퍼
    with tab3:
        _render_card_grid(df_nor, cols_per_row=6, key="nor")   # 누어




    # ────────────────────────────────────────────────────────────────
    # (25.10.10 추가) 캠페인 효과(초반 피크+감소 일치도)
    # ────────────────────────────────────────────────────────────────
    def render_campaign_effect(df_merged_t: pd.DataFrame, ch: str, L: int = 5):
        """
        단일 함수 버전
        - df_merged_t: ['날짜','채널명','Cost','조회수','좋아요수','댓글수','브랜드언급량','링크 클릭수'] 포함
        - ch: 탭에서 넘겨주는 채널명 (자동 선택)
        - L : 평가 일수(3~7 권장). 데이터가 부족하면 가능한 길이로 자동 조정
        - T0는 자동: '조회수>0'인 첫 날짜 (없으면 해당 채널의 첫 날짜)
        """
        import numpy as np
        import pandas as pd
        import plotly.graph_objects as go
        from datetime import timedelta

        # ──────────────────────────────────
        # 내부 헬퍼들 (함수 외부 정의 X)
        # ──────────────────────────────────
        def _as_daily(df: pd.DataFrame) -> pd.DataFrame:
            d = df.copy()
            d["날짜"] = pd.to_datetime(d["날짜"], errors="coerce")
            d = d.dropna(subset=["날짜"]).sort_values("날짜").set_index("날짜").asfreq("D")
            return d

        def _inc_from_cum(s: pd.Series) -> pd.Series:
            # 누적 → 증가분, 음수 방지
            return s.diff().clip(lower=0).fillna(0)

        def _front_loading_index(y: np.ndarray, k: int = 2, n_perm: int = 5000, seed: int = 42) -> dict:
            """
            FLI = (앞 k일 합) / (전체 합). 퍼뮤테이션 p-value(우측꼬리).
            y: 길이 L(3~7)의 비음 아닌 일일 값
            """
            y = np.array(y, dtype=float)
            y[y < 0] = 0.0
            L = len(y)
            if L < max(3, k + 1) or y.sum() <= 0:
                return {"fli": np.nan, "p": np.nan, "k": k, "L": L}

            fli_obs = float(y[:k].sum() / y.sum())

            rng = np.random.default_rng(seed)
            null = []
            for _ in range(n_perm):
                yb = y.copy()
                rng.shuffle(yb)
                fli_b = float(yb[:k].sum() / yb.sum())
                null.append(fli_b)
            null = np.array(null)

            # 우측꼬리 (초반 집중이 클수록 큼)
            p = float((np.sum(null >= fli_obs) + 1) / (n_perm + 1))
            return {"fli": fli_obs, "p": p, "k": k, "L": L}

        def _exp_decay_fit(y: np.ndarray) -> dict:
            """
            y ≈ A * exp(-λ t) (t=1..L) 지수감쇠 OLS 적합 (로그선형화).
            반환: lambda(감쇠율, +면 감소), R2 (0~1), A
            """
            y = np.array(y, dtype=float)
            y[y < 0] = 0.0
            L = len(y)
            if L < 3 or np.all(y <= 0):
                return {"lam": np.nan, "R2": np.nan, "A": np.nan}

            t = np.arange(1, L + 1, dtype=float)
            # 0 값 방지 위해 작은 epsilon 추가
            eps = max(1e-9, 1e-6 * (y.max() if y.max() > 0 else 1.0))
            ly = np.log(y + eps)

            # 선형회귀 ly = c - λ t
            X = np.column_stack([np.ones_like(t), -t])
            beta, *_ = np.linalg.lstsq(X, ly, rcond=None)
            c, lam = beta[0], beta[1]  # lam >= 0 이면 감소 경향

            ly_hat = X @ beta
            ss_res = float(np.sum((ly - ly_hat) ** 2))
            ss_tot = float(np.sum((ly - ly.mean()) ** 2))
            R2 = 0.0 if ss_tot <= 0 else max(0.0, min(1.0, 1.0 - ss_res / ss_tot))
            A = float(np.exp(c))
            return {"lam": float(lam), "R2": float(R2), "A": A}

        def _peak_day(y: np.ndarray) -> str:
            if (y is None) or (len(y) == 0) or np.all(~np.isfinite(y)):
                return "N/A"
            i = int(np.nanargmax(y))  # 0-based
            return f"D{i+1}"

        # 1) 채널 필터 & T0 자동 설정
        cols_need = ["날짜", "채널명", "조회수", "브랜드언급량", "링크 클릭수"]
        dfc = df_merged_t.loc[df_merged_t["채널명"] == ch, cols_need].copy()
        if dfc.empty:
            st.info("표시할 채널 데이터가 없습니다.")
            return

        dfc["날짜"] = pd.to_datetime(dfc["날짜"], errors="coerce")
        dfc = dfc.dropna(subset=["날짜"]).sort_values("날짜")

        # T0 자동: 조회수>0인 첫 날짜 (없으면 해당 채널의 첫 날짜)
        nonzero_dates = dfc.loc[(dfc["조회수"].astype(float) > 0), "날짜"]
        if len(nonzero_dates):
            T0 = pd.to_datetime(nonzero_dates.iloc[0])
        else:
            T0 = pd.to_datetime(dfc["날짜"].iloc[0])

        # 일단위로 정규화
        d = _as_daily(dfc)

        # 2) 누적→증가분 변환
        d["views_inc"]    = _inc_from_cum(d["조회수"])
        d["mentions_inc"] = _inc_from_cum(d["브랜드언급량"])
        d["clicks"]       = d["링크 클릭수"].fillna(0).astype(float)

        # 평가 창 실제 길이 보정
        t_end = T0 + timedelta(days=max(0, L - 1))
        t_end = min(t_end, d.index.max())
        win = d.loc[T0:t_end, ["views_inc", "mentions_inc", "clicks"]].fillna(0)
        L_eff = len(win)
        if L_eff < 3:
            st.warning("평가 구간 데이터가 3일 미만입니다.")
            return

        # 3) FLI + 지수감쇠 보조지표
        v = win["views_inc"].values
        m = win["mentions_inc"].values
        c = win["clicks"].values

        # --- (NEW) FLI(2일, 3일) → 평균 FLI, p-value는 보수적으로 min 사용
        def _fli_23(y: np.ndarray) -> dict:
            r2 = _front_loading_index(y, k=2, n_perm=5000)
            r3 = _front_loading_index(y, k=3, n_perm=5000) if len(y) >= 3 else {"fli": np.nan, "p": np.nan}
            fli_mean = float(np.nanmean([r2["fli"], r3["fli"]])) if not (np.isnan(r2["fli"]) and np.isnan(r3["fli"])) else np.nan
            p_comb  = float(np.nanmin([r2["p"], r3["p"]]))      if not (np.isnan(r2["p"])  and np.isnan(r3["p"]))  else np.nan
            return {"fli": fli_mean, "p": p_comb}

        res_v = _fli_23(v)
        res_m = _fli_23(m)
        res_c = _fli_23(c)

        fit_v = _exp_decay_fit(v)
        fit_m = _exp_decay_fit(m)
        fit_c = _exp_decay_fit(c)



        # 시각화 - 카드
        def _fmt_res(res):
            fli = res["fli"]; p = res["p"]
            return (f"{fli:.2f}" if np.isfinite(fli) else "N/A",
                    f"{p:.3f}"   if np.isfinite(p)   else "N/A")

        f_v, p_v = _fmt_res(res_v); pk_v = _peak_day(v)
        f_m, p_m = _fmt_res(res_m); pk_m = _peak_day(m)
        f_c, p_c = _fmt_res(res_c); pk_c = _peak_day(c)

        c1, c2, c3 = st.columns(3)
        with c1:
            with st.container(border=True):
                st.markdown("**조회수**")
                col_a, col_b, col_c, col_d = st.columns(4, gap="small")
                with col_a:
                    st.metric("FLI", f_v, help="초반 2일이 전체에서 차지하는 비중. 높을수록 초반 효과가 큼")
                with col_b:
                    st.metric("Highest Peak", pk_v)
                with col_c:
                    st.metric("P-Value", p_v, help="퍼뮤테이션 결과 초반 집중이 우연일 가능성. 낮을수록 유의함")
                with col_d:
                    st.metric("감쇠 적합 R²", f"{fit_v['R2']:.2f}" if np.isfinite(fit_v['R2']) else "N/A", help="R²가 높을수록 전형적인 초반효과 패턴에 가까움")

        with c2:
            with st.container(border=True):
                st.markdown("**브랜드언급량**")
                col_a, col_b, col_c, col_d = st.columns(4, gap="small")
                with col_a:
                    st.metric("FLI", f_m)
                with col_b:
                    st.metric("Highest Peak", pk_m)
                with col_c:
                    st.metric("P-Value", p_m)
                with col_d:
                    st.metric("감쇠 적합 R²", f"{fit_m['R2']:.2f}" if np.isfinite(fit_m['R2']) else "N/A")
        
        with c3:
            with st.container(border=True):
                st.markdown("**링크클릭수**")
                col_a, col_b, col_c, col_d = st.columns(4, gap="small")
                with col_a:
                    st.metric("FLI", f_c)
                with col_b:
                    st.metric("Highest Peak", pk_c)
                with col_c:
                    st.metric("P-Value", p_c)
                with col_d:
                    st.metric("감쇠 적합 R²", f"{fit_c['R2']:.2f}" if np.isfinite(fit_c['R2']) else "N/A")

        # 시각화 - 차트
        # plot_df = win.copy()
        # for col in plot_df.columns:
        #     mx = plot_df[col].max()
        #     plot_df[col] = plot_df[col] / (mx if mx > 0 else 1.0)

        # fig = go.Figure()
        # fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["views_inc"],    mode="lines+markers", name="조회수"))
        # fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["mentions_inc"], mode="lines+markers", name="브랜드언급량"))
        # fig.add_trace(go.Scatter(x=plot_df.index, y=plot_df["clicks"],       mode="lines+markers", name="링크클릭수"))

        # # T0 수직선 + 라벨
        # fig.add_vline(x=T0, line_dash="dash", opacity=0.6)
        # fig.add_annotation(x=T0, y=1.05, text="T0 시작", showarrow=False, yref="paper")

        # fig.update_layout(
        #     title=f"[{ch}] 캠페인 초반-피크 일치도(정규화) · T0={T0.date()} · L={L_eff}일",
        #     margin=dict(l=10, r=10, t=60, b=10),
        #     legend=dict(orientation="h", y=1.08, x=1, xanchor="right"),
        #     yaxis=dict(title="정규화(각 지표, max=1)"),
        #     xaxis_title=None
        # )
        # st.plotly_chart(fig, use_container_width=True)

        # 시각화 - 차트 (개선)
        plot_df = win.copy()
        for col in plot_df.columns:
            mx = plot_df[col].max()
            plot_df[col] = plot_df[col] / (mx if mx > 0 else 1.0)


        fig = go.Figure()

        # 부드러운 라인(spline) + 작은 마커
        fig.add_trace(go.Scatter(
            x=plot_df.index, y=plot_df["views_inc"],
            mode="lines+markers", name="조회수",
            line=dict(shape="spline", width=2),
            marker=dict(size=6),
            hovertemplate="날짜: %{x|%Y-%m-%d}<br>정규화 값: %{y:.2f}<extra></extra>"
        ))
        fig.add_trace(go.Scatter(
            x=plot_df.index, y=plot_df["mentions_inc"],
            mode="lines+markers", name="브랜드언급량",
            line=dict(shape="spline", width=2),
            marker=dict(size=6),
            hovertemplate="날짜: %{x|%Y-%m-%d}<br>정규화 값: %{y:.2f}<extra></extra>"
        ))
        fig.add_trace(go.Scatter(
            x=plot_df.index, y=plot_df["clicks"],
            mode="lines+markers", name="링크클릭수",
            line=dict(shape="spline", width=2),
            marker=dict(size=6),
            hovertemplate="날짜: %{x|%Y-%m-%d}<br>정규화 값: %{y:.2f}<extra></extra>"
        ))

        # T0 수직선
        fig.add_vline(x=T0, line_dash="dot", line_width=1.5, opacity=0.6)

        # 초반효과(2~3일) 구간 하이라이트: T0 ~ T0+(k-1)
        k_highlight = min(3, L_eff)
        early_end = T0 + timedelta(days=k_highlight - 1)
        fig.add_vrect(
            x0=T0, x1=early_end,
            fillcolor="LightSalmon", opacity=0.15, line_width=0,
            annotation_text="초반효과(2~3일)", annotation_position="top left"
        )

        # 레이아웃: 날짜 눈금, 유니파이드 툴팁, 여백, 눈금 포맷
        fig.update_layout(
            title=f"[{ch}] 캠페인 초반-피크 일치도(정규화) · T0={T0.date()} · L={L_eff}일",
            margin=dict(l=10, r=10, t=60, b=10),
            legend=dict(orientation="h", y=1.08, x=1, xanchor="right"),
            yaxis=dict(title="정규화(각 지표, max=1)"),
            xaxis_title=None,
            hovermode="x unified"
        )

        # x축: 일 단위 눈금 + 보기 좋은 포맷(YYYY-MM-DD → MM-DD)
        # (밀림 방지: ticklabelmode='instant', dtick=1day(ms), tick0=T0 자정)
        fig.update_xaxes(
            type="date",
            ticklabelmode="instant",                 # ← period 제거
            dtick=24*60*60*1000,                     # 1 day in milliseconds
            tickformat="%m-%d",
            tick0=pd.to_datetime(T0).normalize()     # 첫 눈금을 T0 자정에 맞춤(선택)
        )

        # y축: 살짝 여유 (0~1 범위에 padding)
        fig.update_yaxes(range=[-0.05, 1.05])

        st.plotly_chart(fig, use_container_width=True)




    # ────────────────────────────────────────────────────────────────
    # 채널별 인게이지먼트 및 액션
    # ────────────────────────────────────────────────────────────────
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
    
    
    # 탭 라벨: 실제로 데이터가 있는 채널만, order 내림차순
    order_map = (
        PPL_LIST.loc[:, ["채널명","order"]]
        .assign(order=lambda d: pd.to_numeric(d["order"], errors="coerce"))
        .assign(order=lambda d: d["order"].fillna(float("-inf")))
        .drop_duplicates(subset=["채널명"], keep="first")
        .set_index("채널명")["order"]
        .to_dict()
    )

    channels_present = (
        df_merged_t["채널명"]
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    tab_labels = sorted(
        channels_present,
        key=lambda ch: order_map.get(ch, float("-inf")),
        reverse=True  # 큰 order 먼저
    )

    if not tab_labels:
        st.info("표시할 채널 데이터가 없습니다.")
    else:
        tabs = st.tabs(tab_labels)
        for ch, t in zip(tab_labels, tabs):
            with t:
                # (25.10.10 로직 추가)
                # render_campaign_effect(df_merged_t, ch=ch, L=10)

                
                # 기존
                c1, c2, _ = st.columns([1,1,11])
                add_cvr = c1.checkbox("CVR 추가", key=f"{ch}_cvr", value=False)
                add_cpa = c2.checkbox("CPA 추가", key=f"{ch}_cpa", value=False)
                if add_cvr and add_cpa: opt = 4
                elif add_cvr:           opt = 2
                elif add_cpa:           opt = 3
                else:                   opt = 1

                render_style_eng(
                    df_merged_t[df_merged_t["채널명"] == ch].copy(),
                    select_option=opt
                )
                




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

        
    with tab1:
        # 동적 채널 목록
        channels_slp = CHANNELS_BY_BRAND.get('슬립퍼', [])

        # 슬립퍼 데이터 결합
        df_QueryContribution = ppl_action3.merge(
            query_sum_slp[['날짜','검색량']], on='날짜', how='outer'
        )

        # 누락 채널 컬럼 0으로 생성
        for ch in channels_slp:
            if ch not in df_QueryContribution.columns:
                df_QueryContribution[ch] = 0

        # 숫자형 변환
        cols_to_int = channels_slp + ['검색량']
        if cols_to_int:
            df_QueryContribution[cols_to_int] = (
                df_QueryContribution[cols_to_int]
                .apply(pd.to_numeric, errors='coerce')
                .fillna(0).astype(int)
            )
        else:
            df_QueryContribution['검색량'] = pd.to_numeric(
                df_QueryContribution.get('검색량', 0), errors='coerce'
            ).fillna(0).astype(int)

        # 기본 검색량 및 비중
        df_QueryContribution["기본 검색량"] = (
            df_QueryContribution["검색량"] - df_QueryContribution[channels_slp].sum(axis=1)
        ).clip(lower=0)

        for col in (channels_slp + ['기본 검색량']):
            df_QueryContribution[f"{col}_비중"] = np.where(
                df_QueryContribution['검색량'] > 0,
                df_QueryContribution[col] / df_QueryContribution['검색량'] * 100,
                0.0
            ).round(2)

        # 컬럼 순서 동적
        ordered_cols = (
            ['날짜','검색량','기본 검색량','기본 검색량_비중'] +
            [c for pair in [(ch, f"{ch}_비중") for ch in channels_slp] for c in pair]
        )
        df_QueryContribution = df_QueryContribution[ordered_cols].sort_values("날짜", ascending=True)

        # 기간 슬라이더 (키 유지: slider_01)
        from pandas.tseries.offsets import MonthEnd
        df_QueryContribution["날짜_dt"] = pd.to_datetime(df_QueryContribution["날짜"], format="%Y-%m-%d", errors="coerce")
        start_period = (df_QueryContribution["날짜_dt"].min().to_period("M")
                        if df_QueryContribution["날짜_dt"].notna().any()
                        else pd.Timestamp.now().to_period("M"))
        curr_period  = pd.Timestamp.now().to_period("M")
        month_options = [p.to_timestamp() for p in pd.period_range(start=start_period, end=curr_period, freq="M")]
        now = pd.Timestamp.now()
        curr_ts = now.to_period("M").to_timestamp()
        prev_ts = (now.to_period("M") - 1).to_timestamp()

        st.markdown(" ")
        selected_range = st.select_slider(
            "🚀 기간 선택ㅤ(지난달부터 이번달까지가 기본 선택되어 있습니다)",
            options=month_options,
            value=(prev_ts, curr_ts),
            format_func=lambda x: x.strftime("%Y-%m"),
            key="slider_01"
        )
        start_sel, end_sel = selected_range
        period_start = start_sel
        period_end   = end_sel + MonthEnd(0)

        df_filtered = df_QueryContribution[
            (df_QueryContribution["날짜_dt"] >= period_start) &
            (df_QueryContribution["날짜_dt"] <= period_end)
        ].copy()
        df_filtered["날짜"] = df_filtered["날짜_dt"].dt.strftime("%Y-%m-%d")

        # 차트(채널들 + 기본 검색량)
        plot_cols = channels_slp + ['기본 검색량']
        df_long = df_filtered.melt(id_vars='날짜', value_vars=plot_cols, var_name='콘텐츠', value_name='기여량')
        render_stacked_bar(df_long, x="날짜", y="기여량", color="콘텐츠")

        # 테이블 (동적 포맷)
        render_style_ctb(df_filtered.drop(columns=['날짜_dt']), brand='슬립퍼')

    
    with tab2:
        channels_nor = CHANNELS_BY_BRAND.get('누어', [])

        df_QueryContribution_nor = ppl_action3.merge(
            query_sum_nor[['날짜','검색량']], on='날짜', how='outer'
        )

        for ch in channels_nor:
            if ch not in df_QueryContribution_nor.columns:
                df_QueryContribution_nor[ch] = 0

        cols_to_int = channels_nor + ['검색량']
        if cols_to_int:
            df_QueryContribution_nor[cols_to_int] = (
                df_QueryContribution_nor[cols_to_int]
                .apply(pd.to_numeric, errors='coerce')
                .fillna(0).astype(int)
            )
        else:
            df_QueryContribution_nor['검색량'] = pd.to_numeric(
                df_QueryContribution_nor.get('검색량', 0), errors='coerce'
            ).fillna(0).astype(int)

        df_QueryContribution_nor["기본 검색량"] = (
            df_QueryContribution_nor["검색량"] - df_QueryContribution_nor[channels_nor].sum(axis=1)
        ).clip(lower=0)

        for col in (channels_nor + ['기본 검색량']):
            df_QueryContribution_nor[f"{col}_비중"] = np.where(
                df_QueryContribution_nor['검색량'] > 0,
                df_QueryContribution_nor[col] / df_QueryContribution_nor['검색량'] * 100,
                0.0
            ).round(2)

        ordered_cols_nor = (
            ['날짜','검색량','기본 검색량','기본 검색량_비중'] +
            [c for pair in [(ch, f"{ch}_비중") for ch in channels_nor] for c in pair]
        )
        df_QueryContribution_nor = df_QueryContribution_nor[ordered_cols_nor].sort_values("날짜", ascending=True)

        # 기간 슬라이더 (키 유지: slider_02)
        from pandas.tseries.offsets import MonthEnd
        df_QueryContribution_nor["날짜_dt"] = pd.to_datetime(df_QueryContribution_nor["날짜"], format="%Y-%m-%d", errors="coerce")
        start_period = (df_QueryContribution_nor["날짜_dt"].min().to_period("M")
                        if df_QueryContribution_nor["날짜_dt"].notna().any()
                        else pd.Timestamp.now().to_period("M"))
        curr_period  = pd.Timestamp.now().to_period("M")
        month_options = [p.to_timestamp() for p in pd.period_range(start=start_period, end=curr_period, freq="M")]
        now = pd.Timestamp.now()
        curr_ts = now.to_period("M").to_timestamp()
        prev_ts = (now.to_period("M") - 1).to_timestamp()

        st.markdown(" ")
        selected_range = st.select_slider(
            "🚀 기간 선택ㅤ(지난달부터 이번달까지가 기본 선택되어 있습니다)",
            options=month_options,
            value=(prev_ts, curr_ts),
            format_func=lambda x: x.strftime("%Y-%m"),
            key="slider_02"
        )
        start_sel, end_sel = selected_range
        period_start = start_sel
        period_end   = end_sel + MonthEnd(0)

        df_filtered_nor = df_QueryContribution_nor[
            (df_QueryContribution_nor["날짜_dt"] >= period_start) &
            (df_QueryContribution_nor["날짜_dt"] <= period_end)
        ].copy()
        df_filtered_nor["날짜"] = df_filtered_nor["날짜_dt"].dt.strftime("%Y-%m-%d")

        plot_cols = channels_nor + ['기본 검색량']
        df_long = df_filtered_nor.melt(id_vars='날짜', value_vars=plot_cols, var_name='콘텐츠', value_name='기여량')
        render_stacked_bar(df_long, x="날짜", y="기여량", color="콘텐츠")

        render_style_ctb(df_filtered_nor.drop(columns=['날짜_dt']), brand='누어')

        
    
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
            default_kw   = [kw for kw in keywords if ('슬리퍼' in kw) or ('슬립퍼' in kw) or ('SLEEPER' in kw)] # SLEEPER 추까 (25.09.02)
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




if __name__ == '__main__':
    main()
