# 서희_최신수정일_25-08-20

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta
import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import JsCode
# from oauth2client.service_account import ServiceAccount
from google.oauth2.service_account import Credentials
import gspread
import math
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import re

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
        .reportview-container .main .block-container {
            max-width: 100% !important;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.subheader('퍼포먼스 대시보드')
    st.markdown("""
    이 대시보드는 **GA와 광고 데이터를 연결**해서, 광고비부터 유입, 전환까지 **주요 마케팅 성과**를 한눈에 확인할 수 있는 맞춤 대시보드입니다.  
    여기서는 **기간, 매체, 브랜드, 품목 등 원하는 조건을 선택해서**, 광고 성과 지표들을 자유롭게 비교 · 분석할 수 있습니다.
    """)
    st.link_button(
    "🔍 대시보드 사용 가이드", 
    "https://www.notion.so/Views-241521e07c7680df86eecf5c5f8da4af#241521e07c76805198d9eaf0c28deadb"
    )
    st.divider()


    # ────────────────────────────────────────────────────────────────
    # 사이드바 필터 설정
    # ────────────────────────────────────────────────────────────────
    st.sidebar.header("Filter")
    
    today = datetime.now().date()
    default_end = today - timedelta(days=1)
    default_start = today - timedelta(days=14)
    start_date, end_date = st.sidebar.date_input(
        "기간 선택",
        value=[default_start, default_end],
        max_value=default_end
    )
    cs = start_date.strftime("%Y%m%d")
    ce = end_date.strftime("%Y%m%d")


    # 추가 -----------------------------------------------
    use_compare = st.sidebar.checkbox("비교기간 사용")
    # 기본 비교기간 계산 (기간 길이 만큼 동기간 이전)
    period_len    = (end_date - start_date).days + 1
    default_comp_e= start_date - timedelta(days=1)
    default_comp_s= default_comp_e  - timedelta(days=period_len-1)
    if use_compare:
        comp_start, comp_end = st.sidebar.date_input(
            "비교 기간 선택",
            value=[default_comp_s, default_comp_e],
            max_value=default_comp_e
        )
    show_totals  = st.sidebar.checkbox("기간별 합계 보기")
    # ---------------------------------------------------

    # 글로벌 변수로 핸들.. 휴 다행 먹혀서
    start_date_str = str(start_date.strftime("%m/%d"))
    end_date_str = str(end_date.strftime("%m/%d"))
    default_comp_e_str = str(default_comp_e.strftime("%m/%d"))
    default_comp_s_str = str(default_comp_s.strftime("%m/%d"))
    # ---------------------------------------------------

    @st.cache_data(ttl=3600)
    def load_data(cs: str, ce: str) -> pd.DataFrame:

        # 1) tb_media
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df_bq = bq.get_data("tb_media")
        df_bq["event_date"] = pd.to_datetime(df_bq["event_date"], format="%Y%m%d")
        parts = df_bq['campaign_name'].str.split('_', n=5, expand=True)
        df_bq['campaign_name_short'] = df_bq['campaign_name']
        mask = parts[5].notna()
        df_bq.loc[mask, 'campaign_name_short'] = (
            parts.loc[mask, :4].apply(lambda r: '_'.join(r.dropna().astype(str)), axis=1)
        )
        # 2) Google Sheet
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

        try: 
            creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
        except: # 배포용 (secrets.toml)
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str):  # 혹시 문자열(JSON)로 저장했을 경우
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/11ov-_o6Lv5HcuZo1QxrKOZnLtnxEKTiV78OFBZzVmWA/edit')
        df_sheet = pd.DataFrame(sh.worksheet('parse').get_all_records())
        
        # merge (1+2)
        merged = df_bq.merge(df_sheet, how='left', on='campaign_name_short')
        # cost_gross
        merged['cost_gross'] = np.where(
            merged['media_name'].isin(['GOOGLE','META']), merged['cost']*1.1/0.98, merged['cost']
        )
        # handle NSA
        cond = (
            (merged['media_name']=='NSA') & merged['utm_source'].isna() &
            merged['utm_medium'].isna() & merged['media_name_type'].isin(['RSA_AD','TEXT_45'])
        )
        merged.loc[cond, ['utm_source','utm_medium']] = ['naver','search-nonmatch']
        
        merged["event_date"] = merged["event_date"].dt.strftime("%Y-%m-%d")
        return merged


    # ────────────────────────────────────────────────────────────────
    # 데이터 불러오기
    # ────────────────────────────────────────────────────────────────
    st.toast("GA D-1 데이터는 오전에 예비 처리되고, **15시 이후에 최종 업데이트** 됩니다.", icon="🔔")
    # df_merged = load_data(cs, ce)
    # df_filtered = df_merged.copy()

    header_map = {
        'event_date':       '날짜',
        'media_name':       '매체',
        'utm_source':       '소스',
        'utm_medium':       '미디엄',
        'brand_type':       '브랜드',
        'funnel_type':      '퍼널',
        'product_type':     '품목',
        'campaign_name':    '캠페인',
        'adgroup_name':     '광고그룹',
        'ad_name':          '광고소재',
        'keyword_name':     '키워드',
        'utm_content':      '컨텐츠',
        'utm_term':         '검색어',
    }
    
    # 추가 -----------------------------------------------
    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        if use_compare:
            # cs~ce, cs_cmp~ce_cmp 한 번에 로드
            cs_cmp = comp_start.strftime("%Y%m%d")
            df_merged = load_data(cs_cmp, ce)
            df_merged['event_date'] = pd.to_datetime(df_merged['event_date'])  # ← 추가
            
            df_primary = df_merged[
                (df_merged.event_date >= pd.to_datetime(start_date)) &
                (df_merged.event_date <= pd.to_datetime(end_date))
            ]
            df_compare = df_merged[
                (df_merged.event_date >= pd.to_datetime(comp_start)) &
                (df_merged.event_date <= pd.to_datetime(comp_end))
            ]
        else:
            df_merged  = load_data(cs, ce)
            df_merged['event_date'] = pd.to_datetime(df_merged['event_date'])  # ← 추가
            df_primary = df_merged
        
        df_filtered     = df_primary.copy()
        df_filtered_cmp = df_compare.copy() if use_compare else None
    # ---------------------------------------------------


    # def render_aggrid(
    #     df: pd.DataFrame,
    #     pivot_cols: list[str],
    #     height: int = 480,
    #     use_parent: bool = True
    #     ) -> None:
    #     """
    #     use_parent: False / True
    #     """
    #     df2 = df.copy()
        
    #     if 'event_date' in df2.columns:
    #         df2['event_date'] = pd.to_datetime(df2['event_date']).dt.strftime('%Y-%m-%d')
        
    #     df2.fillna(0, inplace=True)
    #     df2 = df2.where(pd.notnull(df2), None)
    #     df2.replace([np.inf, -np.inf], 0, inplace=True)
        
    #     # 전처리 영역 (파생지표 생성)
    #     # df2['CPC'] = (df2['cost_gross_sum'] / df2['clicks_sum']).round(0)
    #     # df2['CTR'] = (df2['clicks_sum'] / df2['impressions_sum'] * 100).round(2)
    #     df2['CPC'] = ((df2['cost_gross_sum'] / df2['clicks_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(0))
    #     df2['CTR'] = ((df2['clicks_sum'] / df2['impressions_sum'] * 100).replace([np.inf, -np.inf], 0).fillna(0).round(2))
        
    #     df2['session_count_CPA'] = (df2['cost_gross_sum'] / df2['session_count']).replace([np.inf, -np.inf], 0).fillna(0).round(0)
    #     df2['view_item_CPA'] = (df2['cost_gross_sum'] / df2['view_item_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
    #     df2['product_page_scroll_50_CPA'] = (df2['cost_gross_sum'] / df2['product_page_scroll_50_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
    #     df2['product_option_price_CPA'] = (df2['cost_gross_sum'] / df2['product_option_price_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
    #     df2['find_nearby_showroom_CPA'] = (df2['cost_gross_sum'] / df2['find_nearby_showroom_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
    #     df2['showroom_10s_CPA'] = (df2['cost_gross_sum'] / df2['showroom_10s_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
    #     df2['showroom_leads_CPA'] = (df2['cost_gross_sum'] / df2['showroom_leads_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
    #     df2['add_to_cart_CPA'] = (df2['cost_gross_sum'] / df2['add_to_cart_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
    #     df2['purchase_CPA'] = (df2['cost_gross_sum'] / df2['purchase_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        

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
        

    #     # (use_parent) flat_cols
    #     flat_cols = []

    #     # (use_parent) grouped_cols
    #     dynamic_cols = [ # (추가) pivot_cols (선택한 행필드)를 받아야함
    #         {
    #             "headerName": header_map.get(col, col),
    #             "field": col,
    #             "pinned": "left",
    #             "width": 100,
    #             "minWidth": 100,
    #             "flex": 1
    #         }
    #         for col in pivot_cols
    #     ]
    #     static_cols = [
    #         {
    #             "headerName": "MEDIA",
    #             "children": [
    #                 make_num_child("광고비",      "cost_sum"),
    #                 make_num_child("광고비(G)",   "cost_gross_sum"),
    #                 make_num_child("노출수",      "impressions_sum"),
    #                 make_num_child("클릭수",      "clicks_sum"),
    #                 make_num_child("CPC",        "CPC"),
    #                 make_num_child("CTR",        "CTR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #         {
    #             "headerName": "GA & MEDIA",
    #             "children": [
    #                 make_num_child("세션수",         "session_count"),
    #                 make_num_child("세션 CPA",       "session_count_CPA"),                    
    #                 make_num_child("PDP조회",        "view_item_sum"),
    #                 make_num_child("PDP조회 CPA",    "view_item_CPA"),
    #                 make_num_child("PDPscr50",      "product_page_scroll_50_sum"),
    #                 make_num_child("PDPscr50 CPA",  "product_page_scroll_50_CPA"),
    #                 make_num_child("가격표시",       "product_option_price_sum"),
    #                 make_num_child("가격표시 CPA",   "product_option_price_CPA"),
    #                 make_num_child("쇼룸찾기",       "find_nearby_showroom_sum"),
    #                 make_num_child("쇼룸찾기 CPA",   "find_nearby_showroom_CPA"),
    #                 make_num_child("쇼룸10초",       "showroom_10s_sum"),
    #                 make_num_child("쇼룸10초 CPA",   "showroom_10s_CPA"),
    #                 make_num_child("장바구니",       "add_to_cart_sum"),
    #                 make_num_child("장바구니 CPA",   "add_to_cart_CPA"),
    #                 make_num_child("쇼룸예약",       "showroom_leads_sum"),
    #                 make_num_child("쇼룸예약 CPA",   "showroom_leads_CPA"),
    #                 make_num_child("구매하기 ",      "purchase_sum"),
    #                 make_num_child("구매하기 CPA",   "purchase_CPA"),
    #                 # make_num_child("",   ""),
    #                 # make_num_child("",   ""),
    #             ]
    #         },
    #     ]
    
    #     grouped_cols = dynamic_cols + static_cols
        
    #     # (use_parent)
    #     column_defs = grouped_cols if use_parent else flat_cols

    #     # grid_options & 렌더링
    #     grid_options = {
    #         "columnDefs": column_defs,
    #         "defaultColDef": {
    #             "sortable": True,
    #             "filter": True,
    #             "resizable": True,
    #             "minWidth": 100,
    #             "wrapHeaderText": True,
    #         },
    #         "headerHeight": 50,
    #         "groupHeaderHeight": 30,
    #         "autoHeaderHeight": True,
    #     }

    #     # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #     grid_options = add_summary(
    #         grid_options,
    #         df2,
    #         {
    #             'cost_sum': 'sum',
    #             'cost_gross_sum': 'sum',
    #             'impressions_sum': 'sum',
    #             'clicks_sum': 'sum',
    #             'CPC': 'avg',
    #             'CTR': 'avg',
    #             'session_count': 'sum',
    #             'session_count_CPA' : 'avg',
    #             'view_item_sum': 'sum',
    #             'view_item_CPA' : 'avg',
    #             'product_page_scroll_50_sum': 'sum',
    #             'product_page_scroll_50_CPA' : 'avg',
    #             'product_option_price_sum': 'sum',
    #             'product_option_price_CPA' : 'avg',
    #             'find_nearby_showroom_sum': 'sum',
    #             'find_nearby_showroom_CPA' : 'avg',
    #             'showroom_10s_sum': 'sum',
    #             'showroom_10s_CPA' : 'avg',
    #             'add_to_cart_sum': 'sum',
    #             'add_to_cart_CPA' : 'avg',
    #             'showroom_leads_sum': 'sum',
    #             'showroom_leads_CPA' : 'avg',
    #             'purchase_sum': 'sum',
    #             'purchase_CPA' : 'avg',
    #         }
    #     )

    #     AgGrid(
    #         df2,
    #         gridOptions=grid_options,
    #         height=height,
    #         fit_columns_on_grid_load=True,  # True면 전체넓이에서 균등분배 
    #         theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
    #         allow_unsafe_jscode=True
    #     )


    # 커스텀 리포트 파생지표 생성부터 ...
    def decorate_df(
        df: pd.DataFrame,
        pivot_cols: list[str],
        ) -> None:
        df2 = df.copy()
        
        # 자료형 워싱 1 (있으면 << 반드시)
        if 'event_date' in df2.columns:
            df2['event_date'] = pd.to_datetime(df2['event_date'], errors='coerce').dt.strftime('%Y-%m-%d')


        # 파생 지표 생성
        df2['CPC'] = ((df2['cost_gross_sum'] / df2['clicks_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(0))
        df2['CTR'] = ((df2['clicks_sum'] / df2['impressions_sum'] * 100).replace([np.inf, -np.inf], 0).fillna(0).round(2))
        df2['session_count_CPA'] = (df2['cost_gross_sum'] / df2['session_count']).replace([np.inf, -np.inf], 0).fillna(0).round(0)
        df2['view_item_CPA'] = (df2['cost_gross_sum'] / df2['view_item_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        df2['product_page_scroll_50_CPA'] = (df2['cost_gross_sum'] / df2['product_page_scroll_50_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        df2['product_option_price_CPA'] = (df2['cost_gross_sum'] / df2['product_option_price_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        df2['find_nearby_showroom_CPA'] = (df2['cost_gross_sum'] / df2['find_nearby_showroom_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        df2['showroom_10s_CPA'] = (df2['cost_gross_sum'] / df2['showroom_10s_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        df2['showroom_leads_CPA'] = (df2['cost_gross_sum'] / df2['showroom_leads_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        df2['add_to_cart_CPA'] = (df2['cost_gross_sum'] / df2['add_to_cart_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        df2['purchase_CPA'] = (df2['cost_gross_sum'] / df2['purchase_sum']).replace([np.inf, -np.inf], 0).fillna(0).round(2)
        
        
        # # 컬럼 순서 지정
        # df2 = df2[[        
        #     "period",
        #     "event_date",
        #     "cost_sum",
        #     "cost_gross_sum",
        #     "impressions_sum",
        #     "clicks_sum",
        #     "CPC",
        #     "CTR",
        #     "session_count",
        #     "session_count_CPA",
        #     "view_item_sum",
        #     "view_item_CPA",
        #     "product_page_scroll_50_sum",
        #     "product_page_scroll_50_CPA",
        #     "product_option_price_sum",
        #     "product_option_price_CPA",
        #     "find_nearby_showroom_sum",
        #     "find_nearby_showroom_CPA",
        #     "add_to_cart_sum",
        #     "add_to_cart_CPA",
        #     "showroom_10s_sum",
        #     "showroom_10s_CPA",
        #     "showroom_leads_sum",
        #     "showroom_leads_CPA",
        #     "purchase_sum",
        #     "purchase_CPA"
        # ]]
        # # 컬럼 이름 변경 - 멀티 인덱스
        # df2.columns = pd.MultiIndex.from_tuples([
        #     ("기본정보", "기간"), # period
        #     ("기본정보", "날짜"), # event_date
        #     ("MEDIA", "광고비"), # cost_sum
        #     ("MEDIA", "광고비(G)"), # cost_gross_sum
        #     ("MEDIA", "노출수"), # impressions_sum
        #     ("MEDIA", "클릭수"), # clicks_sum
        #     ("MEDIA", "CPC"), # CPC
        #     ("MEDIA", "CTR"), # CTR
        #     ("전체 세션수", "Actual"), # session_count
        #     ("전체 세션수", "CPA"),    # 
        #     ("PDP조회", "Actual"), # view_item_sum
        #     ("PDP조회", "CPA"),    # 
        #     ("PDPscr50", "Actual"), # product_page_scroll_50_sum
        #     ("PDPscr50", "CPA"),    # 
        #     ("가격표시", "Actual"), # product_option_price_sum
        #     ("가격표시", "CPA"),    # 
        #     ("쇼룸찾기", "Actual"), # find_nearby_showroom_sum
        #     ("쇼룸찾기", "CPA"),    # 
        #     ("장바구니", "Actual"), # add_to_cart_sum
        #     ("장바구니", "CPA"),    # 
        #     ("쇼룸10초", "Actual"), # showroom_10s_sum
        #     ("쇼룸10초", "CPA"),    # 
        #     ("쇼룸예약", "Actual"), # showroom_leads_sum
        #     ("쇼룸예약", "CPA"),    # 
        #     ("구매완료", "Actual"), # purchase_sum
        #     ("구매완료", "CPA"),    # 
        # ], names=["그룹","지표"])  # 상단 레벨 이름(옵션)        
        

        # 자료형 워싱 2 
        num_cols = df2.select_dtypes(include=['number']).columns
        df2[num_cols] = (df2[num_cols].replace([np.inf, -np.inf], np.nan).fillna(0))
        
        # 컬럼 순서 지정 - 기본정보 + pivot_cols + 지표들
        base_info = ['period', 'event_date']
        # df에 실제 존재하는 pivot_cols만 반영(중복·미존재 방지)
        pivot_extra = [c for c in pivot_cols if c not in base_info and c in df2.columns]

        metric_cols = [
            "cost_sum","cost_gross_sum","impressions_sum","clicks_sum","CPC","CTR",
            "session_count","session_count_CPA",
            "view_item_sum","view_item_CPA",
            "product_page_scroll_50_sum","product_page_scroll_50_CPA",
            "product_option_price_sum","product_option_price_CPA",
            "find_nearby_showroom_sum","find_nearby_showroom_CPA",
            "add_to_cart_sum","add_to_cart_CPA",
            "showroom_10s_sum","showroom_10s_CPA",
            "showroom_leads_sum","showroom_leads_CPA",
            "purchase_sum","purchase_CPA"
        ]

        # df에 존재하는 컬럼만 선택(안전)
        ordered_cols = [c for c in base_info + pivot_extra + metric_cols if c in df2.columns]
        df2 = df2[ordered_cols]

        # ─────────────────────────
        # 컬럼 이름 변경 - 멀티 인덱스
        # ─────────────────────────
        # 기본정보 맵(존재하는 경우만)
        basic_map = []
        if 'period' in df2.columns:
            basic_map.append(("기본정보", "기간"))
        if 'event_date' in df2.columns:
            basic_map.append(("기본정보", "날짜"))

        # pivot_cols → 전부 "기본정보" 그룹으로
        pivot_map = [( "기본정보", col ) for col in pivot_extra]

        metrics_map_dict = {
            "cost_sum": ("MEDIA","광고비"),
            "cost_gross_sum": ("MEDIA","광고비(G)"),
            "impressions_sum": ("MEDIA","노출수"),
            "clicks_sum": ("MEDIA","클릭수"),
            "CPC": ("MEDIA","CPC"),
            "CTR": ("MEDIA","CTR"),
            "session_count": ("전체 세션수","Actual"),
            "session_count_CPA": ("전체 세션수","CPA"),
            "view_item_sum": ("PDP조회","Actual"),
            "view_item_CPA": ("PDP조회","CPA"),
            "product_page_scroll_50_sum": ("PDPscr50","Actual"),
            "product_page_scroll_50_CPA": ("PDPscr50","CPA"),
            "product_option_price_sum": ("가격표시","Actual"),
            "product_option_price_CPA": ("가격표시","CPA"),
            "find_nearby_showroom_sum": ("쇼룸찾기","Actual"),
            "find_nearby_showroom_CPA": ("쇼룸찾기","CPA"),
            "add_to_cart_sum": ("장바구니","Actual"),
            "add_to_cart_CPA": ("장바구니","CPA"),
            "showroom_10s_sum": ("쇼룸10초","Actual"),
            "showroom_10s_CPA": ("쇼룸10초","CPA"),
            "showroom_leads_sum": ("쇼룸예약","Actual"),
            "showroom_leads_CPA": ("쇼룸예약","CPA"),
            "purchase_sum": ("구매완료","Actual"),
            "purchase_CPA": ("구매완료","CPA"),
        }

        metrics_map = [metrics_map_dict[c] for c in ordered_cols if c in metrics_map_dict]

        # 최종 멀티인덱스(ordered_cols 순서에 맞춰 생성)
        multi_labels: list[tuple[str,str]] = []
        for c in ordered_cols:
            if c in ['period','event_date']:
                multi_labels.append(("기본정보","기간" if c=='period' else "날짜"))
            elif c in pivot_extra:
                multi_labels.append(("기본정보", c))
            else:
                multi_labels.append(metrics_map_dict.get(c, ("기본정보", c)))

        df2.columns = pd.MultiIndex.from_tuples(multi_labels, names=["그룹","지표"])

        return df2


    def render_style(target_df, pivot_cols):
        styled = style_format(
            decorate_df(target_df, pivot_cols),
            decimals_map={
                ("MEDIA", "광고비"): 0,
                ("MEDIA", "광고비(G)"): 0,
                ("MEDIA", "노출수"): 0,
                ("MEDIA", "클릭수"): 0, # clicks_sum
                ("MEDIA", "CPC"): 0, # CPC
                ("MEDIA", "CTR"): 2, # CTR
                ("전체 세션수", "Actual"): 0, # session_count
                ("전체 세션수", "CPA"): 0,    # 
                ("PDP조회", "Actual"): 0, # view_item_sum
                ("PDP조회", "CPA"): 0,    # 
                ("PDPscr50", "Actual"): 0, # product_page_scroll_50_sum
                ("PDPscr50", "CPA"): 0,    # 
                ("가격표시", "Actual"): 0, # product_option_price_sum
                ("가격표시", "CPA"): 0,    # 
                ("쇼룸찾기", "Actual"): 0, # find_nearby_showroom_sum
                ("쇼룸찾기", "CPA"): 0,    # 
                ("장바구니", "Actual"): 0, # add_to_cart_sum
                ("장바구니", "CPA"): 0,    # 
                ("쇼룸10초", "Actual"): 0, # showroom_10s_sum
                ("쇼룸10초", "CPA"): 0,    # 
                ("쇼룸예약", "Actual"): 0, # showroom_leads_sum
                ("쇼룸예약", "CPA"): 0,    # 
                ("구매완료", "Actual"): 0, # purchase_sum
                ("구매완료", "CPA"): 0,    # 
            },
            suffix_map={
                ("MEDIA", "CTR"): " %",
        }
        )
        styled2 = style_cmap(
            styled,
            gradient_rules=[
                {"col": ("전체 세션수", "Actual"), "cmap":"OrRd", "vmax":18000, "low":0.0, "high":0.3},
                {"col": ("PDP조회", "Actual"), "cmap":"OrRd", "vmax":18000, "low":0.0, "high":0.3},
                {"col": ("PDPscr50", "Actual"), "cmap":"OrRd", "vmax":18000, "low":0.0, "high":0.3},
                {"col": ("가격표시", "Actual"), "cmap":"OrRd", "vmax":18000, "low":0.0, "high":0.3},
                {"col": ("쇼룸찾기", "Actual"), "cmap":"OrRd", "vmax":18000, "low":0.0, "high":0.3},
                {"col": ("장바구니", "Actual"), "cmap":"OrRd", "vmax":18000, "low":0.0, "high":0.3},
                {"col": ("쇼룸10초", "Actual"), "cmap":"OrRd", "vmax":18000, "low":0.0, "high":0.3},
                {"col": ("쇼룸예약", "Actual"), "cmap":"OrRd", "vmax":18000, "low":0.0, "high":0.3},
                {"col": ("구매완료", "Actual"), "cmap":"OrRd", "vmax":18000, "low":0.0, "high":0.3},
            ],
        )
        st.dataframe(styled2, use_container_width=True, height=460, hide_index=True)

    # 공통 필터 함수: 멀티셀렉트 vs 텍스트 입력
    def apply_filter_pair(
        df: pd.DataFrame,
        df_cmp: pd.DataFrame | None,
        column: str,
        text_filter: bool = False
    ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """
        df: 주 데이터
        df_cmp: 비교 데이터 (use_compare=False면 None)
        column: 필터 대상 컬럼명
        text_filter: False면 multiselect, True면 text_input
        """
        key = f"{column}_{'text' if text_filter else 'multi'}"
        if text_filter:
            # term = st.text_input(f"{column} 포함 필터", key=key)
            term = st.text_input(f"{header_map.get(column,column)} 포함 검색", key=key)
            if term:
                df = df[df[column].str.contains(term, na=False)]
                if df_cmp is not None:
                    df_cmp = df_cmp[df_cmp[column].str.contains(term, na=False)]
        else:
            opts = sorted(df_primary[column].dropna().unique())
            # sel  = st.multiselect(f"{column} 필터", opts, key=key)
            sel  = st.multiselect(f"{header_map.get(column,column)} 필터", opts, key=key)
            if sel:
                df = df[df[column].isin(sel)]
                if df_cmp is not None:
                    df_cmp = df_cmp[df_cmp[column].isin(sel)]
        return df, df_cmp


    # ──────────────────────────────────
    # 1) 커스텀 리포트
    # ──────────────────────────────────
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)
        
    st.markdown("<h5 style='margin:0'> <span style='color:#FF4B4B;'> 커스텀 </span>리포트</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ필터와 비교기간 기능을 활용하여, **광고 성과부터 GA 액션별 전환 효율까지** 원하는 기준의 데이터를 확인할 수 있습니다.")
    st.markdown(" ")


    # 피벗할 행 필드 선택
    # pivot_cols = st.multiselect(
    #     "행 필드 선택",
    #     [   "event_date", 
    #         "media_name", "utm_source", "utm_medium", 
    #         "brand_type", "funnel_type", "product_type"
    #         "campaign_name", "adgroup_name", "ad_name", "keyword_name",
    #         "utm_content", "utm_term"
    #     ],
    #     default=["event_date"]
    #     )

    pivot_cols = st.multiselect(
        "행 필드 선택",
        options=list(header_map.keys()),
        default=["event_date"],
        format_func=lambda x: header_map.get(x, x)   # 한글로 표시
    )

    # 기간별 합계 보기 모드라면 event_date 는 무시
    if show_totals and "event_date" in pivot_cols:
        pivot_cols.remove("event_date")
        
    # 공통 서치필터 및 상세 서치필터 정렬
    with st.expander("기본 멀티셀렉 필터", expanded=False):
        ft1, ft2, ft3, ft4, ft5, ft6 = st.columns(6)
        with ft1:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "media_name", text_filter=False)
        with ft2:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "utm_source", text_filter=False)
        with ft3:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "utm_medium", text_filter=False)
        with ft4:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "brand_type", text_filter=False)
        with ft5:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "funnel_type", text_filter=False)
        with ft6:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "product_type", text_filter=False)
    
    with st.expander("고급 멀티셀렉 필터", expanded=False):
        ft7, ft8, ft9, ft10 = st.columns([2,1,2,1])
        with ft7:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "campaign_name", text_filter=False)
        with ft8:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "campaign_name", text_filter=True)
        with ft9:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "adgroup_name", text_filter=False)
        with ft10:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "adgroup_name", text_filter=True)
        
        ft11, ft12, ft13, ft14 = st.columns([2,1,2,1])
        with ft11:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "ad_name", text_filter=False)
        with ft12:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "ad_name", text_filter=True)
        with ft13:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "keyword_name", text_filter=False)
        with ft14:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "keyword_name", text_filter=True)

        ft15, ft16, ft17, ft18 = st.columns([2,1,2,1])
        with ft15:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "utm_content", text_filter=False)
        with ft16:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "utm_content", text_filter=True)
        with ft17:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "utm_term", text_filter=False)
        with ft18:
            df_filtered, df_filtered_cmp = apply_filter_pair(df_filtered, df_filtered_cmp, "utm_term", text_filter=True)

    # 표 표시 영역
    if pivot_cols or show_totals:

        # (1) 기간별 합계 보기 모드
        if show_totals:
            # (A) period 태깅
            df_sel = df_filtered.copy()
            # df_sel["period"] = "선택기간"
            df_sel["period"] = f"{start_date_str} ~ {end_date_str}"
            if use_compare:
                df_cmp = df_filtered_cmp.copy()
                # df_cmp["period"] = "비교기간"
                df_cmp["period"] = f"{default_comp_s_str} ~ {default_comp_e_str}"
                df_combined = pd.concat([df_sel, df_cmp], ignore_index=True)
            else:
                df_combined = df_sel

            # (B) event_date 는 이미 pivot_cols 에서 제거되어 있다고 가정
            group_keys = ["period"] + pivot_cols

            # C) Named aggregation 으로 정확한 컬럼명 생성
            df_pivot = (
                df_combined
                .groupby(group_keys, as_index=False)
                .agg(
                    cost_sum                     = ("cost",                       "sum"),
                    cost_gross_sum               = ("cost_gross",                 "sum"),
                    impressions_sum              = ("impressions",                "sum"),
                    clicks_sum                   = ("clicks",                     "sum"),
                    view_item_sum                = ("view_item",                  "sum"),
                    product_page_scroll_50_sum   = ("product_page_scroll_50",     "sum"),
                    product_option_price_sum     = ("product_option_price",       "sum"),
                    find_nearby_showroom_sum     = ("find_nearby_showroom",       "sum"),
                    showroom_10s_sum             = ("showroom_10s",               "sum"),
                    add_to_cart_sum              = ("add_to_cart",                "sum"),
                    showroom_leads_sum           = ("showroom_leads",             "sum"),
                    purchase_sum                 = ("purchase",                   "sum"),
                    session_count                = ("session_start",              "sum"),
                    engagement_time_msec_sum     = ("engagement_time_msec_sum",   "sum"),
                )
            )

            # render_aggrid(df_pivot, group_keys)
            # st.dataframe(
            #     decorate_df(df_pivot, group_keys)
            # )
            render_style(df_pivot, group_keys)

        # (2) 일반 Pivot 모드
        else:
            # …기존 Pivot & 통합 테이블 로직…
            df_sel = (
                df_filtered
                .groupby(pivot_cols, as_index=False)
                .agg(
                    cost_sum                     = ("cost",                       "sum"),
                    cost_gross_sum               = ("cost_gross",                 "sum"),
                    impressions_sum              = ("impressions",                "sum"),
                    clicks_sum                   = ("clicks",                     "sum"),
                    view_item_sum                = ("view_item",                  "sum"),
                    product_page_scroll_50_sum   = ("product_page_scroll_50",     "sum"),
                    product_option_price_sum     = ("product_option_price",       "sum"),
                    find_nearby_showroom_sum     = ("find_nearby_showroom",       "sum"),
                    showroom_10s_sum             = ("showroom_10s",               "sum"),
                    add_to_cart_sum              = ("add_to_cart",                "sum"),
                    showroom_leads_sum           = ("showroom_leads",             "sum"),
                    purchase_sum                 = ("purchase",                  "sum"),
                    session_count                = ("session_start",             "sum"),
                    engagement_time_msec_sum     = ("engagement_time_msec_sum",   "sum")
                )
            )
            # df_sel["period"] = "선택기간"
            df_sel["period"] = f"{start_date_str} ~ {end_date_str}"
            if use_compare:
                df_cmp = (
                    df_filtered_cmp
                    .groupby(pivot_cols, as_index=False)
                    .agg(
                        cost_sum                     = ("cost",                       "sum"),
                        cost_gross_sum               = ("cost_gross",                 "sum"),
                        impressions_sum              = ("impressions",                "sum"),
                        clicks_sum                   = ("clicks",                     "sum"),
                        view_item_sum                = ("view_item",                  "sum"),
                        product_page_scroll_50_sum   = ("product_page_scroll_50",     "sum"),
                        product_option_price_sum     = ("product_option_price",       "sum"),
                        find_nearby_showroom_sum     = ("find_nearby_showroom",       "sum"),
                        showroom_10s_sum             = ("showroom_10s",               "sum"),
                        add_to_cart_sum              = ("add_to_cart",                "sum"),
                        showroom_leads_sum           = ("showroom_leads",             "sum"),
                        purchase_sum                 = ("purchase",                  "sum"),
                        session_count                = ("session_start",             "sum"),
                        engagement_time_msec_sum     = ("engagement_time_msec_sum",   "sum")
                    )
                )
                # df_cmp["period"] = "비교기간"
                df_cmp["period"] = f"{default_comp_s_str} ~ {default_comp_e_str}"
                df_pivot = pd.concat([df_sel, df_cmp], ignore_index=True)
            else:
                df_pivot = df_sel

            # render_aggrid(df_pivot, ["period"] + pivot_cols)
            # st.dataframe(
            #     decorate_df(df_pivot, ["period"] + pivot_cols)
            # )
            render_style(df_pivot, ["period"] + pivot_cols)
            
    else:
        st.warning("피벗할 행 필드를 하나 이상 선택해 주세요.")




    # ──────────────────────────────────
    # 2. 광고비, 노출수, 클릭수, CTR, CPC
    # ──────────────────────────────────

    df3 = df_filtered.copy()
    
    # 자료형 워싱
    df3['event_date'] = pd.to_datetime(df3['event_date'], errors='coerce').dt.strftime('%Y-%m-%d')


    def pivot_ctr(
        df: pd.DataFrame, 
        group_col: str = None,
        value_map: dict = None
    ) -> pd.DataFrame:
        """
        일자별(또는 그룹별을 추가하여) 피벗 테이블 생성.
        group_col: None이면 전체 일자별, 아니면 그룹별 피벗 
        value_map: {"광고비": "cost_gross", ...} 커스텀 가능
        """
        if value_map is None:
            value_map = {
                "광고비(G)": "cost_gross",
                "노출수"   : "impressions",
                "클릭수"   : "clicks"
            }
        if group_col:
            agg_dict = {k: (v, "sum") for k, v in value_map.items()}
            by_grp = (
                df.groupby(["event_date", group_col], as_index=False)
                .agg(**agg_dict)
            )
            by_grp["CPC"] = 0.0
            by_grp["CTR"] = 0.0
            mask_impr = by_grp["노출수"] > 0
            mask_click = by_grp["클릭수"] > 0
            by_grp.loc[mask_impr, "CTR"] = (by_grp.loc[mask_impr, "클릭수"] / by_grp.loc[mask_impr, "노출수"] * 100).round(2)
            by_grp.loc[mask_click, "CPC"] = (by_grp.loc[mask_click, "광고비(G)"] / by_grp.loc[mask_click, "클릭수"]).round(0)
            all_keys = list(value_map.keys()) + ["CPC", "CTR"]
            pivot = by_grp.pivot(index="event_date", columns=group_col, values=all_keys)
            pivot.columns = [f"{g}_{k}" for k, g in pivot.columns]
            pivot = pivot.reset_index()
            return pivot
        else:
            agg_dict = {k: (v, "sum") for k, v in value_map.items()}
            df_total = (
                df.groupby("event_date", as_index=False)
                .agg(**agg_dict)
            )
            df_total["CPC"] = 0.0
            df_total["CTR"] = 0.0
            mask_impr = df_total["노출수"] > 0
            mask_click = df_total["클릭수"] > 0
            df_total.loc[mask_impr, "CTR"] = (df_total.loc[mask_impr, "클릭수"] / df_total.loc[mask_impr, "노출수"] * 100).round(2)
            df_total.loc[mask_click, "CPC"] = (df_total.loc[mask_click, "광고비(G)"] / df_total.loc[mask_click, "클릭수"]).round(0)
            return df_total

    def render_ctr_charts(df: pd.DataFrame, date_col: str = "event_date", key_prefix: str = ""):
        c1, c2, c3 = st.columns(3)
        df_plot = df.copy()
        df_plot[date_col] = pd.to_datetime(df_plot[date_col])

        with c1:
            fig1 = go.Figure()
            y1 = df_plot.columns[df_plot.columns.str.contains("광고비")]
            y2 = df_plot.columns[df_plot.columns.str.contains("노출수")]
            fig1.add_trace(go.Bar(
                x=df_plot[date_col], y=df_plot[y1[0]], name=y1[0], yaxis="y1", opacity=0.6
            ))
            fig1.add_trace(go.Scatter(
                x=df_plot[date_col], y=df_plot[y2[0]], name=y2[0], yaxis="y2", mode="lines+markers"
            ))
            fig1.update_layout(
                title="광고비 대비 노출수",
                xaxis=dict(title="", tickformat="%m월 %d일"),
                yaxis_title=y1[0],
                yaxis2=dict(title=y2[0], overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=400
            )
            st.plotly_chart(fig1, use_container_width=True, key=f"{key_prefix}_fig1")

        with c2:
            fig2 = go.Figure()
            y1 = df_plot.columns[df_plot.columns.str.contains("노출수")]
            y2 = df_plot.columns[df_plot.columns.str.contains("클릭수")]
            fig2.add_trace(go.Bar(
                x=df_plot[date_col], y=df_plot[y1[0]], name=y1[0], yaxis="y1", opacity=0.6
            ))
            fig2.add_trace(go.Scatter(
                x=df_plot[date_col], y=df_plot[y2[0]], name=y2[0], yaxis="y2", mode="lines+markers"
            ))
            fig2.update_layout(
                title="노출수 대비 클릭수",
                xaxis=dict(title="", tickformat="%m월 %d일"),
                yaxis_title=y1[0],
                yaxis2=dict(title=y2[0], overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=400
            )
            st.plotly_chart(fig2, use_container_width=True, key=f"{key_prefix}_fig2")

        with c3:
            fig3 = go.Figure()
            y1 = df_plot.columns[df_plot.columns.str.contains("CTR")]
            y2 = df_plot.columns[df_plot.columns.str.contains("CPC")]
            if len(y1) and len(y2):
                fig3.add_trace(go.Scatter(
                    x=df_plot[date_col], y=df_plot[y1[0]], name=y1[0], mode="lines+markers", yaxis="y1"
                ))
                fig3.add_trace(go.Scatter(
                    x=df_plot[date_col], y=df_plot[y2[0]], name=y2[0], mode="lines+markers", yaxis="y2"
                ))
            fig3.update_layout(
                title="CTR 및 CPC 추이",
                xaxis=dict(title="", tickformat="%m월 %d일"),
                yaxis=dict(title="CTR"),
                yaxis2=dict(title="CPC", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=400
            )
            st.plotly_chart(fig3, use_container_width=True, key=f"{key_prefix}_fig3")


    def render_CTR_style(target_df):
        def to_name(col):
            return " ".join(map(str, col)) if isinstance(col, tuple) else str(col)

        # 포함 조건으로 컬럼 집합 추출
        cols_광고비 = [c for c in target_df.columns if "광고비" in to_name(c)]
        cols_노출수 = [c for c in target_df.columns if "노출수" in to_name(c)]
        cols_클릭수 = [c for c in target_df.columns if "클릭수" in to_name(c)]
        cols_cpc = [c for c in target_df.columns if "CPC" in to_name(c)]
        cols_ctr = [c for c in target_df.columns if "CTR" in to_name(c)]

        # 소수점 자리수 매핑
        decimals_map = {}
        # 광고비* → 0자리
        decimals_map.update({c: 0 for c in cols_광고비})
        decimals_map.update({c: 0 for c in cols_노출수})
        decimals_map.update({c: 0 for c in cols_클릭수})
        decimals_map.update({c: 0 for c in cols_cpc})
        # CTR* → 2자리
        decimals_map.update({c: 2 for c in cols_ctr})
        # 접미사 매핑: CTR* → " %"
        suffix_map = {c: " %" for c in cols_ctr}

        styled = style_format(
            target_df,
            decimals_map=decimals_map,
            suffix_map=suffix_map
        )
        st.dataframe(styled, use_container_width=True, height=400, hide_index=True)



    # ────────────────────────────────────────────────────────────────
    # 고정뷰 리포트
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'> <span style='color:#FF4B4B;'> 고정뷰 </span>리포트</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ매체, 브랜드, 품목, 퍼널별로 **노출과 클릭 효율**을 집중적으로 확인할 수 있습니다. ")

    pivot_total = pivot_ctr(df3, group_col=None)

    tabs = st.tabs(["일자별", "매체별", "브랜드별", "품목별", "퍼널별"])

    with tabs[0]:
        render_CTR_style(pivot_total)
        render_ctr_charts(pivot_total, key_prefix="total")

    with tabs[1]:
        media_values = df3["media_name"].dropna().unique()
        media_sel = st.selectbox("매체 선택", ["(전체)"] + list(media_values), key="media_tab_select")
        if media_sel == "(전체)" or media_sel is None:
            pivot_media = pivot_ctr(df3, group_col="media_name")
            render_CTR_style(pivot_media)
            render_ctr_charts(pivot_media, key_prefix="media")
        else:
            df3_media = df3[df3["media_name"] == media_sel]
            pivot_media = pivot_ctr(df3_media, group_col="media_name")
            render_CTR_style(pivot_media)
            render_ctr_charts(pivot_media, key_prefix="media")

    with tabs[2]:
        brand_values = df3["brand_type"].dropna().unique()
        brand_sel = st.selectbox("브랜드 선택", ["(전체)"] + list(brand_values), key="brand_tab_select")
        if brand_sel == "(전체)" or brand_sel is None:
            pivot_brand = pivot_ctr(df3, group_col="brand_type")
            render_CTR_style(pivot_brand)
            render_ctr_charts(pivot_brand, key_prefix="brand")
        else:
            df3_brand = df3[df3["brand_type"] == brand_sel]
            pivot_brand = pivot_ctr(df3_brand, group_col="brand_type")
            render_CTR_style(pivot_brand)
            render_ctr_charts(pivot_brand, key_prefix="brand")

    with tabs[3]:
        prod_values = df3["product_type"].dropna().unique()
        prod_sel = st.selectbox("품목 선택", ["(전체)"] + list(prod_values), key="prod_tab_select")
        if prod_sel == "(전체)" or prod_sel is None:
            pivot_product = pivot_ctr(df3, group_col="product_type")
            render_CTR_style(pivot_product)
            render_ctr_charts(pivot_product, key_prefix="product")
        else:
            df3_prod = df3[df3["product_type"] == prod_sel]
            pivot_product = pivot_ctr(df3_prod, group_col="product_type")
            render_CTR_style(pivot_product)
            render_ctr_charts(pivot_product, key_prefix="product")

    with tabs[4]:
        funnel_values = df3["funnel_type"].dropna().unique()
        funnel_sel = st.selectbox("퍼널 선택", ["(전체)"] + list(funnel_values), key="funnel_tab_select")
        if funnel_sel == "(전체)" or funnel_sel is None:
            pivot_funnel = pivot_ctr(df3, group_col="funnel_type")
            render_CTR_style(pivot_funnel)
            render_ctr_charts(pivot_funnel, key_prefix="funnel")
        else:
            df3_funnel = df3[df3["funnel_type"] == funnel_sel]
            pivot_funnel = pivot_ctr(df3_funnel, group_col="funnel_type")
            render_CTR_style(pivot_funnel)
            render_ctr_charts(pivot_funnel, key_prefix="funnel")


