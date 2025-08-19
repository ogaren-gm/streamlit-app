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
        .reportview-container .main .block-container {
            max-width: 100% !important;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.subheader('액션 종합 대시보드')
    st.markdown("""
    이 대시보드는 방문 > 상품조회 > 쇼룸찾기 > 구매 등 **자사몰의 주요 액션 흐름**을 단계적으로 보여주는 **행동 퍼널 대시보드**입니다.  
    여기서는 광고비 및 세션수와 더불어 “**각 단계까지 몇 명이 행동했고, 그때의 비용/전환 효율은 어땠는지**”를 확인할 수 있습니다.
    """)
    st.markdown(
        '<a href="https://www.notion.so/Views-241521e07c7680df86eecf5c5f8da4af#241521e07c7680d299bee34d617a1626" target="_blank">'
        '지표설명 & 가이드</a>',
        unsafe_allow_html=True
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
            (merged['media_name']=='NSA') &
            merged['utm_source'].isna() & merged['utm_medium'].isna() &
            merged['media_name_type'].isin(['RSA_AD','TEXT_45'])
        )
        merged.loc[cond, ['utm_source','utm_medium']] = ['naver','search-nonmatch']
        
        # 3) tb_sleeper_product_report (tb_sleeper_psi를 이미 가공한 빅쿼리 테이블)
        df_prod_rep = bq.get_data("tb_sleeper_product_report")
        df_prod_rep["event_date"] = pd.to_datetime(df_prod_rep["event_date"], format="%Y%m%d")

        # 4) tb_sleeper_psi, 넓게 이벤트까지 피벗해옴    
        df_psi = bq.get_data("tb_sleeper_psi")
        df_psi["event_date"] = pd.to_datetime(df_psi["event_date"], format="%Y%m%d")
        df_psi = (
            df_psi
            .groupby("event_date", as_index=False)
            .agg(
                session_count            = ("pseudo_session_id",       "nunique"),
                view_item                = ("view_item",               "sum"),
                product_page_scroll_50   = ("product_page_scroll_50",  "sum"),
                product_option_price     = ("product_option_price",    "sum"),
                find_nearby_showroom     = ("find_nearby_showroom",    "sum"),
                showroom_10s             = ("showroom_10s",            "sum"),
                add_to_cart              = ("add_to_cart",             "sum"),
                showroom_leads           = ("showroom_leads",          "sum"),
                purchase                 = ("purchase",                "sum"),
            )
            .sort_values("event_date")
        )
        df_psi['event_date'] = pd.to_datetime(df_psi['event_date'], errors='coerce')
        df_psi['event_date'] = df_psi['event_date'].dt.strftime('%Y-%m-%d')

    
        return merged, df_prod_rep, df_psi
    
    # ────────────────────────────────────────────────────────────────
    # 데이터 불러오기
    # ────────────────────────────────────────────────────────────────
    st.toast("GA D-1 데이터는 오전에 예비 처리되고, **15시 이후에 최종 업데이트** 됩니다.", icon="🔔")
    with st.spinner("데이터가 많아 로딩에 조금 시간이 소요됩니다. 조금만 기다려 주세요 😊"):
        df_merged, df_prodRep, df_psi = load_data(cs, ce)

    # 공통합수 (1) 일자별 광고비, 세션수 (파생변수는 해당 함수가 계산하지 않음)
    def pivot_cstSes(
        df: pd.DataFrame,
        brand_type: str | None = None,
        product_type: str | None = None
        ) -> pd.DataFrame:
        """
        1) 함수 작성
        :  pivot_cstSes(df, brand_type="슬립퍼", product_type="매트리스")
        2) 결과 컬럼
        :  ['event_date', 'session_count', 'cost_gross_sum']
        """
        df_f = df.copy()

        if brand_type:
            df_f = df_f[df_f['brand_type'].astype(str).str.contains(brand_type, regex=True, na=False)]
        if product_type:
            df_f = df_f[df_f['product_type'].astype(str).str.contains(product_type, regex=True, na=False)]

        df_f['event_date'] = pd.to_datetime(df_f['event_date'], errors='coerce')
        df_f['event_date'] = df_f['event_date'].dt.strftime('%Y-%m-%d')

        pivot = (
            df_f
            .groupby('event_date', as_index=False) # 반드시 False로 유지 (그래야 컬럼에 살아있음)
            .agg(
                session_count=('pseudo_session_id', 'sum'),
                cost_gross_sum=('cost_gross', 'sum')
            )
            .reset_index(drop=True)
        )
        return pivot

    # 공통함수 (2) 일자별 이벤트수 (파생변수는 해당 함수가 계산하지 않음)
    def pivot_prdRep(
        df: pd.DataFrame,
        brand_type: str | None = None,
        product_type: str | None = None,
        is_paid: str | None = None
        ) -> pd.DataFrame:
        """
        1) 함수 작성
        :  pivot_prdRep(df, brand_type="슬립퍼", product_type="매트리스", is_paid="y")
        2) 결과 컬럼
        :  ['event_date', 'session_start', 'view_item', ..., 'purchase']
        """
        df_f = df.copy()
        
        if is_paid is not None:
            df_f = df_f[df_f['is_paid'].astype(str) == is_paid]
        if brand_type:
            df_f = df_f[df_f['product_cat_a'].astype(str).str.contains(brand_type, regex=True, na=False)]
        if product_type:
            df_f = df_f[df_f['product_cat_b'].astype(str).str.contains(product_type, regex=True, na=False)]
        
        df_f['event_date'] = pd.to_datetime(df_f['event_date'], errors='coerce')
        df_f['event_date'] = df_f['event_date'].dt.strftime('%Y-%m-%d')

        pivot = (
            df_f
            .groupby(['event_date', 'event_name'])['pseudo_session_id']
            .nunique()
            .unstack(fill_value=0)
            .reset_index()
        )
        return pivot


    # ────────────────────────────────────────────────────────────────
    # 데이터프레임 생성 
    # ────────────────────────────────────────────────────────────────
    # 1-1) 슬립퍼
    _sctSes_slp        = pivot_cstSes(df_merged,  brand_type="슬립퍼")
    _prdRep_slp        = pivot_prdRep(df_prodRep, brand_type="슬립퍼")
    df_slp             = _sctSes_slp.merge(_prdRep_slp, on='event_date', how='left')
    
    # 1-2) 슬립퍼 & PAID
    _sctSes_slp_y        = pivot_cstSes(df_merged,  brand_type="슬립퍼")
    _prdRep_slp_y        = pivot_prdRep(df_prodRep, brand_type="슬립퍼", is_paid="y")
    df_slp_y             = _sctSes_slp_y.merge(_prdRep_slp_y, on='event_date', how='left')
    
    # 1-3) 슬립퍼 & 매트리스
    _sctSes_slp_mat        = pivot_cstSes(df_merged,  brand_type="슬립퍼", product_type="매트리스")
    _prdRep_slp_mat        = pivot_prdRep(df_prodRep, brand_type="슬립퍼", product_type="매트리스")
    df_slp_mat             = _sctSes_slp_mat.merge(_prdRep_slp_mat, on='event_date', how='left')
    
    # 1-4) 슬립퍼 & 매트리스 & PAID
    _sctSes_slp_mat_y        = pivot_cstSes(df_merged,  brand_type="슬립퍼", product_type="매트리스")
    _prdRep_slp_mat_y        = pivot_prdRep(df_prodRep, brand_type="슬립퍼", product_type="매트리스", is_paid="y")
    df_slp_mat_y             = _sctSes_slp_mat_y.merge(_prdRep_slp_mat_y, on='event_date', how='left')
    
    # 1-5) 슬립퍼 & 프레임 - 매우 주의, Regex 사용 필수 
    _sctSes_slp_frm        = pivot_cstSes(df_merged,  brand_type="슬립퍼", product_type="원목 침대|패브릭 침대|프레임")
    _prdRep_slp_frm        = pivot_prdRep(df_prodRep, brand_type="슬립퍼", product_type="원목 침대|패브릭 침대|프레임")
    df_slp_frm             = _sctSes_slp_frm.merge(_prdRep_slp_frm, on='event_date', how='left')
    
    # 1-6) 슬립퍼 & 프레임 & PAID - 매우 주의, Regex 사용 필수 
    _sctSes_slp_frm_y        = pivot_cstSes(df_merged,  brand_type="슬립퍼", product_type="원목 침대|패브릭 침대|프레임")
    _prdRep_slp_frm_y        = pivot_prdRep(df_prodRep, brand_type="슬립퍼", product_type="원목 침대|패브릭 침대|프레임", is_paid="y")
    df_slp_frm_y             = _sctSes_slp_frm_y.merge(_prdRep_slp_frm_y, on='event_date', how='left')
    
    # 2-1) 누어
    _sctSes_nor        = pivot_cstSes(df_merged,  brand_type="누어")
    _prdRep_nor        = pivot_prdRep(df_prodRep, brand_type="누어")
    df_nor             = _sctSes_nor.merge(_prdRep_nor, on='event_date', how='left')
    
    # 2-2) 누어 & 매트리스
    _sctSes_nor_mat        = pivot_cstSes(df_merged,  brand_type="누어", product_type="매트리스")
    _prdRep_nor_mat        = pivot_prdRep(df_prodRep, brand_type="누어", product_type="매트리스")
    df_nor_mat             = _sctSes_nor_mat.merge(_prdRep_nor_mat, on='event_date', how='left')
    
    # 2-3) 누어 & 프레임
    _sctSes_nor_frm        = pivot_cstSes(df_merged,  brand_type="누어", product_type="원목 침대|패브릭 침대|프레임")
    _prdRep_nor_frm        = pivot_prdRep(df_prodRep, brand_type="누어", product_type="원목 침대|패브릭 침대|프레임")
    df_nor_frm             = _sctSes_nor_frm.merge(_prdRep_nor_frm, on='event_date', how='left')
    
    # 3) 통합 데이터 (3번 이지만, 위치상 최상위에 위치함 주의)
    _df_psi_total        = df_psi  # 이미 날짜별로 세션수와 이벤트수가 피벗되어 있는 데이터프레임
    _sctSes_total        = pivot_cstSes(df_merged)
    _sctSes_total        = _sctSes_total[['event_date', 'cost_gross_sum']] # cost_gross_sum 만
    df_total             = _df_psi_total.merge(_sctSes_total, on='event_date', how='left')


    # 모든 데이터프레임이 동일한 파생 지표를 가짐
    def decorate_df(df: pd.DataFrame) -> pd.DataFrame:
        # 키에러 방지
        required = ['event_date','session_count','view_item','product_page_scroll_50','product_option_price','find_nearby_showroom','showroom_10s','add_to_cart','showroom_leads','purchase']
        for c in required:
            if c not in df.columns:
                df[c] = 0  
        num_cols = ['session_count','view_item','product_page_scroll_50','product_option_price','find_nearby_showroom','showroom_10s','add_to_cart','showroom_leads','purchase']
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
            
        # 파생지표 생성 - CPA
        df['session_count_CPA']               = (df['cost_gross_sum']               / df['session_count']             ).round(0)
        df['view_item_CPA']                   = (df['cost_gross_sum']               / df['view_item']                 ).round(0)
        df['product_page_scroll_50_CPA']      = (df['cost_gross_sum']                   / df['product_page_scroll_50']).round(0)
        df['product_option_price_CPA']        = (df['cost_gross_sum']                   / df['product_option_price']  ).round(0)
        df['find_nearby_showroom_CPA']        = (df['cost_gross_sum']                   / df['find_nearby_showroom']  ).round(0)
        df['showroom_10s_CPA']                = (df['cost_gross_sum']                   / df['showroom_10s']          ).round(0)
        df['add_to_cart_CPA']                 = (df['cost_gross_sum']                   / df['add_to_cart']           ).round(0)
        df['showroom_leads_CPA']              = (df['cost_gross_sum']                   / df['showroom_leads']        ).round(0)
        df['purchase_CPA']                    = (df['cost_gross_sum']                   / df['purchase']              ).round(0)
        # 파생지표 생성 - CVR
        df['session_count_CVR']          = (df['session_count']               / df['session_count']              * 100).round(2)
        df['view_item_CVR']              = (df['view_item']                   / df['session_count']              * 100).round(2)
        df['product_page_scroll_50_CVR'] = (df['product_page_scroll_50']      / df['view_item']                  * 100).round(2)
        df['product_option_price_CVR']   = (df['product_option_price']        / df['view_item']                  * 100).round(2)
        df['find_nearby_showroom_CVR']   = (df['find_nearby_showroom']        / df['view_item']                  * 100).round(2)
        df['showroom_10s_CVR']           = (df['showroom_10s']                / df['view_item']                  * 100).round(2)
        df['add_to_cart_CVR']            = (df['add_to_cart']                 / df['view_item']                  * 100).round(2)
        df['showroom_leads_CVR']         = (df['showroom_leads']              / df['view_item']                  * 100).round(2)
        df['purchase_CVR1']              = (df['purchase']                    / df['view_item']                  * 100).round(2)
        df['purchase_CVR2']              = (df['purchase']                    / df['showroom_leads']             * 100).round(2)
        
        # 컬럼 순서 지정
        df = df[['event_date',
                'cost_gross_sum',
                'session_count','session_count_CPA',
                'view_item','view_item_CPA','view_item_CVR',
                'product_page_scroll_50','product_page_scroll_50_CPA','product_page_scroll_50_CVR',
                'product_option_price','product_option_price_CPA','product_option_price_CVR',
                'find_nearby_showroom','find_nearby_showroom_CPA','find_nearby_showroom_CVR',
                'add_to_cart','add_to_cart_CPA','add_to_cart_CVR',
                'showroom_10s','showroom_10s_CPA','showroom_10s_CVR',
                'showroom_leads','showroom_leads_CPA','showroom_leads_CVR',
                'purchase','purchase_CPA','purchase_CVR1','purchase_CVR2'
                ]]
        
        # 자료형 워싱
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        num_cols = df.select_dtypes(include=['number']).columns
        df[num_cols] = (df[num_cols].replace([np.inf, -np.inf], np.nan).fillna(0))

        # 컬럼 이름 변경 - 멀티 인덱스
        df.columns = pd.MultiIndex.from_tuples([
            ("기본정보",      "날짜"),             # event_date
            ("기본정보",      "매출"),             # cost_gross_sum
            
            ("유입 세션수",   "Actual"),           # session_count
            ("유입 세션수",   "CPA"),              # session_count_CPA

            ("PDP조회",   "Actual"),              # view_item
            ("PDP조회",   "CPA"),                 # view_item_CPA
            ("PDP조회",   "CVR"),                 # view_item_CVR

            ("PDPscr50",   "Actual"),            # product_page_scroll_50
            ("PDPscr50",   "CPA"),               # product_page_scroll_50_CPA
            ("PDPscr50",   "CVR"),               # product_page_scroll_50_CVR

            ("가격표시",   "Actual"),             # product_option_price
            ("가격표시",   "CPA"),                # product_option_price_CPA
            ("가격표시",   "CVR"),                # product_option_price_CVR

            ("쇼룸찾기",   "Actual"),             # find_nearby_showroom
            ("쇼룸찾기",   "CPA"),                # find_nearby_showroom_CPA
            ("쇼룸찾기",   "CVR"),                # find_nearby_showroom_CVR

            ("장바구니",   "Actual"),             # add_to_cart
            ("장바구니",   "CPA"),                # add_to_cart_CPA
            ("장바구니",   "CVR"),                # add_to_cart_CVR

            ("쇼룸10초",   "Actual"),             # showroom_10s
            ("쇼룸10초",   "CPA"),                # showroom_10s_CPA
            ("쇼룸10초",   "CVR"),                # showroom_10s_CVR

            ("쇼룸예약",   "Actual"),             # showroom_leads
            ("쇼룸예약",   "CPA"),                # showroom_leads_CPA
            ("쇼룸예약",   "CVR"),                # showroom_leads_CVR
            
            ("구매완료",   "Actual"),             # purchase
            ("구매완료",   "CPA"),                # purchase_CPA
            ("구매완료",   "CVR1"),               # purchase_CVR1
            ("구매완료",   "CVR2"),               # purchase_CVR2
            
        ], names=["그룹","지표"])  # 상단 레벨 이름(옵션)        
        
        return df


    def render_style(target_df):
        styled = style_format(
            decorate_df(target_df),
            decimals_map={
                ("기본정보",      "매출"): 0,
                ("유입 세션수",   "Actual"): 0,           # session_count
                ("유입 세션수",   "CPA"): 0,              # session_count_CPA
                ("PDP조회",   "Actual"): 0,              # view_item
                ("PDP조회",   "CPA"): 0,                 # view_item_CPA
                ("PDP조회",   "CVR"): 2,                 # view_item_CVR
                ("PDPscr50",   "Actual"): 0,            # product_page_scroll_50
                ("PDPscr50",   "CPA"): 0,               # product_page_scroll_50_CPA
                ("PDPscr50",   "CVR"): 2,               # product_page_scroll_50_CVR
                ("가격표시",   "Actual"): 0,             # product_option_price
                ("가격표시",   "CPA"): 0,                # product_option_price_CPA
                ("가격표시",   "CVR"): 2,                # product_option_price_CVR
                ("쇼룸찾기",   "Actual"): 0,             # find_nearby_showroom
                ("쇼룸찾기",   "CPA"): 0,                # find_nearby_showroom_CPA
                ("쇼룸찾기",   "CVR"): 2,                # find_nearby_showroom_CVR
                ("장바구니",   "Actual"): 0,             # add_to_cart
                ("장바구니",   "CPA"): 0,                # add_to_cart_CPA
                ("장바구니",   "CVR"): 2,                # add_to_cart_CVR
                ("쇼룸10초",   "Actual"): 0,             # showroom_10s
                ("쇼룸10초",   "CPA"): 0,                # showroom_10s_CPA
                ("쇼룸10초",   "CVR"): 2,                # showroom_10s_CVR
                ("쇼룸예약",   "Actual"): 0,             # showroom_leads
                ("쇼룸예약",   "CPA"): 0,                # showroom_leads_CPA
                ("쇼룸예약",   "CVR"): 2,                # showroom_leads_CVR
                ("구매완료",   "Actual"): 0,             # purchase
                ("구매완료",   "CPA"): 0,                # purchase_CPA
                ("구매완료",   "CVR1"): 2,               # purchase_CVR1
                ("구매완료",   "CVR2"): 2,               # purchase_CVR2
            },
            suffix_map={
                ("PDP조회",   "CVR"): " %",                 # view_item_CVR
                ("PDPscr50",   "CVR"): " %",               # product_page_scroll_50_CVR
                ("가격표시",   "CVR"): " %",                # product_option_price_CVR
                ("쇼룸찾기",   "CVR"): " %",                # find_nearby_showroom_CVR
                ("장바구니",   "CVR"): " %",                # add_to_cart_CVR
                ("쇼룸10초",   "CVR"): " %",                # showroom_10s_CVR
                ("쇼룸예약",   "CVR"): " %",                # showroom_leads_CVR
                ("구매완료",   "CVR1"): " %",               # purchase_CVR1
                ("구매완료",   "CVR2"): " %",               # purchase_CVR2
        }
        )
        styled2 = style_cmap(
            styled,
            gradient_rules=[
                {"col": ("유입 세션수", "Actual"), "cmap":"Blues", "vmax":20000, "low":0.0, "high":0.3},
                {"col": ("PDP조회", "Actual"), "cmap":"Blues", "vmax":10000, "low":0.0, "high":0.3},
                {"col": ("PDPscr50", "Actual"), "cmap":"Blues", "vmax":3000, "low":0.0, "high":0.3},
                {"col": ("가격표시", "Actual"), "cmap":"Blues", "vmax":2000, "low":0.0, "high":0.3},
                {"col": ("장바구니", "Actual"), "cmap":"Blues", "vmax":1000, "low":0.0, "high":0.3},
                {"col": ("쇼룸10초", "Actual"), "cmap":"Blues", "vmax":2000, "low":0.0, "high":0.3},
                {"col": ("쇼룸예약", "Actual"), "cmap":"Blues", "vmax":200, "low":0.0, "high":0.3},
                {"col": ("구매완료", "Actual"), "cmap":"Blues", "vmax":100, "low":0.0, "high":0.3},
            ]
        )
        
        st.dataframe(styled2, use_container_width=True)


    # def render_aggrid(
    #     df: pd.DataFrame,
    #     height: int = 401,
    #     use_parent: bool = True
    #     ) -> None:
    #     """
    #     use_parent: False / True
    #     """
    #     df2 = df.copy()
        
    #     # (주의) 누락됱 컬럼히 당연히 있을수 있음, 그래서 fillna만 해주는게 아니라 컬럼 자리를 만들어서 fillna 해야함.
    #     expected_cols = ['session_count','view_item','product_page_scroll_50','product_option_price','find_nearby_showroom','showroom_10s','add_to_cart','showroom_leads','purchase']
    #     for col in expected_cols:
    #         df2[col] = df2.get(col, 0)
    #     df2.fillna(0, inplace=True)     # (기존과 동일) 값이 없는 경우 일단 0으로 치환
        
    #     # 전처리 영역 (파생지표 생성) - CPA
    #     df2['session_count_CPA']               = (df2['cost_gross_sum']               / df2['session_count']             ).round(0)
    #     df2['view_item_CPA']                   = (df2['cost_gross_sum']               / df2['view_item']                 ).round(0)
    #     df2['product_page_scroll_50_CPA']      = (df2['cost_gross_sum']                   / df2['product_page_scroll_50']).round(0)
    #     df2['product_option_price_CPA']        = (df2['cost_gross_sum']                   / df2['product_option_price']  ).round(0)
    #     df2['find_nearby_showroom_CPA']        = (df2['cost_gross_sum']                   / df2['find_nearby_showroom']  ).round(0)
    #     df2['showroom_10s_CPA']                = (df2['cost_gross_sum']                   / df2['showroom_10s']          ).round(0)
    #     df2['add_to_cart_CPA']                 = (df2['cost_gross_sum']                   / df2['add_to_cart']           ).round(0)
    #     df2['showroom_leads_CPA']              = (df2['cost_gross_sum']                   / df2['showroom_leads']        ).round(0)
    #     df2['purchase_CPA']                    = (df2['cost_gross_sum']                   / df2['purchase']              ).round(0)
        
    #     # 전처리 영역 (파생지표 생성) - CVR
    #     df2['session_count_CVR']          = (df2['session_count']               / df2['session_count']              * 100).round(2)
    #     df2['view_item_CVR']              = (df2['view_item']                   / df2['session_count']              * 100).round(2)
    #     df2['product_page_scroll_50_CVR'] = (df2['product_page_scroll_50']      / df2['view_item']                  * 100).round(2)
    #     df2['product_option_price_CVR']   = (df2['product_option_price']        / df2['view_item']                  * 100).round(2)
    #     df2['find_nearby_showroom_CVR']   = (df2['find_nearby_showroom']        / df2['view_item']                  * 100).round(2)
    #     df2['showroom_10s_CVR']           = (df2['showroom_10s']                / df2['view_item']                  * 100).round(2)
    #     df2['add_to_cart_CVR']            = (df2['add_to_cart']                 / df2['view_item']                  * 100).round(2)
    #     df2['showroom_leads_CVR']         = (df2['showroom_leads']              / df2['view_item']                  * 100).round(2)
    #     df2['purchase_CVR1']              = (df2['purchase']                    / df2['view_item']                  * 100).round(2)
    #     df2['purchase_CVR2']              = (df2['purchase']                    / df2['showroom_leads']             * 100).round(2)
        

    #     # 컬럼순서 지정
    #     df2 = df2[['event_date',
    #                 'cost_gross_sum',
    #                 'session_count','session_count_CPA','session_count_CVR',
    #                 'view_item','view_item_CPA','view_item_CVR',
    #                 'product_page_scroll_50','product_page_scroll_50_CPA','product_page_scroll_50_CVR',
    #                 'product_option_price','product_option_price_CPA','product_option_price_CVR',
    #                 'find_nearby_showroom','find_nearby_showroom_CPA','find_nearby_showroom_CVR',
    #                 'showroom_10s','showroom_10s_CPA','showroom_10s_CVR',
    #                 'add_to_cart','add_to_cart_CPA','add_to_cart_CVR',
    #                 'showroom_leads','showroom_leads_CPA','showroom_leads_CVR',
    #                 'purchase','purchase_CPA','purchase_CVR1','purchase_CVR2'
    #             ]]

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
    #         "field": "event_date",
    #         "pinned": "left",
    #         "width": 100,
    #         "cellStyle": JsCode("params=>({textAlign:'left'})"),
    #         "sort": "desc"
    #     }
        
    #     flat_cols = [
    #         date_col,
    #         make_num_child("광고비",                         "cost_gross_sum"),
    #         make_num_child("세션수",                         "session_count"),
    #         make_num_child("세션수 CPA",                     "session_count_CPA"),
    #         make_num_child("세션수 CVR",                     "session_count_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("PDP조회",                       "view_item"),
    #         make_num_child("PDP조회 CPA",                   "view_item_CPA"),
    #         make_num_child("PDP조회 CVR",                   "view_item_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("PDP스크롤50",                   "product_page_scroll_50"),
    #         make_num_child("PDP스크롤50 CPA",               "product_page_scroll_50_CPA"),
    #         make_num_child("PDP스크롤50 CVR",               "product_page_scroll_50_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("가격표시",                       "product_option_price"),
    #         make_num_child("가격표시 CPA",                   "product_option_price_CPA"),
    #         make_num_child("가격표시 CVR",                   "product_option_price_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("쇼룸찾기",                       "find_nearby_showroom"),
    #         make_num_child("쇼룸찾기 CPA",                   "find_nearby_showroom_CPA"),
    #         make_num_child("쇼룸찾기 CVR",                   "find_nearby_showroom_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("쇼룸10초",                       "showroom_10s"),
    #         make_num_child("쇼룸10초 CPA",                   "showroom_10s_CPA"),
    #         make_num_child("쇼룸10초 CVR",                   "showroom_10s_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("장바구니",                       "add_to_cart"),
    #         make_num_child("장바구니 CPA",                   "add_to_cart_CPA"),
    #         make_num_child("장바구니 CVR",                   "add_to_cart_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("쇼룸예약",                       "showroom_leads"),
    #         make_num_child("쇼룸예약 CPA",                   "showroom_leads_CPA"),
    #         make_num_child("쇼룸예약 CVR",                   "showroom_leads_CVR", fmt_digits=2, suffix="%"),
    #         make_num_child("구매완료",                       "purchase"),
    #         make_num_child("구매완료 CPA",                   "purchase_CPA"),
    #         make_num_child("구매완료 CVR1",                  "purchase_CVR1", fmt_digits=2, suffix="%"),
    #         make_num_child("구매완료 CVR2",                  "purchase_CVR2", fmt_digits=2, suffix="%"),
    #     ]

        
    #     # (use_parent) grouped_cols
    #     grouped_cols = [
    #         date_col,
    #         make_num_child("광고비", "cost_gross_sum"),
    #         # 세션수
    #         {
    #             "headerName": "세션수",
    #             "children": [
    #                 make_num_child("세션수",           "session_count"),
    #                 make_num_child("CPA",             "session_count_CPA"),
    #                 make_num_child("CVR",             "session_count_CVR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #         # PDP 조회
    #         {
    #             "headerName": "PDP조회",
    #             "children": [
    #                 make_num_child("Actual",         "view_item"),
    #                 make_num_child("CPA",             "view_item_CPA"),
    #                 make_num_child("CVR",             "view_item_CVR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #         # PDP스크롤50
    #         {
    #             "headerName": "PDPscr50",
    #             "children": [
    #                 make_num_child("Actual",         "product_page_scroll_50"),
    #                 make_num_child("CPA",             "product_page_scroll_50_CPA"),
    #                 make_num_child("CVR",             "product_page_scroll_50_CVR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #         # 가격표시
    #         {
    #             "headerName": "가격표시",
    #             "children": [
    #                 make_num_child("Actual",         "product_option_price"),
    #                 make_num_child("CPA",             "product_option_price_CPA"),
    #                 make_num_child("CVR",             "product_option_price_CVR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #         # 쇼룸찾기
    #         {
    #             "headerName": "쇼룸찾기",
    #             "children": [
    #                 make_num_child("Actual",         "find_nearby_showroom"),
    #                 make_num_child("CPA",             "find_nearby_showroom_CPA"),
    #                 make_num_child("CVR",             "find_nearby_showroom_CVR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #         # 쇼룸10초
    #         {
    #             "headerName": "쇼룸10초",
    #             "children": [
    #                 make_num_child("Actual",         "showroom_10s"),
    #                 make_num_child("CPA",             "showroom_10s_CPA"),
    #                 make_num_child("CVR",             "showroom_10s_CVR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #         # 장바구니
    #         {
    #             "headerName": "장바구니",
    #             "children": [
    #                 make_num_child("Actual",         "add_to_cart"),
    #                 make_num_child("CPA",             "add_to_cart_CPA"),
    #                 make_num_child("CVR",             "add_to_cart_CVR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #         # 쇼룸예약
    #         {
    #             "headerName": "쇼룸예약",
    #             "children": [
    #                 make_num_child("Actual",         "showroom_leads"),
    #                 make_num_child("CPA",             "showroom_leads_CPA"),
    #                 make_num_child("CVR",             "showroom_leads_CVR", fmt_digits=2, suffix="%"),
    #             ]
    #         },
    #         # 구매완료 (CVR1 & CVR2)
    #         {
    #             "headerName": "구매완료",
    #             "children": [
    #                 make_num_child("Actual",         "purchase"),
    #                 make_num_child("CPA",             "purchase_CPA"),
    #                 make_num_child("CVR1",            "purchase_CVR1", fmt_digits=2, suffix="%"),
    #                 make_num_child("CVR2",            "purchase_CVR2", fmt_digits=2, suffix="%"),
    #             ]
    #         },
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
    #         "minWidth": 90   # 최소 너비
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
        
    #     # AgGrid(
    #     #     df2,
    #     #     gridOptions=grid_options,
    #     #     height=height,
    #     #     fit_columns_on_grid_load=False,  # True면 전체넓이에서 균등분배 
    #     #     theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
    #     #     allow_unsafe_jscode=True
    #     # )

    #     # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #     grid_options = add_summary(
    #         grid_options,
    #         df2,
    #         {
    #             'cost_gross_sum': 'sum',
    #             'session_count': 'sum',
    #             'session_count_CPA': 'avg',
    #             'session_count_CVR': 'avg',
    #             'view_item': 'sum',
    #             'view_item_CPA': 'avg',
    #             'view_item_CVR': 'avg',
    #             'product_page_scroll_50': 'sum',
    #             'product_page_scroll_50_CPA': 'avg',
    #             'product_page_scroll_50_CVR': 'avg',
    #             'product_option_price': 'sum',
    #             'product_option_price_CPA': 'avg',
    #             'product_option_price_CVR': 'avg',
    #             'find_nearby_showroom': 'sum',
    #             'find_nearby_showroom_CPA': 'avg',
    #             'find_nearby_showroom_CVR': 'avg',
    #             'showroom_10s': 'sum',
    #             'showroom_10s_CPA': 'avg',
    #             'showroom_10s_CVR': 'avg',
    #             'add_to_cart': 'sum',
    #             'add_to_cart_CPA': 'avg',
    #             'add_to_cart_CVR': 'avg',
    #             'showroom_leads': 'sum',
    #             'showroom_leads_CPA': 'avg',
    #             'showroom_leads_CVR': 'avg',
    #             'purchase': 'sum',
    #             'purchase_CPA': 'avg',
    #             'purchase_CVR1': 'avg',
    #             'purchase_CVR2': 'avg',
    #         }
    #     )

    #     AgGrid(
    #         df2,
    #         gridOptions=grid_options,
    #         height=height,
    #         fit_columns_on_grid_load=False,  # True면 전체넓이에서 균등분배 
    #         theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
    #         allow_unsafe_jscode=True,
    #         reload_data=True
    #     )
    

    
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)
    
    
    # ────────────────────────────────────────────────────────────────
    # 통합 액션 리포트 
    # ────────────────────────────────────────────────────────────────
    st.markdown("<h5 style='margin:0'><span style='color:#FF804B;'>통합</span> 액션 리포트</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ날짜별 **광고비**, **세션수 및 주요 액션별 효율**(GA) 데이터를 표에서 확인할 수 있습니다.", unsafe_allow_html=True)

    render_style(df_total)


    # ────────────────────────────────────────────────────────────────
    # 슬립퍼 액션 리포트 
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'><span style='color:#FF804B;'>슬립퍼</span> 액션 리포트</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ탭을 클릭하여, 품목별 데이터를 확인할 수 있습니다.", unsafe_allow_html=True)
    
    tabs = st.tabs(["슬립퍼 통합", "슬립퍼 PAID", "슬립퍼 매트리스", "슬립퍼 매트리스 PAID", "슬립퍼 프레임", "슬립퍼 프레임 PAID"])
    with tabs[0]:
        render_style(df_slp)
    with tabs[1]:
        render_style(df_slp_y)
    with tabs[2]:
        render_style(df_slp_mat)
    with tabs[3]:
        render_style(df_slp_mat_y)
    with tabs[4]:
        render_style(df_slp_frm)
    with tabs[5]:
        render_style(df_slp_frm_y)


    # ────────────────────────────────────────────────────────────────
    # 누어 액션 리포트 
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'><span style='color:#FF804B;'>누어</span> 액션 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ탭을 클릭하여, 품목별 데이터를 확인할 수 있습니다.", unsafe_allow_html=True)

    tabs = st.tabs(["누어 통합", "누어 매트리스", "누어 프레임"])
    with tabs[0]:
        render_style(df_nor)
    with tabs[1]:
        render_style(df_nor_mat)
    with tabs[2]:
        render_style(df_nor_frm)    


if __name__ == '__main__':
    main()
