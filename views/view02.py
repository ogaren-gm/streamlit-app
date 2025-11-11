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
from zoneinfo import ZoneInfo



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


    # ────────────────────────────────────────────────────────────────
    # 사이드바 필터 설정
    # ────────────────────────────────────────────────────────────────
    st.sidebar.header("Filter")
    
    today = datetime.now().date()
    default_end = today - timedelta(days=1)
    default_start = today - timedelta(days=7)
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
        
        # # cost_gross
        # merged['cost_gross'] = np.where(
        #     merged['media_name'].isin(['GOOGLE','META']), merged['cost']*1.1/0.98, merged['cost']
        # )
        
        # cost_gross(v2)
        merged['cost_gross'] = np.where(
            merged['event_date'] < pd.to_datetime("2025-11-06"),
            np.where(
                merged['media_name'].isin(['GOOGLE', 'META']),
                merged['cost'] * 1.1 / 0.98,
                merged['cost']
            ),
            np.where(
                merged['media_name'].isin(['GOOGLE', 'META']),
                merged['cost'] * 1.1 / 0.955,
                merged['cost']
            )
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
        
        # df_psi = (
        #     df_psi
        #     .groupby("event_date", as_index=False)
        #     .agg(
        #         session_count            = ("pseudo_session_id",       "nunique"),
        #         view_item                = ("view_item",               "sum"),
        #         product_page_scroll_50   = ("product_page_scroll_50",  "sum"),
        #         product_option_price     = ("product_option_price",    "sum"),
        #         find_nearby_showroom     = ("find_nearby_showroom",    "sum"),
        #         showroom_10s             = ("showroom_10s",            "sum"),
        #         add_to_cart              = ("add_to_cart",             "sum"),
        #         showroom_leads           = ("showroom_leads",          "sum"),
        #         purchase                 = ("purchase",                "sum"),
        #     )
        #     .sort_values("event_date")
        # )

        ## 대체 
        events = [
            "view_item","product_page_scroll_50","product_option_price",
            "find_nearby_showroom","showroom_10s","add_to_cart",
            "showroom_leads","purchase"
        ]
        df_psi[events] = df_psi[events].apply(pd.to_numeric, errors="coerce").fillna(0)

        ses_level = (
            df_psi
            .groupby(["event_date", "pseudo_session_id"], as_index=False)[events]
            .sum()
        )
        ses_level[events] = (ses_level[events] > 0).astype(int)  # 0/1 플래그화

        agg_dict = {"pseudo_session_id": "nunique"}
        agg_dict.update({e: "sum" for e in events})

        df_psi = (
            ses_level
            .groupby("event_date", as_index=False)
            .agg(agg_dict)
            .rename(columns={"pseudo_session_id": "session_count"})
            .sort_values("event_date")
        )

        df_psi['event_date'] = pd.to_datetime(df_psi['event_date'], errors='coerce')
        df_psi['event_date'] = df_psi['event_date'].dt.strftime('%Y-%m-%d')

        last_updated_time = merged['event_date'].max()
    
        return merged, df_prod_rep, df_psi, last_updated_time
    
    # ────────────────────────────────────────────────────────────────
    # 데이터 불러오기
    # ────────────────────────────────────────────────────────────────
    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df_merged, df_prodRep, df_psi, last_updated_time = load_data(cs, ce)

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

        # 합계 & 평균 행 추가
        sum_row = df[num_cols].sum().to_frame().T
        sum_row['event_date'] = "합계"
        mean_row = df[num_cols].mean().to_frame().T
        mean_row['event_date'] = "평균"
        df = pd.concat([df, sum_row, mean_row], ignore_index=True)

        # 컬럼 이름 변경 - 멀티 인덱스
        df.columns = pd.MultiIndex.from_tuples([
            ("기본정보",      "날짜"),             # event_date
            ("기본정보",      "광고비"),             # cost_gross_sum
            
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
                ("기본정보",      "광고비"): 0,
                ("유입 세션수",   "Actual"): 0,           # session_count
                ("유입 세션수",   "CPA"): 0,              # session_count_CPA
                ("PDP조회",   "Actual"): 0,              # view_item
                ("PDP조회",   "CPA"): 0,                 # view_item_CPA
                ("PDP조회",   "CVR"): 1,                 # view_item_CVR
                ("PDPscr50",   "Actual"): 0,            # product_page_scroll_50
                ("PDPscr50",   "CPA"): 0,               # product_page_scroll_50_CPA
                ("PDPscr50",   "CVR"): 1,               # product_page_scroll_50_CVR
                ("가격표시",   "Actual"): 0,             # product_option_price
                ("가격표시",   "CPA"): 0,                # product_option_price_CPA
                ("가격표시",   "CVR"): 1,                # product_option_price_CVR
                ("쇼룸찾기",   "Actual"): 0,             # find_nearby_showroom
                ("쇼룸찾기",   "CPA"): 0,                # find_nearby_showroom_CPA
                ("쇼룸찾기",   "CVR"): 1,                # find_nearby_showroom_CVR
                ("장바구니",   "Actual"): 0,             # add_to_cart
                ("장바구니",   "CPA"): 0,                # add_to_cart_CPA
                ("장바구니",   "CVR"): 1,                # add_to_cart_CVR
                ("쇼룸10초",   "Actual"): 0,             # showroom_10s
                ("쇼룸10초",   "CPA"): 0,                # showroom_10s_CPA
                ("쇼룸10초",   "CVR"): 1,                # showroom_10s_CVR
                ("쇼룸예약",   "Actual"): 0,             # showroom_leads
                ("쇼룸예약",   "CPA"): 0,                # showroom_leads_CPA
                ("쇼룸예약",   "CVR"): 1,                # showroom_leads_CVR
                ("구매완료",   "Actual"): 0,             # purchase
                ("구매완료",   "CPA"): 0,                # purchase_CPA
                ("구매완료",   "CVR1"): 1,               # purchase_CVR1
                ("구매완료",   "CVR2"): 1,               # purchase_CVR2
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
                {"col": ("유입 세션수", "Actual"), "cmap":"OrRd", "low":0.0, "high":0.3},
                {"col": ("PDP조회", "Actual"), "cmap":"OrRd",  "low":0.0, "high":0.3},
                {"col": ("PDPscr50", "Actual"), "cmap":"OrRd", "low":0.0, "high":0.3},
                {"col": ("가격표시", "Actual"), "cmap":"OrRd",  "low":0.0, "high":0.3},
                {"col": ("쇼룸찾기", "Actual"), "cmap":"OrRd",  "low":0.0, "high":0.3},
                {"col": ("장바구니", "Actual"), "cmap":"OrRd", "low":0.0, "high":0.3},
                {"col": ("쇼룸10초", "Actual"), "cmap":"OrRd",  "low":0.0, "high":0.3},
                {"col": ("쇼룸예약", "Actual"), "cmap":"OrRd",  "low":0.0, "high":0.3},
                {"col": ("구매완료", "Actual"), "cmap":"OrRd",  "low":0.0, "high":0.3},
            ]
        )
        
        st.dataframe(styled2, use_container_width=True, row_height=30, hide_index=True)

    
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)


    # (25.11.10) 제목 + 설명 + 업데이트 시각 + 캐시초기화 
    # last_updated_time
    # 제목
    st.subheader("액션 종합 대시보드")

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
            방문 → 상품조회 → 쇼룸찾기 → 구매 등 
            자사몰의 <b>주요 액션 흐름</b>을 단계적으로 보여주는 
            <b>행동 퍼널 대시보드</b>입니다.<br>
            </div>
            <div style="
                color:#6c757d;        
                font-size:14px;       
                line-height:2.0;      
            ">
            ※ GA×MEDIA D-1 매칭 데이터는 매일 15시 ~ 16시 사이에 업데이트됩니다.
            </div>
            """,
            unsafe_allow_html=True
        )
        
    with col2:
        # last_updated_time
        if isinstance(last_updated_time, str):
            lut = datetime.strptime(last_updated_time, "%Y%m%d")
        else:
            lut = last_updated_time
        lut_date = lut.date()
        
        now_kst   = datetime.now(ZoneInfo("Asia/Seoul"))
        today_kst = now_kst.date()
        delta_days = (today_kst - lut_date).days
        
        # 기본값
        # msg    = f"{lut_date.strftime('%m월 %d일')} (D-{delta_days})"
        msg    = f"D-{delta_days} 업데이트 완료"
        sub_bg = "#fff7ed"
        sub_bd = "#fdba74"
        sub_fg = "#c2410c"
        
        
        # 렌더링
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
            <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 10px;
                font-size:13px;line-height:1.1;
                color:{sub_fg};background:{sub_bg};border:1px solid {sub_bd};
                border-radius:10px;white-space:nowrap;">
                📢 {msg}
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



    # ────────────────────────────────────────────────────────────────
    # 시각화 (개별 기간 조정 · 독립 키 · 차트 전용 재조회)
    # ────────────────────────────────────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>시계열 분석</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ주요 매출 지표의 추이를 스무딩 기법으로 정제해, 단기 변동 대신 핵심 흐름을 시각화합니다.", unsafe_allow_html=True)

    with st.expander("스무딩은 시계열 분석에서 노이즈를 제거하고 추세를 도출하는 밥법론입니다. ", expanded=False):
        st.markdown("""
    - **MA (이동평균)** : 기본 스무딩, 최근 S일 평균으로 요동을 눌러 큰 흐름만 보이게 합니다.
    - **EWMA (지수가중 이동평균)** : 가중 스무딩, 최근 값에 더 큰 가중치를 주어 변화에 민감합니다.
    - **STL 분해** : 주기성(Seasonal)을 제거하고, 이상/극단치의 영향을 적게 받는 방식으로, 순수 추세만 보여줍니다.
    - **Seasonally Adjusted** : 주기성(Seasonal)만 제거하고, 이벤트나 프로모션 등의 잔차는 남겨, 순수 변화량 추세를 보여줍니다.
    """)

    # ── 차트 전용 날짜(보고서 필터와 분리)
    from pandas.tseries.offsets import MonthEnd, DateOffset
    _today = pd.Timestamp.today().normalize()
    _chart_end = (_today - pd.Timedelta(days=1)).date()            # D-1
    # "항상 가지고 있는 전체 데이터" 시작일로 원하는 기준을 넣어줘 (예: 2025-07-01)
    cs_chart = "20250701"
    ce_chart = pd.Timestamp(_chart_end).strftime("%Y%m%d")

    # 차트 전용으로 재조회 (보고서 표의 cs/ce와 무관)
    df_merged_chart, df_prodRep_chart, df_psi_chart, last_updated_time = load_data(cs_chart, ce_chart)
    

    # ── 차트용 집계 헬퍼
    def _pivot_cstSes(df, brand_type=None, product_type=None):
        d = df.copy()
        if brand_type:
            d = d[d['brand_type'].astype(str).str.contains(brand_type, regex=True, na=False)]
        if product_type:
            d = d[d['product_type'].astype(str).str.contains(product_type, regex=True, na=False)]
        d['event_date'] = pd.to_datetime(d['event_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        return (d.groupby('event_date', as_index=False)
                .agg(session_count=('pseudo_session_id','sum'),
                    cost_gross_sum=('cost_gross','sum'))
                .sort_values('event_date'))

    EVENTS = [
        "view_item","product_page_scroll_50","product_option_price",
        "find_nearby_showroom","showroom_10s","add_to_cart",
        "showroom_leads","purchase"
    ]

    def _pivot_prdRep_events(df, brand_type=None, product_type=None):
        d = df.copy()
        if brand_type:
            d = d[d['product_cat_a'].astype(str).str.contains(brand_type, regex=True, na=False)]
        if product_type:
            d = d[d['product_cat_b'].astype(str).str.contains(product_type, regex=True, na=False)]
        d['event_date'] = pd.to_datetime(d['event_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        pv = (d.groupby(['event_date','event_name'])['pseudo_session_id']
                .nunique().unstack(fill_value=0).reset_index())

        # 누락 컬럼 보호
        for c in EVENTS:
            if c not in pv.columns:
                pv[c] = 0

        cols = ['event_date'] + EVENTS
        return pv[cols].sort_values('event_date')


    # 전체
    _df_c_all   = _pivot_cstSes(df_merged_chart)
    _df_e_all   = _pivot_prdRep_events(df_prodRep_chart)
    df_total_ts = (_df_c_all.merge(_df_e_all, on='event_date', how='left')
                            .fillna(0).sort_values('event_date'))

    # 슬립퍼
    _df_c_slp   = _pivot_cstSes(df_merged_chart, brand_type="슬립퍼")
    _df_e_slp   = _pivot_prdRep_events(df_prodRep_chart, brand_type="슬립퍼")
    df_slp_ts   = (_df_c_slp.merge(_df_e_slp, on='event_date', how='left')
                            .fillna(0).sort_values('event_date'))

    # 누어
    _df_c_nor   = _pivot_cstSes(df_merged_chart, brand_type="누어")
    _df_e_nor   = _pivot_prdRep_events(df_prodRep_chart, brand_type="누어")
    df_nor_ts   = (_df_c_nor.merge(_df_e_nor, on='event_date', how='left')
                            .fillna(0).sort_values('event_date'))


    options = {
        "전체 통합": df_total_ts,
        "슬립퍼 통합": df_slp_ts,
        "누어 통합": df_nor_ts
    }

    # ── (2) 컨트롤 (이 영역만 독립 키 사용)
    c1, c2, c3, _pad, c4 = st.columns([3,3,3,0.5,3])
    with c1:
        ds_name = st.selectbox("데이터 선택", list(options.keys()), index=0, key="ts2_ds")
    df_ts = options[ds_name].copy()

    # 날짜 정규화
    date_col = 'event_date' if 'event_date' in df_ts.columns else None
    if date_col is None:
        st.error("날짜 컬럼이 없어 시계열을 그릴 수 없습니다.")
    else:
        df_ts[date_col] = pd.to_datetime(df_ts[date_col], errors='coerce')
        df_ts = df_ts.dropna(subset=[date_col]).sort_values(date_col)

    # ── 지표 라벨 맵 (새 스키마 대응)
    label_map = {
        "session_count"            : "세션수",
        "cost_gross_sum"           : "광고비",
        "view_item"                : "PDP조회",
        "product_page_scroll_50"   : "PDPscr50",
        "product_option_price"     : "가격표시",
        "find_nearby_showroom"     : "쇼룸찾기",
        "showroom_10s"             : "쇼룸10초",
        "add_to_cart"              : "장바구니",
        "showroom_leads"           : "쇼룸예약",
        "purchase"                 : "구매완료",
    }

    # 화면에 노출할 지표 우선순위(앞에 있을수록 select 기본값에 가깝게 됨)
    _metric_priority = [
        "view_item", "product_page_scroll_50", "product_option_price",
        "find_nearby_showroom", "showroom_10s", "add_to_cart",
        "showroom_leads", "purchase"
        
    ]

    # 실제 df_ts에 존재하는 컬럼만 후보로 구성
    metric_candidates = [c for c in _metric_priority if c in df_ts.columns]

    with c2:
        metric = st.selectbox(
            "지표 선택",
            metric_candidates if metric_candidates else [c for c in df_ts.columns if c in label_map],
            index=0,
            key="ts2_metric",
            format_func=lambda k: label_map.get(k, k)
        )


    overlay_options = ["MA (이동평균)", "EWMA (지수가중 이동평균)", "STL 분해", "Seasonally Adjusted"]
    with c3:
        overlay = st.selectbox("스무딩 기법 선택", overlay_options, index=0, key="ts2_overlay")

    with c4:
        period = st.radio("주기(S) 선택", [7, 14], horizontal=True, index=0, key="ts2_period",
                        help="디폴트값인 7일을 권장합니다. 이 값은 이동평균 평활, 세로선 간격, 볼린저 밴드에 사용됩니다.")

    # ── (3) 월 단위 선택 슬라이더 — 기본: 최신 2개월
    if date_col:
        date_min = pd.to_datetime(df_ts[date_col].min()).normalize()
        date_max = pd.to_datetime(df_ts[date_col].max()).normalize()
        if pd.isna(date_min) or pd.isna(date_max):
            st.warning("유효한 날짜 데이터가 없습니다.")
        else:
            start_period = date_min.to_period("M")
            end_period   = date_max.to_period("M")
            month_options = [p.to_timestamp() for p in pd.period_range(start=start_period, end=end_period, freq="M")]

            if len(month_options) == 0:
                st.warning("표시할 월 정보가 없습니다.")
            elif len(month_options) == 1:
                start_sel = end_sel = month_options[0]
                st.select_slider("기간(월)", options=month_options, value=start_sel,
                                format_func=lambda x: x.strftime("%Y-%m"), key="ts2_period_single")
            else:
                default_start, default_end = (month_options[-2], month_options[-1])
                start_sel, end_sel = st.select_slider(
                    "기간(월)", options=month_options, value=(default_start, default_end),
                    format_func=lambda x: x.strftime("%Y-%m"), key="ts2_period_range"
                )
                if start_sel > end_sel:
                    start_sel, end_sel = end_sel, start_sel

            period_start, period_end = start_sel, end_sel + MonthEnd(0)
            dfp = df_ts[(df_ts[date_col] >= period_start) & (df_ts[date_col] <= period_end)].copy()

            # ── (4) 시계열 계산 및 그리기
            s = dfp.set_index(date_col)[metric].asfreq('D').fillna(0)
            if s.empty or s.dropna().shape[0] < 2:
                st.warning("선택한 기간에 표시할 데이터가 부족합니다. 기간을 넓혀주세요.")
            else:
                win = int(period)
                y_ma   = s.rolling(win, min_periods=1).mean() if overlay == "MA (이동평균)" else None
                y_ewma = s.ewm(halflife=win, adjust=False, min_periods=1).mean() if overlay == "EWMA (지수가중 이동평균)" else None

                y_trend = y_seas = y_sa = None
                if overlay in ("STL 분해", "Seasonally Adjusted"):
                    try:
                        from statsmodels.tsa.seasonal import STL
                        stl_period = max(2, min(win, max(2, len(s)//2)))
                        stl = STL(s, period=stl_period, robust=True).fit()
                        y_trend, y_seas = stl.trend, stl.seasonal
                    except Exception:
                        key    = np.arange(len(s)) % win
                        y_seas = s.groupby(key).transform('mean')
                        y_trend= (s - y_seas).rolling(win, min_periods=1, center=True).mean()
                    y_sa = (s - y_seas) if y_seas is not None else None

                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(
                    go.Scatter(x=s.index, y=s, name="RAW", mode="lines+markers",
                            line=dict(color="#666"), opacity=0.45),
                    secondary_y=False
                )

                # Bollinger Bands
                k = 2.0
                minp = int(min(win, max(2, len(s))))
                ma_bb = s.rolling(win, min_periods=minp).mean()
                sd_bb = s.rolling(win, min_periods=minp).std(ddof=0)
                bb_upper = ma_bb + k * sd_bb
                bb_lower = ma_bb - k * sd_bb

                fig.add_trace(go.Scatter(x=bb_upper.index, y=bb_upper, name="BB Upper",
                                        mode="lines", line=dict(width=1, color="#FFB6C1")),
                            secondary_y=False)
                fig.add_trace(go.Scatter(x=bb_lower.index, y=bb_lower, name="BB Lower",
                                        mode="lines", line=dict(width=1, color="#ADD8E6"),
                                        fill="tonexty", fillcolor="rgba(128,128,128,0.12)"),
                            secondary_y=False)

                # 오버레이
                overlay_series = None
                if overlay == "MA (이동평균)" and y_ma is not None:
                    overlay_series = y_ma
                    fig.add_trace(go.Scatter(x=y_ma.index, y=y_ma, name=f"MA{win}",
                                            mode="lines", line=dict(color="#FF4B4B")),
                                secondary_y=True)
                elif overlay == "EWMA (지수가중 이동평균)" and y_ewma is not None:
                    overlay_series = y_ewma
                    fig.add_trace(go.Scatter(x=y_ewma.index, y=y_ewma, name=f"EWMA(h={win})",
                                            mode="lines", line=dict(color="#FF4B4B")),
                                secondary_y=True)
                elif overlay == "STL 분해" and y_trend is not None:
                    overlay_series = y_trend
                    fig.add_trace(go.Scatter(x=y_trend.index, y=y_trend, name="STL 분해",
                                            mode="lines", line=dict(color="#FF4B4B")),
                                secondary_y=True)
                elif overlay == "Seasonally Adjusted" and y_sa is not None:
                    overlay_series = y_sa
                    fig.add_trace(go.Scatter(x=y_sa.index, y=y_sa, name="Seasonally Adjusted",
                                            mode="lines", line=dict(color="#FF4B4B")),
                                secondary_y=True)

                # 축 범위: 좌측은 RAW(+BB) 기준 / STL·SA는 우측 독립
                left_candidates = [s.dropna()]
                if (bb_upper is not None) and (not bb_upper.dropna().empty): left_candidates.append(bb_upper.dropna())
                if (bb_lower is not None) and (not bb_lower.dropna().empty): left_candidates.append(bb_lower.dropna())
                left_all = pd.concat(left_candidates) if left_candidates else s.dropna()
                right = overlay_series.dropna() if (overlay_series is not None) else None

                def _minmax_with_pad(series_min, series_max, pad_ratio=0.05, fallback_pad=1.0):
                    if (series_min is None) or (series_max is None): return None
                    if (not np.isfinite(series_min)) or (not np.isfinite(series_max)): return None
                    if series_max <= series_min:
                        return (series_min - fallback_pad, series_max + fallback_pad)
                    pad = (series_max - series_min) * pad_ratio
                    return (series_min - pad, series_max + pad)

                if not left_all.empty:
                    lrange = _minmax_with_pad(float(left_all.min()), float(left_all.max()))
                    if lrange is not None:
                        fig.update_yaxes(range=list(lrange), secondary_y=False)
                fig.update_yaxes(tickformat="~s", secondary_y=False)

                if (right is not None) and (not right.empty):
                    if overlay in ("STL 분해", "Seasonally Adjusted"):
                        rrange = _minmax_with_pad(float(right.min()), float(right.max()))
                        if rrange is not None:
                            fig.update_yaxes(range=list(rrange), secondary_y=True)
                    else:
                        if not left_all.empty and lrange is not None:
                            fig.update_yaxes(range=list(lrange), secondary_y=True)
                fig.update_yaxes(tickformat="~s", secondary_y=True)

                # 주기별 세로선
                start_ts = pd.to_datetime(s.index.min()).normalize()
                end_ts   = pd.to_datetime(s.index.max()).normalize()
                offset_days  = (6 - start_ts.weekday()) % 7  # 첫 일요일
                first_sunday = start_ts + pd.Timedelta(days=offset_days)
                step = 7 if win == 7 else 14
                t = first_sunday
                while t <= end_ts:
                    fig.add_vline(x=t, line_dash="dash", line_width=1, opacity=0.6, line_color="#8c8c8c")
                    t += pd.Timedelta(days=step)

                fig.update_yaxes(title_text=f"{label_map.get(metric, metric)} · RAW / BB", secondary_y=False)
                overlay_title = {
                    "MA (이동평균)": f"{label_map.get(metric, metric)} · MA{win}",
                    "EWMA (지수가중 이동평균)": f"EWMA (halflife={win})",
                    "STL 분해": "STL 분해",
                    "Seasonally Adjusted": "Seasonally Adjusted",
                }[overlay]
                fig.update_yaxes(title_text=overlay_title, secondary_y=True)

                # ★ 가로 그리드 제거 (좌/우 모두)
                fig.update_yaxes(showgrid=False, zeroline=False, secondary_y=False)
                fig.update_yaxes(showgrid=False, zeroline=False, secondary_y=True)

                fig.update_layout(
                    margin=dict(l=10, r=10, t=30, b=10),
                    legend=dict(orientation="h", y=1.03, x=1, xanchor="right", yanchor="bottom", title=None),
                )
                st.plotly_chart(fig, use_container_width=True)


    
    # ────────────────────────────────────────────────────────────────
    # 통합 액션 리포트 
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'>통합 액션 리포트</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일자별 **통합** 데이터와 효율 추이를 확인할 수 있습니다. ", unsafe_allow_html=True)
    with st.popover("지표 설명"):
        st.markdown("""
        - **CPA** (Cost Per Action) : **행동당 비용** (광고비 ÷ 전환수)  
        - **액션별 CVR**은 **이전행동**에서 **다음행동**으로 넘어가는 비율을 나타냅니다.  
            - PDP조회는 **전체 세션**을 기준으로,  
            - 이후 액션은 **PDP조회**를 기준으로,  
            - 구매완료는 2가지로 측정합니다.  
                - CVR1 : **PDP조회** → 구매완료  
                - CVR2 : **쇼룸예약** → 구매완료  
        """)
    render_style(df_total)


    # ────────────────────────────────────────────────────────────────
    # 슬립퍼 액션 리포트 
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>슬립퍼</span> 액션 리포트</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일자별 **품목** 데이터와 효율 추이를 확인할 수 있습니다. <span style='color:#8E9097;'>(15시 이후 D-1 데이터 제공)</span> ", unsafe_allow_html=True)
    with st.popover("지표 설명"):
        st.markdown("""
        - **CPA** (Cost Per Action) : **행동당 비용** (광고비 ÷ 전환수)  
        - **액션별 CVR**은 **이전행동**에서 **다음행동**으로 넘어가는 비율을 나타냅니다.  
            - PDP조회는 **전체 세션**을 기준으로,  
            - 이후 액션은 **PDP조회**를 기준으로,  
            - 구매완료는 2가지로 측정합니다.  
                - CVR1 : **PDP조회** → 구매완료  
                - CVR2 : **쇼룸예약** → 구매완료  
        """)
        
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
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>누어</span> 액션 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일자별 **품목** 데이터와 효율 추이를 확인할 수 있습니다. <span style='color:#8E9097;'>(15시 이후 D-1 데이터 제공)</span> ", unsafe_allow_html=True)
    with st.popover("지표 설명"):
        st.markdown("""
        - **CPA** (Cost Per Action) : **행동당 비용** (광고비 ÷ 전환수)  
        - **액션별 CVR**은 **이전행동**에서 **다음행동**으로 넘어가는 비율을 나타냅니다.  
            - PDP조회는 **전체 세션**을 기준으로,  
            - 이후 액션은 **PDP조회**를 기준으로,  
            - 구매완료는 2가지로 측정합니다.  
                - CVR1 : **PDP조회** → 구매완료  
                - CVR2 : **쇼룸예약** → 구매완료  
        """)
        
    tabs = st.tabs(["누어 통합", "누어 매트리스", "누어 프레임"])
    with tabs[0]:
        render_style(df_nor)
    with tabs[1]:
        render_style(df_nor_mat)
    with tabs[2]:
        render_style(df_nor_frm)    


if __name__ == '__main__':
    main()
