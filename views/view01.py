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
    st.subheader('매출 종합 대시보드')
    st.markdown("""
    이 대시보드는 **매출 · 광고비 · 유입** 데이터를 일자별로 한눈에 보여주는 **가장 개괄적인 대시보드**입니다.  
    여기서는 일자/브랜드/품목별로 “**얼마 벌었고, 얼마 썼고, 얼마 유입됐고**”를 효율 지표(AOV, ROAS, CVR)와 함께 확인할 수 있습니다.
    """)
    # st.markdown(
    #     '<a href="https://www.notion.so/Views-241521e07c7680df86eecf5c5f8da4af#241521e07c76805198d9eaf0c28deadb" target="_blank">'
    #     '🔍 지표 설명 & 대시보드 사용법 바로가기</a>',
    #     unsafe_allow_html=True
    # )
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
    default_start = today - timedelta(days=9)
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
            (merged['media_name']=='NSA') & merged['utm_source'].isna() &
            merged['utm_medium'].isna() & merged['media_name_type'].isin(['RSA_AD','TEXT_45'])
        )
        merged.loc[cond, ['utm_source','utm_medium']] = ['naver','search-nonmatch']
        
        # 3) tb_sleeper_psi
        df_psi = bq.get_data("tb_sleeper_psi")
        df_psi["event_date"] = pd.to_datetime(df_psi["event_date"], format="%Y%m%d")
        df_psi = (df_psi
                .assign(event_date=pd.to_datetime(df_psi["event_date"], format="%Y%m%d"))
                .groupby("event_date", as_index=False)
                .agg(session_count=("pseudo_session_id", "nunique")))

        return merged, df_psi

    # ────────────────────────────────────────────────────────────────
    # 데이터 불러오기
    # ────────────────────────────────────────────────────────────────
    st.toast("GA D-1 데이터는 오전에 예비 처리되고, **15시 이후에 최종 업데이트** 됩니다.", icon="🔔")

    with st.spinner("데이터가 많아 로딩에 조금 시간이 소요됩니다. 조금만 기다려 주세요."):
        df_merged, df_psi = load_data(cs, ce)
    

    # 공통합수 (1) 일자별 광고비, 세션수 (파생변수는 해당 함수가 계산하지 않음 -> 나중에 계산함)
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


    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    try: 
        creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
    except: # 배포용 (secrets.toml)
        sa_info = st.secrets["sleeper-462701-admin"]
        if isinstance(sa_info, str):  # 혹시 문자열(JSON)로 저장했을 경우
            sa_info = json.loads(sa_info)
        creds = Credentials.from_service_account_info(sa_info, scopes=scope)

    
    gc = gspread.authorize(creds)
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1chXeCek1UZPyCr18zLe7lV-8tmv6YPWK6cEqAJcDPqs/edit')
    df_order = pd.DataFrame(sh.worksheet('온오프라인_종합').get_all_records())
    df_order = df_order.rename(columns={"판매수량": "주문수"})  # 컬럼 이름 치환
    def convert_dot_date(x):
        try:
            # 1. 문자열로 변환 + 공백 제거
            s = str(x).replace(" ", "")
            # 2. 마침표 기준 split
            parts = s.split(".")
            if len(parts) == 3:
                y = parts[0]
                m = parts[1].zfill(2)
                d = parts[2].zfill(2)
                return pd.to_datetime(f"{y}-{m}-{d}", format="%Y-%m-%d", errors="coerce")
            return pd.NaT
        except:
            return pd.NaT
    df_order["주문일"] = pd.to_datetime(df_order["주문일"].apply(convert_dot_date), format="%Y-%m-%d", errors="coerce")
    df_order["실결제금액"] = pd.to_numeric(df_order["실결제금액"], errors='coerce')
    df_order["주문수"] = pd.to_numeric(df_order["주문수"], errors='coerce')
    df_order = df_order.dropna(subset=["주문일"])


    # 공통합수 (2) 일자별 매출, 주문수 (파생변수는 해당 함수가 계산하지 않음)
    def pivot_ord(
        df: pd.DataFrame,
        brand_type: str | None = None,
        product_type: str | None = None
        ) -> pd.DataFrame:
        """
        1) 함수 작성
        :  pivot_ord(df, brand_type="슬립퍼", product_type="매트리스")
        2) 결과 컬럼
        :  ['주문일', 'ord_amount_sum', 'ord_count_sum']
        
        """
        df_f = df.copy()

        if brand_type:
            df_f = df_f[df_f['브랜드'].astype(str).str.contains(brand_type, regex=True, na=False)]
        if product_type:
            df_f = df_f[df_f['카테고리'].astype(str).str.contains(product_type, regex=True, na=False)]
            
        df_f['주문일'] = pd.to_datetime(df_f['주문일'], errors='coerce')
        df_f['주문일'] = df_f['주문일'].dt.strftime('%Y-%m-%d')

        pivot = (
            df_f
            .groupby('주문일', as_index=False) # 반드시 False로 유지 (그래야 컬럼에 살아있음)
            .agg(
                ord_amount_sum=('실결제금액', 'sum'),
                ord_count_sum=('주문수', 'sum')
            )
            .reset_index(drop=True)
        )
        return pivot


    # ────────────────────────────────────────────────────────────────
    # 데이터프레임 생성 (JOIN인 경우는 고의로 "주문일" 컬럼 떨굴 목적)
    # ────────────────────────────────────────────────────────────────
    # 1-1) 슬립퍼
    _sctSes_slp      = pivot_cstSes(df_merged, brand_type="슬립퍼")
    _ord_slp         = pivot_ord(df_order,     brand_type="슬립퍼")
    df_slp           = _sctSes_slp.join(_ord_slp.set_index('주문일'), on='event_date', how='left')
    
    # 1-2) 슬립퍼 & 매트리스
    _sctSes_slp_mat  = pivot_cstSes(df_merged, brand_type="슬립퍼", product_type="매트리스")
    _ord_slp_mat     = pivot_ord(df_order,     brand_type="슬립퍼", product_type="매트리스")
    df_slp_mat       = _sctSes_slp_mat.join(_ord_slp_mat.set_index('주문일'), on='event_date', how='left')
    
    # 1-3) 슬립퍼 & 프레임
    _sctSes_slp_frm  = pivot_cstSes(df_merged, brand_type="슬립퍼", product_type="프레임")
    _ord_slp_frm     = pivot_ord(df_order,     brand_type="슬립퍼", product_type="프레임")
    df_slp_frm       = _sctSes_slp_frm.join(_ord_slp_frm.set_index('주문일'), on='event_date', how='left')
    
    # 2-1) 누어 
    _sctSes_nor      = pivot_cstSes(df_merged, brand_type="누어")
    _ord_nor         = pivot_ord(df_order,     brand_type="누어")
    df_nor           = _sctSes_nor.join(_ord_nor.set_index('주문일'), on='event_date', how='left')
    
    # 2-2) 누어 & 매트리스
    _sctSes_nor_mat  = pivot_cstSes(df_merged, brand_type="누어", product_type="매트리스")
    _ord_nor_mat     = pivot_ord(df_order,     brand_type="누어", product_type="매트리스")
    df_nor_mat       = _sctSes_nor_mat.join(_ord_nor_mat.set_index('주문일'), on='event_date', how='left')
    
    # 2-3) 누어 & 프레임
    _sctSes_nor_frm  = pivot_cstSes(df_merged, brand_type="누어", product_type="프레임")
    _ord_nor_frm     = pivot_ord(df_order,     brand_type="누어", product_type="프레임")
    df_nor_frm       = _sctSes_nor_frm.join(_ord_nor_frm.set_index('주문일'), on='event_date', how='left')
    
    # 3) 통합 데이터 (3번 이지만, 위치상 최상위에 위치함 주의)
    _df_total_psi    = df_psi  # 이미 날짜별로 세션수가 피벗되어 있는 데이터프레임
    _df_total_cost   = df_merged.groupby('event_date', as_index=False).agg(cost_gross_sum=('cost_gross','sum')).sort_values('event_date')  # df_merged에서 cost_gross만 가져옴
    _df_total_order  = df_order.groupby('주문일', as_index=False).agg(ord_amount_sum=('실결제금액','sum'), ord_count_sum  =('주문수', 'sum')).sort_values('주문일')
    _df_total_order  = _df_total_order.rename(columns={'주문일':'event_date'}) # 주문일 -> event_date
    df_total = (_df_total_psi
                .merge(_df_total_cost,  on='event_date', how='left')
                .merge(_df_total_order, on='event_date', how='left')
                )

    
    # 모든 데이터프레임이 동일한 파생 지표를 가짐
    def decorate_df(df: pd.DataFrame) -> pd.DataFrame:
        # 키에러 방지
        required = ["event_date", "ord_amount_sum", "ord_count_sum", "cost_gross_sum", "session_count"]
        for c in required:
            if c not in df.columns:
                df[c] = 0  
        num_cols = ["ord_amount_sum", "ord_count_sum", "cost_gross_sum", "session_count"]
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
            
        # 파생지표 생성
        df['CVR']    =  (df['ord_count_sum']  / df['session_count']  * 100).round(2)
        df['AOV']    =  (df['ord_amount_sum'] / df['ord_count_sum']  ).round(0)
        df['ROAS']   =  (df['ord_amount_sum'] / df['cost_gross_sum'] * 100).round(2)
        
        # 컬럼 순서 지정
        df = df[['event_date','ord_amount_sum','ord_count_sum','AOV','cost_gross_sum','ROAS','session_count','CVR']]
        
        # 자료형 워싱
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        num_cols = df.select_dtypes(include=['number']).columns
        df[num_cols] = (df[num_cols].replace([np.inf, -np.inf], np.nan).fillna(0))        

        # 컬럼 이름 변경 - 단일 인덱스
        # rename_map = {
        #     "event_date":       "날짜",
        #     "ord_amount_sum":   "매출",
        #     "ord_count_sum":    "주문수",
        #     "AOV":              "AOV(평균주문금액)",
        #     "cost_gross_sum":   "광고비",
        #     "ROAS":             "ROAS(광고수익률)",
        #     "session_count":    "세션수",
        #     "CVR":              "CVR(전환율)",
        # }
        # apply_map = {k: v for k, v in rename_map.items() if k in df.columns}
        # df = df.rename(columns=apply_map)

        # 컬럼 이름 변경 - 멀티 인덱스
        df.columns = pd.MultiIndex.from_tuples([
            ("기본정보",      "날짜"),              # event_date
            ("COST",        "매출"),               # ord_amount_sum
            ("COST",        "주문수"),             # ord_count_sum
            ("COST",        "AOV(평균주문금액)"),    # AOV
            ("PERFORMANCE", "광고비"),              # cost_gross_sum
            ("PERFORMANCE", "ROAS(광고수익률)"),     # ROAS
            ("GA",          "세션수"),              # session_count
            ("GA",          "CVR(전환율)"),          # CVR
        ], names=["그룹","지표"])  # 상단 레벨 이름(옵션)        
        
        return df

    def render_style(target_df):
        styled = style_format(
            decorate_df(target_df),
            decimals_map={
                ("COST",        "매출"): 0,
                ("COST",        "주문수"): 0,
                ("COST",        "AOV(평균주문금액)"): 0,
                ("PERFORMANCE", "광고비"): 0,
                ("PERFORMANCE", "ROAS(광고수익률)"): 1,
                ("GA",          "세션수"): 0,
                ("GA",          "CVR(전환율)"): 2,
            },
            suffix_map={
                ("PERFORMANCE", "ROAS(광고수익률)"): " %",
                ("GA",          "CVR(전환율)"): " %",
        }
        )
        styled2 = style_cmap(
            styled,
            gradient_rules=[
                {"col": ("COST",         "매출"), "cmap":"OrRd", "vmax":200000000, "low":0.0, "high":0.3},
            ]
        )
        st.dataframe(styled2, use_container_width=True, height=388, hide_index=True)

    # def render_aggrid(
    #     df: pd.DataFrame,
    #     height: int = 323,
    #     use_parent: bool = True
    #     ) -> None:
    #     """
    #     use_parent: False / True
    #     """
    #     df2 = df.copy()
    #     df2.fillna(0, inplace=True)     # 값이 없는 경우 일단 0으로 치환
        
    #     # 전처리 영역 (파생지표 생성, 컬럼순서 지정)
    #     df2['CVR']  = (df2['ord_count_sum']  / df2['session_count']  * 100).round(2)
    #     df2['AOV']  = (df2['ord_amount_sum'] / df2['ord_count_sum']  ).round(0)
    #     df2['ROAS'] = (df2['ord_amount_sum'] / df2['cost_gross_sum'] * 100).round(2)
    #     df2 = df2[['event_date','ord_amount_sum','ord_count_sum','AOV','cost_gross_sum','ROAS','session_count','CVR']]
        
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

    #     # (필수함수) add_summary - deprecated !!
    #     # def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]): #'sum'|'avg'|'mid'
    #     #     summary: dict[str, float] = {}
    #     #     for col, op in agg_map.items():
    #     #         if op == 'sum':
    #     #             summary[col] = int(df[col].sum())
    #     #         elif op == 'avg':
    #     #             summary[col] = float(df[col].mean())
    #     #         elif op == 'mid':
    #     #             summary[col] = float(df[col].median())
    #     #         else:
    #     #             summary[col] = "-"  # 에러 발생시, "-"로 표기하고 raise error 하지 않음
                    
    #     #     grid_options['pinnedBottomRowData'] = [summary]
    #     #     return grid_options

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

    #     # date_col
    #     date_col = {
    #         "headerName": "날짜",
    #         "field": "event_date",
    #         "pinned": "left",
    #         "width": 100,
    #         "cellStyle": JsCode("params=>({textAlign:'left'})"),
    #         "sort": "desc"
    #     }

    #     # (use_parent) flat_cols
    #     flat_cols = [
    #         date_col,
    #         make_num_child("매출",   "ord_amount_sum"),
    #         make_num_child("주문수", "ord_count_sum"),
    #         make_num_child("AOV(평균주문금액)",    "AOV"),
    #         make_num_child("광고비", "cost_gross_sum"),
    #         make_num_child("ROAS(광고수익률)",   "ROAS", fmt_digits=2, suffix='%'),
    #         make_num_child("세션수", "session_count"),
    #         make_num_child("CVR(전환율)",    "CVR", fmt_digits=2, suffix='%'),
    #     ]

    #     # (use_parent) grouped_cols
    #     grouped_cols = [
    #         date_col,
    #         {
    #             "headerName": "COST",
    #             "children": [
    #                 make_num_child("매출",   "ord_amount_sum"),
    #                 make_num_child("주문수", "ord_count_sum"),
    #                 make_num_child("AOV(평균주문금액)",    "AOV"),
    #             ]
    #         },
    #         {
    #             "headerName": "PERP",
    #             "children": [
    #                 make_num_child("광고비", "cost_gross_sum"),
    #                 make_num_child("ROAS(광고수익률)",   "ROAS", fmt_digits=2, suffix='%'),
    #             ]
    #         },
    #         {
    #             "headerName": "GA",
    #             "children": [
    #                 make_num_child("세션수", "session_count"),
    #                 make_num_child("CVR(전환율)",    "CVR", fmt_digits=2, suffix='%'),
    #             ]
    #         },
    #     ]

    #     # (use_parent)
    #     column_defs = grouped_cols if use_parent else flat_cols
        
    #     # grid_options & 렌더링
    #     grid_options = {
    #         "columnDefs": column_defs,
    #         "defaultColDef": {"sortable": True, "filter": True, "resizable": True},
    #         "headerHeight": 30,
    #         "groupHeaderHeight": 30,
    #     }

    #     # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
    #     grid_options = add_summary(
    #         grid_options,
    #         df2,
    #         {
    #             'ord_amount_sum': 'sum',
    #             'ord_count_sum' : 'sum',
    #             'AOV'           : 'avg',
    #             'cost_gross_sum': 'sum',
    #             'ROAS'          : 'avg',
    #             'session_count' : 'sum',
    #             'CVR'           : 'avg',
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



    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)


    # ────────────────────────────────────────────────────────────────
    # 통합 매출 리포트 (FF4B4B -> FF804B)
    # ────────────────────────────────────────────────────────────────
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>통합</span> 매출 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ날짜별 **COST**(매출), **PERFORMANCE**(광고비), **GA**(유입) 데이터를 표에서 확인할 수 있습니다.", unsafe_allow_html=True)
    
    render_style(df_total)


    # ────────────────────────────────────────────────────────────────
    # 슬립퍼 매출 리포트
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>슬립퍼</span> 매출 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ탭을 클릭하여, 품목별 데이터를 확인할 수 있습니다. ", unsafe_allow_html=True)

    tabs = st.tabs(["슬립퍼 통합", "슬립퍼 매트리스", "슬립퍼 프레임"])
    with tabs[0]:
        render_style(df_slp)
    with tabs[1]:
        render_style(df_slp_mat)
    with tabs[2]:
        render_style(df_slp_frm)


    # ────────────────────────────────────────────────────────────────
    # 누어 매출 리포트
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>누어</span> 매출 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ탭을 클릭하여, 품목별 데이터를 확인할 수 있습니다.", unsafe_allow_html=True)

    tabs = st.tabs(["누어 통합", "누어 매트리스", "누어 프레임"])
    with tabs[0]:
        render_style(df_nor)
    with tabs[1]:
        render_style(df_nor_mat)
    with tabs[2]:
        render_style(df_nor_frm)


    # ────────────────────────────────────────────────────────────────
    # 시각화 차트 (나중에 시각화 영역 따로 추가할 거 같아서 주석처리함 // 08.19)
    # ────────────────────────────────────────────────────────────────
    # st.header(" ")
    # st.markdown("<h5 style='margin:0'>리포트 시각화</h5>", unsafe_allow_html=True)
    # st.markdown(
    #     ":gray-badge[:material/Info: Info]ㅤ리포트, 지표, 차트 옵션을 자유롭게 선택하여, 원하는 방식으로 데이터를 살펴보세요.",
    #     unsafe_allow_html=True,
    # )

    # dfs = {
    #     "통합 리포트":    df_total,
    #     "슬립퍼 통합":    df_slp,
    #     "슬립퍼 매트리스": df_slp_mat,
    #     "슬립퍼 프레임":   df_slp_frm,
    #     "누어 통합":     df_nor,
    #     "누어 매트리스":  df_nor_mat,
    #     "누어 프레임":    df_nor_frm,
    # }
    
    # metrics = ["매출","주문수","AOV","광고비","ROAS","세션수","CVR"]
    
    # col_map = {
    #     "매출":   "ord_amount_sum",
    #     "주문수": "ord_count_sum",
    #     "AOV":    "AOV",
    #     "광고비": "cost_gross_sum",
    #     "ROAS":   "ROAS",
    #     "세션수": "session_count",
    #     "CVR":    "CVR"
    # }

    # default_yaxis = {
    #     "매출": "left",
    #     "주문수": "left",
    #     "AOV": "left",
    #     "광고비": "left",
    #     "ROAS": "right",
    #     "세션수": "left",
    #     "CVR": "right"
    # }
    # default_chart = {
    #     "매출": "bar",
    #     "주문수": "bar",
    #     "AOV": "line",
    #     "광고비": "bar",
    #     "ROAS": "line",
    #     "세션수": "bar",
    #     "CVR": "line"
    # }


    # # ── 1) 선택 UI
    # c_report, c_metric = st.columns([3, 7])
    # with c_report:
    #     sel_report = st.selectbox("리포트 선택", list(dfs.keys()), key="select_report")
    # with c_metric:
    #     sel_metrics = st.multiselect("지표 선택", metrics, default=["AOV", "ROAS"], key="select_metrics")

    # # ── 2) 컬럼별 옵션 선택 UI (표 형태)
    # with st.expander("지표별 옵션 선택", expanded=False):

    #     metric_settings = {}
    #     for i, metric in enumerate(sel_metrics):
    #         c2, c3 = st.columns([2, 2])
    #         with c2:
    #             yaxis = st.selectbox(
    #                 f"Y축 위치: {metric}", ["왼쪽", "오른쪽"],
    #                 key=f"y_axis_{metric}_{i}",
    #                 index=0 if default_yaxis[metric] == "left" else 1
    #             )
    #         with c3:
    #             chart_type = st.selectbox(
    #                 f"차트 유형: {metric}", ["꺾은선", "막대"],
    #                 key=f"chart_type_{metric}_{i}",
    #                 index=0 if default_chart[metric] == "line" else 1
    #             )
    #         metric_settings[metric] = {
    #             "yaxis": "right" if yaxis == "오른쪽" else "left",
    #             "chart": "bar" if chart_type == "막대" else "line"
    #         }

    # # ── 3) 차트 로직
    # if not sel_metrics:
    #     st.warning("하나 이상의 지표를 선택해주세요.")
    # else:
    #     df = dfs[sel_report].sort_values("event_date").copy()
    #     # 파생지표 생성 (수식이 필요한 항목만)
    #     df["AOV"]  = df["ord_amount_sum"] / df["ord_count_sum"]
    #     df["ROAS"] = df["ord_amount_sum"] / df["cost_gross_sum"] * 100
    #     df["CVR"]  = df["ord_count_sum"]  / df["session_count"] * 100

    #     fig = make_subplots(specs=[[{"secondary_y": True}]])

    #     for metric in sel_metrics:
    #         col = col_map[metric]
    #         y_axis = metric_settings[metric]["yaxis"] == "right"
    #         chart_type = metric_settings[metric]["chart"]

    #         if chart_type == "bar":
    #             fig.add_trace(
    #                 go.Bar(
    #                     x=df["event_date"],
    #                     y=df[col],
    #                     name=metric,
    #                     opacity=0.5,
    #                     # width=0.9
    #                 ),
    #                 secondary_y=y_axis
    #             )
    #         else:  # 꺾은선
    #             fig.add_trace(
    #                 go.Scatter(
    #                     x=df["event_date"],
    #                     y=df[col],
    #                     name=metric,
    #                     mode="lines+markers"
    #                 ),
    #                 secondary_y=y_axis
    #             )

    #     left_titles  = [m for m in sel_metrics if metric_settings[m]["yaxis"]=="left"]
    #     right_titles = [m for m in sel_metrics if metric_settings[m]["yaxis"]=="right"]
    #     left_title  = " · ".join(left_titles)  if left_titles  else None
    #     right_title = " · ".join(right_titles) if right_titles else None

    #     fig.update_layout(
    #         title=f"{sel_report}  -  {' / '.join(sel_metrics)} 추이",
    #         xaxis=dict(tickformat="%m월 %d일"),
    #         legend=dict(
    #             orientation="h",
    #             x=1, y=1.1, xanchor="right", yanchor="bottom"
    #         ),
    #         margin=dict(t=80, b=20, l=20, r=20)
    #     )
    #     if left_title:
    #         fig.update_yaxes(title_text=left_title, secondary_y=False)
    #     if right_title:
    #         fig.update_yaxes(title_text=right_title, secondary_y=True)

    #     st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()