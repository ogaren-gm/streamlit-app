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



def main():

    # ──────────────────────────────────
    # 스트림릿 페이지 설정
    # ──────────────────────────────────
    st.set_page_config(layout="wide", page_title="SLPR 대시보드 | 퍼포먼스 대시보드")
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
    st.markdown("설명")
    st.markdown(":primary-badge[:material/Cached: Update]ㅤD-1 데이터는 오전 중 예비 처리된 후, **15:00 이후** 매체 분류가 완료되어 최종 업데이트됩니다.")
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


    ## 추가
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
        creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
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
    # df_merged = load_data(cs, ce)
    # df_filtered = df_merged.copy()
    
    ## 수정
    if use_compare:
        # cs~ce, cs_cmp~ce_cmp 한 번에 로드
        cs_cmp = comp_start.strftime("%Y%m%d")
        df_merged = load_data(cs_cmp, ce)
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
        df_primary = df_merged
    
    df_filtered     = df_primary.copy()
    df_filtered_cmp = df_compare.copy() if use_compare else None


    def render_aggrid(
        df: pd.DataFrame,
        pivot_cols: list[str],
        height: int = 490,
        use_parent: bool = True
        ) -> None:
        """
        use_parent: False / True
        """
        df2 = df.copy()
        df2.fillna(0, inplace=True)
        df2 = df2.where(pd.notnull(df2), None)
        df2.replace([np.inf, -np.inf], 0, inplace=True)
        
        # 전처리 영역 (파생지표 생성)
        # df2['CPC'] = (df2['cost_gross_sum'] / df2['clicks_sum']).round(0)
        # df2['CTR'] = (df2['clicks_sum'] / df2['impressions_sum'] * 100).round(2)
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
        

        # (use_parent) flat_cols
        flat_cols = []

        # (use_parent) grouped_cols
        dynamic_cols = [ # (추가) pivot_cols (선택한 행필드)를 받아야함
            {
                "headerName": col,
                "field": col,
                "pinned": "left",
                "width": 100,
                "minWidth": 100,
                "flex": 1
            }
            for col in pivot_cols
        ]
        static_cols = [
            {
                "headerName": "MEDIA",
                "children": [
                    make_num_child("광고비",      "cost_sum"),
                    make_num_child("광고비(G)",   "cost_gross_sum"),
                    make_num_child("노출수",      "impressions_sum"),
                    make_num_child("클릭수",      "clicks_sum"),
                    make_num_child("CPC",        "CPC"),
                    make_num_child("CTR",        "CTR", fmt_digits=2, suffix="%"),
                ]
            },
            {
                "headerName": "GA & MEDIA",
                "children": [
                    make_num_child("세션수",         "session_count"),
                    make_num_child("세션 CPA",       "session_count_CPA"),                    
                    make_num_child("PDP조회",        "view_item_sum"),
                    make_num_child("PDP조회 CPA",    "view_item_CPA"),
                    make_num_child("PDPscr50",      "product_page_scroll_50_sum"),
                    make_num_child("PDPscr50 CPA",  "product_page_scroll_50_CPA"),
                    make_num_child("가격표시",       "product_option_price_sum"),
                    make_num_child("가격표시 CPA",   "product_option_price_CPA"),
                    make_num_child("쇼룸찾기",       "find_nearby_showroom_sum"),
                    make_num_child("쇼룸찾기 CPA",   "find_nearby_showroom_CPA"),
                    make_num_child("쇼룸10초",       "showroom_10s_sum"),
                    make_num_child("쇼룸10초 CPA",   "showroom_10s_CPA"),
                    make_num_child("장바구니",       "add_to_cart_sum"),
                    make_num_child("장바구니 CPA",   "add_to_cart_CPA"),
                    make_num_child("쇼룸예약",       "showroom_leads_sum"),
                    make_num_child("쇼룸예약 CPA",   "showroom_leads_CPA"),
                    make_num_child("구매하기 ",      "purchase_sum"),
                    make_num_child("구매하기 CPA",   "purchase_CPA"),
                    # make_num_child("",   ""),
                    # make_num_child("",   ""),
                ]
            },
        ]
    
        grouped_cols = dynamic_cols + static_cols
        
        # (use_parent)
        column_defs = grouped_cols if use_parent else flat_cols

        # grid_options & 렌더링
        grid_options = {
            "columnDefs": column_defs,
            "defaultColDef": {
                "sortable": True,
                "filter": True,
                "resizable": True,
                "minWidth": 100,
                "wrapHeaderText": True,
            },
            "headerHeight": 50,
            "groupHeaderHeight": 30,
            "autoHeaderHeight": True,
        }

        # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
        grid_options = add_summary(
            grid_options,
            df2,
            {
                'cost_sum': 'sum',
                'cost_gross_sum': 'sum',
                'impressions_sum': 'sum',
                'clicks_sum': 'sum',
                'CPC': 'avg',
                'CTR': 'avg',
                'session_count': 'sum',
                'session_count_CPA' : 'avg',
                'view_item_sum': 'sum',
                'view_item_CPA' : 'avg',
                'product_page_scroll_50_sum': 'sum',
                'product_page_scroll_50_CPA' : 'avg',
                'product_option_price_sum': 'sum',
                'product_option_price_CPA' : 'avg',
                'find_nearby_showroom_sum': 'sum',
                'find_nearby_showroom_CPA' : 'avg',
                'showroom_10s_sum': 'sum',
                'showroom_10s_CPA' : 'avg',
                'add_to_cart_sum': 'sum',
                'add_to_cart_CPA' : 'avg',
                'showroom_leads_sum': 'sum',
                'showroom_leads_CPA' : 'avg',
                'purchase_sum': 'sum',
                'purchase_CPA' : 'avg',
            }
        )

        AgGrid(
            df2,
            gridOptions=grid_options,
            height=height,
            fit_columns_on_grid_load=True,  # True면 전체넓이에서 균등분배 
            theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
            allow_unsafe_jscode=True
        )



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
            term = st.text_input(f"{column} 포함 필터", key=key)
            if term:
                df = df[df[column].str.contains(term, na=False)]
                if df_cmp is not None:
                    df_cmp = df_cmp[df_cmp[column].str.contains(term, na=False)]
        else:
            opts = sorted(df_primary[column].dropna().unique())
            sel  = st.multiselect(f"{column} 필터", opts, key=key)
            if sel:
                df = df[df[column].isin(sel)]
                if df_cmp is not None:
                    df_cmp = df_cmp[df_cmp[column].isin(sel)]
        return df, df_cmp


    # ──────────────────────────────────
    # 1. 퍼포먼스 커스텀 리포트
    # ──────────────────────────────────
    st.markdown("<h5>퍼포먼스 커스텀 리포트</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명")
    st.markdown(" ")


    # 피벗할 행 필드 선택
    pivot_cols = st.multiselect(
        "행 필드 선택",
        [   "event_date", 
            "media_name", "utm_source", "utm_medium", 
            "brand_type", "funnel_type", "product_type"
            "campaign_name", "adgroup_name", "ad_name", "keyword_name",
            "utm_content", "utm_term"
        ],
        default=["event_date"]
        )

    # 기간별 합계 보기 모드라면 event_date 는 무시
    if show_totals and "event_date" in pivot_cols:
        pivot_cols.remove("event_date")
        
    # 공통 서치필터 및 상세 서치필터 정렬
    with st.expander("✔ㅤ기본 멀티셀렉 필터", expanded=False):
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
    
    with st.expander("✔ㅤ고급 멀티셀렉 필터", expanded=False):
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

    st.subheader(" ")
    
    # 표 표시 영역
    if pivot_cols or show_totals:

        # (1) 기간별 합계 보기 모드
        if show_totals:
            # (A) period 태깅
            df_sel = df_filtered.copy()
            df_sel["period"] = "선택기간"
            if use_compare:
                df_cmp = df_filtered_cmp.copy()
                df_cmp["period"] = "비교기간"
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

            render_aggrid(df_pivot, group_keys)

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
            df_sel["period"] = "선택기간"
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
                df_cmp["period"] = "비교기간"
                df_pivot = pd.concat([df_sel, df_cmp], ignore_index=True)
            else:
                df_pivot = df_sel

            render_aggrid(df_pivot, ["period"] + pivot_cols)
            
    else:
        st.warning("피벗할 행 필드를 하나 이상 선택해 주세요.")




    # ──────────────────────────────────
    # 2. 일자별 광고비, 노출수, 클릭수, CTR, CPC 시각화 분석
    # ──────────────────────────────────
    st.divider()
    st.markdown("<h5>일자별 광고비, 노출수, 클릭수, CTR, CPC 분석</h5>", unsafe_allow_html=True)
    
    # 날짜 필터만 적용된 데이터 복사
    df_daily = df_filtered.copy()
    st.dataframe(df_pivot)