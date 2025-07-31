# 서희_최신수정일_25-07-24

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


def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정
    # ──────────────────────────────────
    st.set_page_config(layout="wide", page_title="SLPR | 매출 종합 대시보드")
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
    # st.markdown("설명")
    st.markdown("""
    이 대시보드는 **매출 · 광고비 · 유입** 데이터를 일자별로 한눈에 보여주는 **가장 개괄적인 대시보드**입니다.  
    여기서는 일자/브랜드/품목별로 “**얼마 벌었고, 얼마 썼고, 얼마 유입됐고**”를 효율 지표(AOV, ROAS, CVR)와 함께 확인할 수 있습니다.
    """)
    # st.markdown(":primary-badge[:material/Cached: Update]ㅤ설명.")
    st.markdown(
        '<a href="https://www.notion.so/Views-241521e07c7680df86eecf5c5f8da4af#241521e07c76805198d9eaf0c28deadb" target="_blank">'
        'Dashboard Guide</a>',
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
    df_merged, df_psi = load_data(cs, ce)

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


    # df_order
    
    # dates = pd.date_range("2025-07-01", "2025-07-21")
    # n = 200
    # df_order = pd.DataFrame({
    #     "주문일": np.random.choice(dates, size=n),
    #     "실결제금액": np.random.randint(100_000, 1_000_001, size=n),
    #     "카테고리": np.random.choice(["매트리스", "프레임"], size=n),
    #     "브랜드": np.random.choice(["슬립퍼", "누어"], size=n),
    #     "주문수": np.random.randint(1, 4, size=n)  # 1~3 랜덤
    # })
    
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
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
    df_slp           = _sctSes_slp.join(_ord_slp.set_index('주문일'), on='event_date', how='left') # df_slp = _sctSes_slp.join(_ord_slp, how='left', left_on='event_date', right_on='주문일')
    
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
    df_total['event_date'] = pd.to_datetime(df_total['event_date'], errors='coerce').dt.strftime('%Y-%m-%d')


    # ────────────────────────────────────────────────────────────────
    # 시각화
    # ────────────────────────────────────────────────────────────────
    # 공통함수 (3) render_aggrid 
    def render_aggrid(
        df: pd.DataFrame,
        height: int = 410,
        use_parent: bool = True
        ) -> None:
        """
        use_parent: False / True
        """
        df2 = df.copy()
        df2.fillna(0, inplace=True)     # 값이 없는 경우 일단 0으로 치환
        
        # 전처리 영역 (파생지표 생성, 컬럼순서 지정)
        df2['CVR']  = (df2['ord_count_sum']  / df2['session_count']  * 100).round(2)
        df2['AOV']  = (df2['ord_amount_sum'] / df2['ord_count_sum']  ).round(0)
        df2['ROAS'] = (df2['ord_amount_sum'] / df2['cost_gross_sum'] * 100).round(2)
        df2 = df2[['event_date','ord_amount_sum','ord_count_sum','AOV','cost_gross_sum','ROAS','session_count','CVR']]
        
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

        # (필수함수) add_summary - deprecated !!
        # def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]): #'sum'|'avg'|'mid'
        #     summary: dict[str, float] = {}
        #     for col, op in agg_map.items():
        #         if op == 'sum':
        #             summary[col] = int(df[col].sum())
        #         elif op == 'avg':
        #             summary[col] = float(df[col].mean())
        #         elif op == 'mid':
        #             summary[col] = float(df[col].median())
        #         else:
        #             summary[col] = "-"  # 에러 발생시, "-"로 표기하고 raise error 하지 않음
                    
        #     grid_options['pinnedBottomRowData'] = [summary]
        #     return grid_options

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

        # date_col
        date_col = {
            "headerName": "날짜",
            "field": "event_date",
            "pinned": "left",
            "width": 100,
            "cellStyle": JsCode("params=>({textAlign:'left'})")
        }

        # (use_parent) flat_cols
        flat_cols = [
            date_col,
            make_num_child("매출",   "ord_amount_sum"),
            make_num_child("주문수", "ord_count_sum"),
            make_num_child("AOV",    "AOV"),
            make_num_child("광고비", "cost_gross_sum"),
            make_num_child("ROAS",   "ROAS", fmt_digits=2, suffix='%'),
            make_num_child("세션수", "session_count"),
            make_num_child("CVR",    "CVR", fmt_digits=2, suffix='%'),
        ]

        # (use_parent) grouped_cols
        grouped_cols = [
            date_col,
            {
                "headerName": "COST",
                "children": [
                    make_num_child("매출",   "ord_amount_sum"),
                    make_num_child("주문수", "ord_count_sum"),
                    make_num_child("AOV",    "AOV"),
                ]
            },
            {
                "headerName": "PERP",
                "children": [
                    make_num_child("광고비", "cost_gross_sum"),
                    make_num_child("ROAS",   "ROAS", fmt_digits=2, suffix='%'),
                ]
            },
            {
                "headerName": "GA",
                "children": [
                    make_num_child("세션수", "session_count"),
                    make_num_child("CVR",    "CVR", fmt_digits=2, suffix='%'),
                ]
            },
        ]

        # (use_parent)
        column_defs = grouped_cols if use_parent else flat_cols
        
        # grid_options & 렌더링
        grid_options = {
            "columnDefs": column_defs,
            "defaultColDef": {"sortable": True, "filter": True, "resizable": True},
            "headerHeight": 30,
            "groupHeaderHeight": 30,
        }

        # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
        grid_options = add_summary(
            grid_options,
            df2,
            {
                'ord_amount_sum': 'sum',
                'ord_count_sum' : 'sum',
                'AOV'           : 'avg',
                'cost_gross_sum': 'sum',
                'ROAS'          : 'avg',
                'session_count' : 'sum',
                'CVR'           : 'avg',
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


    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)


    # 1) 통합 영역 (탭 X)
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>통합</span> 매출 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    render_aggrid(df_total)
    
    # 2) 슬립퍼 영역 (탭 구성)
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>슬립퍼</span> 매출 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    tabs = st.tabs(["슬립퍼 통합", "슬립퍼 매트리스", "슬립퍼 프레임"])
    with tabs[0]:
        render_aggrid(df_slp)
    with tabs[1]:
        render_aggrid(df_slp_mat)
    with tabs[2]:
        render_aggrid(df_slp_frm)

    # 3) 누어 영역 (탭 구성)
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>누어</span> 매출 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    tabs = st.tabs(["누어 통합", "누어 매트리스", "누어 프레임"])
    with tabs[0]:
        render_aggrid(df_nor)
    with tabs[1]:
        render_aggrid(df_nor_mat)
    with tabs[2]:
        render_aggrid(df_nor_frm)


    # ────────────────────────────────────────────────────────────────
    # 시각화 차트
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'>리포트 시각화</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)
    dfs = {
        "통합 리포트":    df_total,
        "슬립퍼 통합":    df_slp,
        "슬립퍼 매트리스": df_slp_mat,
        "슬립퍼 프레임":   df_slp_frm,
        "누어 통합":     df_nor,
        "누어 매트리스":  df_nor_mat,
        "누어 프레임":    df_nor_frm,
    }
    metrics = ["매출","주문수","AOV","광고비","ROAS","세션수","CVR"]
    col_map = {
        "매출":   "ord_amount_sum",
        "주문수": "ord_count_sum",
        "AOV":    "AOV",
        "광고비": "cost_gross_sum",
        "ROAS":   "ROAS",
        "세션수": "session_count",
        "CVR":    "CVR"
    }
    left_labels  = {"매출","주문수","AOV","광고비","세션수"}
    right_labels = {"ROAS","CVR"}

    # 1) 선택 UI: 좌우 3:7
    col1, col2 = st.columns([3, 7])
    with col1:
        df_key = st.selectbox("리포트 선택", list(dfs.keys()))
    with col2:
        sel = st.multiselect("컬럼 선택", metrics, default=["AOV", "ROAS"])

    # 2) 차트 로직
    if not sel:
        st.warning("하나 이상의 컬럼을 선택해주세요.")
    else:
        df_sel   = dfs[df_key].sort_values("event_date")
        df_chart = df_sel.assign(
            AOV  = lambda x: x.ord_amount_sum / x.ord_count_sum,
            ROAS = lambda x: x.ord_amount_sum / x.cost_gross_sum * 100,
            CVR  = lambda x: x.ord_count_sum  / x.session_count   * 100,
        )

        fig = make_subplots(specs=[[{"secondary_y": True}]])

        for m in sel:
            col = col_map[m]
            is_right = (m in right_labels)
            fig.add_trace(
                go.Scatter(
                    x=df_chart["event_date"],
                    y=df_chart[col],
                    name=m,
                    mode="lines+markers",
                    line=dict(dash="dash" if is_right else "solid")
                ),
                secondary_y=is_right
            )

        # 3) 레이아웃
        fig.update_layout(
            title=f"{df_key}  -  {' / '.join(sel)} 추이",
            # xaxis_title="날짜",
            xaxis=dict(tickformat="%m월 %d일"),
            legend=dict(
                orientation="h",
                x=1, y=1.1,
                xanchor="right",
                yanchor="bottom"
            ),
            margin=dict(t=100, b=20, l=20, r=20)
        )
        left_title  = "·".join([m for m in sel if m in left_labels])
        right_title = "·".join([m for m in sel if m in right_labels])
        if left_title:
            fig.update_yaxes(title_text=left_title, secondary_y=False)
        if right_title:
            fig.update_yaxes(title_text=right_title, secondary_y=True)

        st.plotly_chart(fig, use_container_width=True)



if __name__ == "__main__":
    main()