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

import modules.ui_common as ui
importlib.reload(ui)
from pandas.tseries.offsets import MonthEnd
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
    # 탭 간격 CSS
    st.markdown("""
        <style>
            [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)


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

        df_bq["campaign_name_short"] = (
            df_bq["campaign_name"]
            .astype(str)
            .str.split("_")
            .apply(lambda x: "_".join(x[:5]))
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
        
        last_updated_time__media = df_bq["event_date"].max()
        last_updated_time__GA    = df_psi["event_date"].max()


        return merged, df_psi, last_updated_time__media, last_updated_time__GA


    # PROGRESS BAR
    spacer_placeholder = st.empty()
    progress_placeholder = st.empty()

    spacer_placeholder.markdown("<br>", unsafe_allow_html=True)
    progress_bar = progress_placeholder.progress(0, text="데이터베이스 연결 확인 중입니다...")
    
    import time
    time.sleep(0.2)
    
    for i in range(1, 80, 5):
        progress_bar.progress(i, text=f"데이터를 불러오고 있습니다...{i}%")
        time.sleep(0.1)
    
    df_merged, df_psi, last_updated_time__media, last_updated_time__GA = load_data(cs, ce)
    
    progress_bar.progress(95, text="데이터 분석 및 시각화를 구성 중입니다...")
    time.sleep(0.4)
    
    progress_bar.progress(100, text="데이터 로드 완료!")
    time.sleep(0.6)

    progress_placeholder.empty()
    spacer_placeholder.empty()
    



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

    def summary_row(df):
        sum_row = df.iloc[:, 1:].sum(numeric_only=True)
        avg_row = df.iloc[:, 1:].mean(numeric_only=True)

        # 합계/평균 행 DataFrame
        summary = pd.DataFrame([
            ["합계"] + sum_row.astype(int).tolist(),
            ["평균"] + avg_row.round(1).tolist()
        ], columns=df.columns)
        
        return summary



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
        
        # 합계 & 평균 행 추가
        sum_row = df[num_cols].sum().to_frame().T
        sum_row['event_date'] = "합계"
        mean_row = df[num_cols].mean().to_frame().T
        mean_row['event_date'] = "평균"
        df = pd.concat([df, sum_row, mean_row], ignore_index=True)

        # 컬럼 이름 변경 - 멀티 인덱스
        df.columns = pd.MultiIndex.from_tuples([
            ("기본정보",      "날짜"),              # event_date
            ("COST",        "매출"),               # ord_amount_sum
            ("COST",        "주문수"),             # ord_count_sum
            ("COST",        "AOV"),    # AOV
            ("MEDIA", "광고비"),              # cost_gross_sum
            ("MEDIA", "ROAS"),     # ROAS
            ("GA",          "세션수"),              # session_count
            ("GA",          "CVR"),          # CVR
        ], names=["그룹","지표"])  # 상단 레벨 이름(옵션)        
        
        return df

    def render_style(target_df):
        styled = style_format(
            decorate_df(target_df),
            decimals_map={
                ("COST",        "매출"): 0,
                ("COST",        "주문수"): 0,
                ("COST",        "AOV"): 0,
                ("MEDIA", "광고비"): 0,
                ("MEDIA", "ROAS"): 1,
                ("GA",          "세션수"): 0,
                ("GA",          "CVR"): 1,
            },
            suffix_map={
                ("MEDIA", "ROAS"): " %",
                ("GA",          "CVR"): " %",
        }
        )
        styled2 = style_cmap(
            styled,
            gradient_rules=[
                {"col": ("COST",   "매출"), "cmap":"Greens", "low":0.0, "high":0.3},
                {"col": ("MEDIA", "광고비"), "cmap":"Blues", "low":0.0, "high":0.3},
                {"col": ("GA",          "세션수"), "cmap":"OrRd",  "low":0.0, "high":0.3},

            ],
        )
        st.dataframe(styled2, use_container_width=True, hide_index=True, row_height=30)



    # (25.11.10) 제목 + 설명 + 업데이트 시각 + 캐시초기화 
    # last_updated_time__media, last_updated_time__GA
    # 제목
    st.subheader("매출 종합 대시보드")

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
            <b>매출 · 매체 · 유입</b> 데이터를 효율 지표와 함께 한눈에 확인하는 
            <b>가장 개괄적인 대시보드</b>입니다.<br>
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
        # last_updated_time__media -> lut_media
        if isinstance(last_updated_time__media, str):
            lut_m = datetime.strptime(last_updated_time__media, "%Y%m%d")
        else:
            lut_m = last_updated_time__media
        lut_media = lut_m.date()
        
        # last_updated_time__GA -> lut_ga
        if isinstance(last_updated_time__GA, str):
            lut_g = datetime.strptime(last_updated_time__GA, "%Y%m%d")
        else:
            lut_g = last_updated_time__GA
        lut_ga = lut_g.date()
        
        now_kst   = datetime.now(ZoneInfo("Asia/Seoul"))
        today_kst = now_kst.date()
        delta_days__lut_media = (today_kst - lut_media).days
        delta_days__lut_ga    = (today_kst - lut_ga).days

        
        # lut_media 기본값
        # msg__lut_media = f"{lut_media.strftime('%m월 %d일')} (D-{delta_days__lut_media})"
        msg__lut_media    = f"D-{delta_days__lut_media} 업데이트 완료"
        sub_bg__lut_media = "#fff7ed"
        sub_bd__lut_media = "#fdba74"
        sub_fg__lut_media = "#c2410c" 
        
        # # lut_ga 기본값
        # # msg__lut_ga    = f"{lut_ga.strftime('%m월 %d일')} (D-{delta_days__lut_ga})"
        # msg__lut_ga    = f"D-{delta_days__lut_ga} 업데이트 완료"
        # sub_bg__lut_ga = "#fff7ed"
        # sub_bd__lut_ga = "#fdba74"
        # sub_fg__lut_ga = "#c2410c"

        
        
        # 렌더링
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
            <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 10px;
                font-size:13px;line-height:1.1;
                color:{sub_fg__lut_media};background:{sub_bg__lut_media};border:1px solid {sub_bd__lut_media};
                border-radius:10px;white-space:nowrap;">
                📢 {msg__lut_media}
            </span>
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

    # st.markdown("""
    # <div style="
    #     background-color:#F0F2F6;
    #     border-radius:8px;
    #     padding:12px 16px;
    #     color:#333;
    #     font-size:0.9rem;
    #     line-height:1.5;
    # ">
    # ✅ <b>정보</b><br>
    # MEDIA 데이터는 
    # </div>
    # """, unsafe_allow_html=True)
    

    # ────────────────────────────────────────────────────────────────
    # 시각화 (신규 - 기간 조정 개별~)
    # ────────────────────────────────────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>시계열 분석</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ주요 매출 지표의 추이를 스무딩 기법으로 정제해, 단기 변동 대신 핵심 흐름을 시각화합니다.", unsafe_allow_html=True)

    with st.expander("스무딩은 시계열 분석에서 노이즈를 제거하고 추세를 도출하는 방법론입니다. ", expanded=False):
        st.markdown("""
    - **MA (이동평균)** : 기본 스무딩, 최근 S일 평균으로 요동을 눌러 큰 흐름만 보이게 합니다.
    - **EWMA (지수가중 이동평균)** : 가중 스무딩, 최근 값에 더 큰 가중치를 주어 변화에 민감합니다.
    - **STL 분해** : 주기성(Seasonal)을 제거하고, 이상/극단치의 영향을 적게 받는 방식으로, 순수 추세만 보여줍니다.
    - **Seasonally Adjusted** : 주기성(Seasonal)만 제거하고, 이벤트나 프로모션 등의 잔차는 남겨, 순수 변화량 추세를 보여줍니다.
    """)

    #  날짜 로드
    _today = pd.Timestamp.today().normalize()
    _chart_end = (_today - pd.Timedelta(days=1)).date()  # D-1

    cs_chart = "20250701"
    ce_chart = pd.Timestamp(_chart_end).strftime("%Y%m%d")

    # 이후 로직 동일
    df_merged_chart, df_psi_chart, last_updated_time__media, last_updated_time__GA = load_data(cs_chart, ce_chart)

    # 기존 load_data(cs, ce)를 재사용하여 차트 전용 데이터 확보
    df_merged_chart, df_psi_chart, last_updated_time__media, last_updated_time__GA = load_data(cs_chart, ce_chart)

    # ── 1) 브랜드별 일자 피벗(차트용) – 보고서 쪽과 동일한 로직 간소 복제
    def _pivot_cstSes(df, brand_type=None, product_type=None):
        df_f = df.copy()
        if brand_type:
            df_f = df_f[df_f['brand_type'].astype(str).str.contains(brand_type, regex=True, na=False)]
        if product_type:
            df_f = df_f[df_f['product_type'].astype(str).str.contains(product_type, regex=True, na=False)]
        df_f['event_date'] = pd.to_datetime(df_f['event_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        return (df_f.groupby('event_date', as_index=False)
                    .agg(session_count=('pseudo_session_id','sum'),
                        cost_gross_sum=('cost_gross','sum'))
                    .reset_index(drop=True))

    def _pivot_ord(df, brand_type=None, product_type=None):
        df_f = df.copy()
        if brand_type:
            df_f = df_f[df_f['브랜드'].astype(str).str.contains(brand_type, regex=True, na=False)]
        if product_type:
            df_f = df_f[df_f['카테고리'].astype(str).str.contains(product_type, regex=True, na=False)]
        df_f['주문일'] = pd.to_datetime(df_f['주문일'], errors='coerce').dt.strftime('%Y-%m-%d')
        return (df_f.groupby('주문일', as_index=False)
                    .agg(ord_amount_sum=('실결제금액','sum'),
                        ord_count_sum=('주문수','sum'))
                    .reset_index(drop=True))

    # 차트용 데이터프레임 구성 (통합/슬립퍼/누어)
    _df_total_cost = (df_merged_chart.groupby('event_date', as_index=False)
                    .agg(cost_gross_sum=('cost_gross','sum'))
                    .sort_values('event_date'))
    _df_total_order = (df_order.groupby('주문일', as_index=False)
                    .agg(ord_amount_sum=('실결제금액','sum'), ord_count_sum=('주문수','sum'))
                    .sort_values('주문일')
                    .rename(columns={'주문일':'event_date'}))
    df_total_chart = (df_psi_chart
                    .merge(_df_total_cost,  on='event_date', how='left')
                    .merge(_df_total_order, on='event_date', how='left'))

    _s_slp  = _pivot_cstSes(df_merged_chart, brand_type="슬립퍼")
    _o_slp  = _pivot_ord(df_order,          brand_type="슬립퍼")
    df_slp_chart = _s_slp.join(_o_slp.set_index('주문일'), on='event_date', how='left')

    _s_nor  = _pivot_cstSes(df_merged_chart, brand_type="누어")
    _o_nor  = _pivot_ord(df_order,          brand_type="누어")
    df_nor_chart = _s_nor.join(_o_nor.set_index('주문일'), on='event_date', how='left')

    # ── 2) 컨트롤 (이 영역만 독립 키 사용)
    options = {"전체 통합": df_total_chart, "슬립퍼 통합": df_slp_chart, "누어 통합": df_nor_chart}
    c1, c2, c3, _p, c4 = st.columns([3,3,3,0.5,3])

    with c1:
        ds_name = st.selectbox("데이터 선택", list(options.keys()), index=0, key="ts_ds")
    df_ts = options[ds_name].copy()

    # 날짜 정규화
    DATE_CANDS = ['날짜','date','event_date']
    date_col = next((c for c in DATE_CANDS if c in df_ts.columns), None)
    if date_col is None:
        st.error("날짜 컬럼이 없어 시계열을 그릴 수 없습니다."); st.stop()
    df_ts[date_col] = pd.to_datetime(df_ts[date_col], errors='coerce')
    df_ts = df_ts.dropna(subset=[date_col]).sort_values(date_col)

    label_map = {
        'ord_amount_sum': '매출',
        'cost_gross_sum': '광고비',
        'session_count' : '세션수',
        'ord_count_sum' : '주문수',
    }
    metric_options = [k for k in ['ord_amount_sum','cost_gross_sum','session_count','ord_count_sum'] if k in df_ts.columns]
    with c2:
        metric = st.selectbox("지표 선택", metric_options, index=0, key="ts_metric",
                            format_func=lambda k: label_map.get(k, k))
    overlay_options = ["MA (이동평균)", "EWMA (지수가중 이동평균)", "STL 분해", "Seasonally Adjusted"]
    with c3:
        overlay = st.selectbox("스무딩 기법 선택", overlay_options, index=0, key="ts_overlay")
    with _p:
        pass
    with c4:
        period = st.radio("주기(S) 선택", [7, 14], horizontal=True, index=0, key="ts_period",
                        help="디폴트값인 7일을 권장합니다. 이 값은 이동평균 평활, 세로선 간격, 볼린저 밴드에 사용됩니다.")

    # ── 3) 월 단위 선택 슬라이더 (이 영역만 독립) — 기본: 최신 2개월
    date_min = pd.to_datetime(df_ts[date_col].min()).normalize()
    date_max = pd.to_datetime(df_ts[date_col].max()).normalize()
    if pd.isna(date_min) or pd.isna(date_max):
        st.warning("유효한 날짜 데이터가 없습니다."); st.stop()

    start_period = date_min.to_period("M")
    end_period   = date_max.to_period("M")
    month_options = [p.to_timestamp() for p in pd.period_range(start=start_period, end=end_period, freq="M")]

    # 옵션이 0/1개일 때 방어 (RangeError 예방)
    if len(month_options) == 0:
        st.warning("표시할 월 정보가 없습니다."); st.stop()
    elif len(month_options) == 1:
        start_sel = end_sel = month_options[0]
        st.select_slider("기간(월)", options=month_options, value=start_sel,
                        format_func=lambda x: x.strftime("%Y-%m"), key="ts_period_single")
    else:
        # 기본값: 마지막 두 달
        default_start, default_end = (month_options[-2], month_options[-1])
        start_sel, end_sel = st.select_slider(
            "기간(월)", options=month_options, value=(default_start, default_end),
            format_func=lambda x: x.strftime("%Y-%m"), key="ts_period_range"
        )
        if start_sel > end_sel:
            start_sel, end_sel = end_sel, start_sel

    period_start, period_end = start_sel, end_sel + MonthEnd(0)
    dfp = df_ts[(df_ts[date_col] >= period_start) & (df_ts[date_col] <= period_end)].copy()

    # ── 4) 시계열 계산 및 그리기 
    s = dfp.set_index(date_col)[metric].asfreq('D').fillna(0)
    if s.empty or s.dropna().shape[0] < 2:
        st.warning("선택한 기간에 표시할 데이터가 부족합니다. 기간을 넓혀주세요.")
    else:
        win = period
        y_ma = s.rolling(win, min_periods=1).mean() if overlay == "MA (이동평균)" else None

        y_trend = y_seas = y_sa = None
        if overlay in ("STL 분해", "Seasonally Adjusted"):
            try:
                from statsmodels.tsa.seasonal import STL
                stl_period = max(2, min(int(period), max(2, len(s)//2)))
                stl = STL(s, period=stl_period, robust=True).fit()
                y_trend, y_seas = stl.trend, stl.seasonal
            except Exception:
                key = np.arange(len(s)) % period
                y_seas  = s.groupby(key).transform('mean')
                y_trend = (s - y_seas).rolling(period, min_periods=1, center=True).mean()
            y_sa = (s - y_seas) if y_seas is not None else None

        y_ewma = s.ewm(halflife=period, adjust=False, min_periods=1).mean() if overlay == "EWMA (지수가중 이동평균)" else None

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(x=s.index, y=s, name="RAW", mode="lines+markers", line=dict(color="#666"), opacity=0.45),
            secondary_y=False
        )

        # Bollinger Bands (짧은 구간 방어)
        k = 2.0
        minp = int(min(period, max(2, len(s))))
        ma_bb = s.rolling(period, min_periods=minp).mean()
        sd_bb = s.rolling(period, min_periods=minp).std(ddof=0)
        bb_upper = ma_bb + k * sd_bb
        bb_lower = ma_bb - k * sd_bb

        fig.add_trace(go.Scatter(x=bb_upper.index, y=bb_upper, name="BB Upper", mode="lines", line=dict(width=1, color="#FFB6C1")), secondary_y=False)
        fig.add_trace(go.Scatter(x=bb_lower.index, y=bb_lower, name="BB Lower", mode="lines", line=dict(width=1, color="#ADD8E6"),
                                fill="tonexty", fillcolor="rgba(128,128,128,0.12)"), secondary_y=False)

        overlay_series = None
        if overlay == "MA (이동평균)" and y_ma is not None:
            overlay_series = y_ma
            fig.add_trace(go.Scatter(x=y_ma.index, y=y_ma, name=f"MA{win}", mode="lines", line=dict(color="#FF4B4B")), secondary_y=True)
        elif overlay == "STL 분해" and y_trend is not None:
            overlay_series = y_trend
            fig.add_trace(go.Scatter(x=y_trend.index, y=y_trend, name="STL 분해", mode="lines", line=dict(color="#FF4B4B")), secondary_y=True)
        elif overlay == "Seasonally Adjusted" and y_sa is not None:
            overlay_series = y_sa
            fig.add_trace(go.Scatter(x=y_sa.index, y=y_sa, name="Seasonally Adjusted", mode="lines", line=dict(color="#FF4B4B")), secondary_y=True)
        elif overlay == "EWMA (지수가중 이동평균)" and y_ewma is not None:
            overlay_series = y_ewma
            fig.add_trace(go.Scatter(x=y_ewma.index, y=y_ewma, name=f"EWMA(h={period})", mode="lines", line=dict(color="#FF4B4B")), secondary_y=True)

        # left_candidates = [s.dropna()]
        # if (bb_upper is not None) and (not bb_upper.dropna().empty): left_candidates.append(bb_upper.dropna())
        # if (bb_lower is not None) and (not bb_lower.dropna().empty): left_candidates.append(bb_lower.dropna())
        # left_all = pd.concat(left_candidates) if left_candidates else s.dropna()
        # right = overlay_series.dropna() if (overlay_series is not None) else None

        # if (not left_all.empty) and (right is not None) and (not right.empty):
        #     ymin = float(np.nanmin([left_all.min(), right.min()]))
        #     ymax = float(np.nanmax([left_all.max(), right.max()]))
        #     if (not np.isfinite(ymin)) or (not np.isfinite(ymax)) or (ymax <= ymin):
        #         pad = 1.0
        #         ymin = (ymin if np.isfinite(ymin) else 0.0) - pad
        #         ymax = (ymax if np.isfinite(ymax) else 0.0) + pad
        #     else:
        #         pad = (ymax - ymin) * 0.05
        #         ymin, ymax = ymin - pad, ymax + pad
        #     fig.update_yaxes(range=[ymin, ymax], secondary_y=False)
        #     fig.update_yaxes(range=[ymin, ymax], secondary_y=True)
        #     fig.update_yaxes(tickformat="~s", secondary_y=False)
        #     fig.update_yaxes(tickformat="~s", secondary_y=True)
        
        # ── 좌/우 축 범위 설정 (STL/SA는 우측 독립 스케일)
        left_candidates = [s.dropna()]
        if (bb_upper is not None) and (not bb_upper.dropna().empty):
            left_candidates.append(bb_upper.dropna())
        if (bb_lower is not None) and (not bb_lower.dropna().empty):
            left_candidates.append(bb_lower.dropna())
        left_all = pd.concat(left_candidates) if left_candidates else s.dropna()
        right = overlay_series.dropna() if (overlay_series is not None) else None

        def _minmax_with_pad(series_min, series_max, pad_ratio=0.05, fallback_pad=1.0):
            # 유효하지 않으면 None 반환 (자동 스케일)
            if (series_min is None) or (series_max is None):
                return None
            if (not np.isfinite(series_min)) or (not np.isfinite(series_max)):
                return None
            if series_max <= series_min:
                pad = fallback_pad
                return (series_min - pad, series_max + pad)
            pad = (series_max - series_min) * pad_ratio
            return (series_min - pad, series_max + pad)

        # 1) 좌측 축: RAW(+BB) 기준 범위 설정
        if not left_all.empty:
            lmin = float(left_all.min())
            lmax = float(left_all.max())
            lrange = _minmax_with_pad(lmin, lmax)
            if lrange is not None:
                fig.update_yaxes(range=list(lrange), secondary_y=False)
        fig.update_yaxes(tickformat="~s", secondary_y=False)

        # 2) 우측 축: 오버레이별 처리
        if (right is not None) and (not right.empty):
            rmin = float(right.min())
            rmax = float(right.max())

            # STL / Seasonally Adjusted → 우측 독립 스케일
            if overlay in ("STL 분해", "Seasonally Adjusted"):
                rrange = _minmax_with_pad(rmin, rmax)
                if rrange is not None:
                    fig.update_yaxes(range=list(rrange), secondary_y=True)
            else:
                # 그 외(MA/EWMA)는 좌측과 동일 범위(동기화)
                if not left_all.empty:
                    if lrange is not None:
                        fig.update_yaxes(range=list(lrange), secondary_y=True)

        fig.update_yaxes(tickformat="~s", secondary_y=True)


        # 주기별 세로선
        start_ts = pd.to_datetime(s.index.min()).normalize()
        end_ts   = pd.to_datetime(s.index.max()).normalize()
        offset_days = (6 - start_ts.weekday()) % 7   # 0=월 ... 6=일 → 첫 일요일
        first_sunday = start_ts + pd.Timedelta(days=offset_days)
        step = 7 if period == 7 else 14
        t = first_sunday
        while t <= end_ts:
            fig.add_vline(x=t, line_dash="dash", line_width=1, opacity=0.6, line_color="#8c8c8c")
            t += pd.Timedelta(days=step)

        fig.update_yaxes(title_text=f"{label_map.get(metric, metric)} · RAW / BB", secondary_y=False)
        overlay_title = {
            "MA (이동평균)": f"{label_map.get(metric, metric)} · MA{win}",
            "STL 분해": "STL 분해",
            "Seasonally Adjusted": "Seasonally Adjusted",
            "EWMA (지수가중 이동평균)": f"EWMA (halflife={period})"
        }[overlay]
        fig.update_yaxes(title_text=overlay_title, secondary_y=True)
        fig.update_yaxes(showgrid=False, zeroline=False)
        fig.update_layout(
            margin=dict(l=10, r=10, t=30, b=10),
            legend=dict(orientation="h", y=1.03, x=1, xanchor="right", yanchor="bottom", title=None),
        )
        st.plotly_chart(fig, use_container_width=True)







    # ────────────────────────────────────────────────────────────────
    # 통합 매출 리포트 
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'>통합 매출 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일자별 **통합** 데이터와 효율 추이를 확인할 수 있습니다. ", unsafe_allow_html=True)
    with st.popover("지표 설명"):
        st.markdown("""
                    - **AOV** (Average Order Value) : **평균주문금액** (매출 ÷ 주문수)  
                    - **ROAS** (Return On Ad Spend) : **광고 수익률** (매출 ÷ 광고비 × 100)  
                    - **CVR** (Conversion Rate) : **전환율** (주문수 ÷ 세션수 × 100)  
                    """)
    render_style(df_total)


    # ────────────────────────────────────────────────────────────────
    # 슬립퍼 매출 리포트
    # ────────────────────────────────────────────────────────────────
    st.header(" ") # 공백용
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>슬립퍼</span> 매출 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일자별 **품목** 데이터와 효율 추이를 확인할 수 있습니다. <span style='color:#8E9097;'>(15시 이후 D-1 데이터 제공)</span> ", unsafe_allow_html=True)
    with st.popover("지표 설명"):
        st.markdown("""
                    - **AOV** (Average Order Value) : **평균주문금액** (매출 ÷ 주문수)  
                    - **ROAS** (Return On Ad Spend) : **광고 수익률** (매출 ÷ 광고비 × 100)  
                    - **CVR** (Conversion Rate) : **전환율** (주문수 ÷ 세션수 × 100)  
                    """)

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
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일자별 **품목** 데이터와 효율 추이를 확인할 수 있습니다. <span style='color:#8E9097;'>(15시 이후 D-1 데이터 제공)</span> ", unsafe_allow_html=True)
    with st.popover("지표 설명"):
        st.markdown("""
                    - **AOV** (Average Order Value) : **평균주문금액** (매출 ÷ 주문수)  
                    - **ROAS** (Return On Ad Spend) : **광고 수익률** (매출 ÷ 광고비 × 100)  
                    - **CVR** (Conversion Rate) : **전환율** (주문수 ÷ 세션수 × 100)  
                    """)

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