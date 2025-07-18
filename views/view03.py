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



def main():

    # ──────────────────────────────────
    # 스트림릿 페이지 설정 (반드시 최상단)
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


    # ──────────────────────────────────
    # 1. 캐시된 데이터 로더
    # ──────────────────────────────────
    @st.cache_data(ttl=3600)
    
    def load_data(cs: str, ce: str) -> pd.DataFrame:
        # 1) 빅쿼리
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df_bq = bq.get_data("tb_media")
        df_bq["event_date"] = pd.to_datetime(df_bq["event_date"], format="%Y%m%d")
        
        # campaign_name_short
        parts = df_bq['campaign_name'].str.split('_', n=5, expand=True)

        df_bq['campaign_name_short'] = df_bq['campaign_name']

        mask = parts[5].notna()
        df_bq.loc[mask, 'campaign_name_short'] = (
            parts.loc[mask, [0,1,2,3,4]]             # 0~4번 컬럼만 선택 (0~4 조각을 합쳐서 덮어쓰기)
                 .apply(lambda row: '_'.join(row.dropna().astype(str)), axis=1)
        )

        # 2) 스프레드시트
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        json_path = "C:/_code/auth/sleeper-461005-c74c5cd91818.json"
        
        # creds = ServiceAccountCredentials.from_json_keyfile_name(json_path, scope)
        # gc = gspread.authorize(creds)
        
        creds = Credentials.from_service_account_file(json_path, scopes=scope)
        gc = gspread.authorize(creds)
        
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/11ov-_o6Lv5HcuZo1QxrKOZnLtnxEKTiV78OFBZzVmWA/edit')
        ws = sh.worksheet('parse') # 시트 명
        records = ws.get_all_records()
        df_sheet = pd.DataFrame(records)
        
        # 3) 두 DF를 left join (campaign_name)
        merged = (
            df_bq
            .merge(
                df_sheet,
                how='left',
                left_on='campaign_name_short',    # bq 컬럼
                right_on='campaign_name_short'    # sheet 컬럼
            )
        )
        
        # 4) cost_gross 컬럼 추가
        merged['cost_gross'] = np.where(
            merged['media_name'].isin(['GOOGLE', 'META']),
            merged['cost'] * 1.1 / 0.98,
            merged['cost']
        )


        # 간결하고 효과적인 규칙
        cond = (
            (merged['media_name'] == 'NSA') &
            merged['utm_source'].isna() &
            merged['utm_medium'].isna() &
            merged['media_name_type'].isin(['RSA_AD', 'TEXT_45'])
        )
        merged.loc[cond, ['utm_source', 'utm_medium']] = ['naver', 'search-nonmatch']


        #5) 두번째 데이터 프레임 
        df_psi = bq.get_data("tb_sleeper_psi")
        df_psi["event_date"] = pd.to_datetime(df_psi["event_date"], format="%Y%m%d")
        
        df_daily_psi = (df_psi
            .groupby("event_date", as_index=False)
            .agg(psi_sum = ("pseudo_session_id", "nunique"))
            .sort_values("event_date")
        )
        
        df_cost = (
            merged
            .groupby("event_date", as_index=False)
            .agg(
                cost_sum       = ("cost",       "sum"),
                cost_gross_sum = ("cost_gross", "sum")
            )
            .sort_values("event_date")
        )

        # 3) 날짜 기준으로 병합
        df_daily = (
            df_daily_psi
            .merge(df_cost, on="event_date", how="left")
        )
        
        return merged, df_daily



    # ──────────────────────────────────
    # 2. 2주치 데이터 로드 (본문에서 기간 필터링)
    # ──────────────────────────────────
    today = datetime.now().date()
    default_end_sel   = today - timedelta(days=1)
    default_start_sel = default_end_sel - timedelta(days=6)
    default_end_cmp   = default_start_sel - timedelta(days=1)
    default_start_cmp = default_end_cmp - timedelta(days=6)

    # 로드 범위: 비교기간 포함
    load_start = default_start_cmp.strftime("%Y%m%d")
    load_end   = default_end_sel.strftime("%Y%m%d")
    df, df_daily = load_data(load_start, load_end)




    # ──────────────────────────────────
    # 3. 일자별 전체 GA데이터 기준의 CPA 표
    # ──────────────────────────────────
    df_daily = df_daily.copy()
    df_daily["CPA"] = df_daily.apply(
        lambda r: r["cost_gross_sum"] / r["psi_sum"] if r["psi_sum"] > 0 else 0,
        axis=1
    )
    df_daily["날짜_표시"] = df_daily["event_date"].dt.strftime("%m월 %d일")

    st.markdown("<h5 style='margin:0'>CPA 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일자별 **광고비(Gross) 당 전체 방문수** 현황을 확인할 수 있습니다.")
    # st.markdown(" ")

    col1, col2, col3 = st.columns([6.0, 0.2, 3.8])
    
    with col1:
        df_daily["날짜"] = df_daily["event_date"]
        df_daily["CPA"] = df_daily["cost_gross_sum"] / df_daily["psi_sum"]
        df_daily["날짜_표시"] = df_daily["날짜"].dt.strftime("%m월 %d일")
        fig = px.line(df_daily, x="날짜", y=["CPA"], markers=True, labels={"variable":""})
        for d in df_daily["날짜"]:
            start, end = d - timedelta(hours=12), d + timedelta(hours=12)
            color = "blue" if d.weekday()==5 else "red" if d.weekday()==6 else None
            if color:
                fig.add_vrect(x0=start, x1=end, fillcolor=color,
                            opacity=0.2, layer="below", line_width=0)
        fig.update_xaxes(tickvals=df_daily["날짜"], ticktext=df_daily["날짜_표시"])
        fig.update_yaxes(range=[500, df_daily["CPA"].max()+200]) # y 축 고정
        fig.update_layout(
            xaxis_title=None,
            yaxis_title=None,
            legend=dict(orientation="h", y=1.02, x=1,
                        xanchor="right", yanchor="bottom")
        )
        st.plotly_chart(fig, use_container_width=True)
        # CPA는 유지, 날짜 표시용 컬럼만 제거
        df_daily.drop(columns=["날짜", "날짜_표시"], inplace=True)


    with col2:
        pass
    
    with col3:
        st.markdown("")
        df_disp = df_daily.copy()
        df_disp["날짜"] = df_disp["event_date"].dt.strftime("%m월 %d일 (%a)")
        df_disp.rename(columns={
            "psi_sum": "방문수",
            "cost_sum": "광고비",
            "cost_gross_sum": "광고비(G)"
        }, inplace=True)

        # NA를 0으로 채우고 반올림
        for col in ["방문수", "광고비", "광고비(G)", "CPA"]:
            df_disp[col] = df_disp[col].fillna(0).round(0)

        # 파이썬 int로 변환
        for col in ["방문수", "광고비", "광고비(G)", "CPA"]:
            df_disp[col] = df_disp[col].apply(lambda x: int(x))

        table_cols = ["방문수", "광고비", "광고비(G)", "CPA"]
        df_grid = df_disp[["날짜"] + table_cols]

        bottom = {
            col: ("합계" if col == "날짜" else sum(df_grid[col]))
            for col in df_grid.columns
        }

        gb = GridOptionsBuilder.from_dataframe(df_grid)
        gb.configure_default_column(flex=1, sortable=True, filter=True)
        for col in table_cols:
            gb.configure_column(
                col,
                type=["numericColumn", "customNumericFormat"],
                valueFormatter=JsCode("""
                    function(params) {
                        return params.value.toLocaleString();
                    }
                """),
                cellStyle=JsCode("function(params){ return { textAlign:'right' }; }")
            )

        gb.configure_grid_options(pinnedBottomRowData=[bottom])
        gb.configure_grid_options(onGridReady=JsCode("""
            function(params) {
                params.api.sizeColumnsToFit();
            }
        """))
        grid_options = gb.build()

        base_theme = st.get_option("theme.base")
        ag_theme = "streamlit-dark" if base_theme == "dark" else "streamlit"

        AgGrid(
            df_grid,
            gridOptions=grid_options,
            height=380,
            theme=ag_theme,
            fit_columns_on_grid_load=True,  # 사이즈 콜백을 사용하므로 여기선 False 권장
            allow_unsafe_jscode=True
        )


    # ──────────────────────────────────
    # 4. 표 영역: 기간 비교 + PSI 집계
    # ──────────────────────────────────
    st.divider()
    st.markdown("<h5>퍼포먼스 리포트</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명")
    st.markdown(" ")


    # (B) 비교기간 사용 여부
    _x, _y = st.columns([1.61,10])
    with _x :
        use_compare = st.checkbox(
            "비교기간 사용",
            value=False, key="tbl_use_compare",
            help="비교기간 종료는 선택기간 시작 이전이어야 합니다.")
    with _y :
        # (D) 기간별 합계 보기 토글
        show_period = st.checkbox(
            "기간별 합계 보기",
            value=False,
            help="체크 시 날짜 필드를 무시하고 기간별 합계만 표시합니다."
        )

    _a, _b, _c = st.columns([2,2,3])
    with _a:
        # (A) 날짜 선택
        select_start, select_end = st.date_input(
            "선택기간",
            value=[default_start_sel, default_end_sel],
            key="tbl_select_period"
        )
    with _b:
        # (C) 비교기간 입력 (옵션)
        if use_compare:
            compare_start, compare_end = st.date_input(
                "비교기간 (이전기간)",
                value=[default_start_cmp, default_end_cmp],
                key="tbl_compare_period"
            )
            if compare_end >= select_start:
                st.error("비교기간 종료는 선택기간 시작 이전이어야 합니다.")
                compare_start = compare_end = None
        else:
            compare_start = compare_end = None
    with _c : pass


    # (E) 선택·비교 기간별 레이블
    df_sel = df[df.event_date.between(pd.to_datetime(select_start), pd.to_datetime(select_end))].copy()
    df_sel["period"] = "선택기간"
    if use_compare and compare_start:
        df_cmp = df[df.event_date.between(pd.to_datetime(compare_start), pd.to_datetime(compare_end))].copy()
        df_cmp["period"] = "비교기간"
        df_all = pd.concat([df_sel, df_cmp], ignore_index=True)
    else:
        df_all = df_sel



    # (F) 행 필드 선택 — 광고그룹/광고소재/키워드 추가
    row_options = [
        "날짜", "매체", "소스", "미디엄", "캠페인",
        "브랜드 구분", "퍼널 구분", "품목 유형",
        "광고그룹", "광고소재", "키워드"   # ← 새로 추가
    ]
    sel_rows = st.multiselect(
        "행 필드 선택",
        options=row_options,
        default=["날짜", "매체", "소스", "미디엄"],
        key="tbl_row_fields"
    )

    # (F‑2) 각 한글 라벨 → DataFrame 컬럼명 매핑
    col_map = {
        "날짜":        "event_date",
        "매체":        "media_name",
        "소스":        "utm_source",
        "미디엄":      "utm_medium",
        "캠페인":      "campaign_name",
        "브랜드 구분":  "brand_type",
        "퍼널 구분":    "funnel_type",
        "품목 유형":    "product_type",
        "광고그룹":    "adgroup_name",   # ← 매핑 추가
        "광고소재":    "ad_name",        # ← 매핑 추가
        "키워드":      "keyword_name"    # ← 매핑 추가
    }

    # (F‑3) show_period 토글을 고려한 group_cols 결정 (기존 로직 유지)
    if show_period:
        group_cols = ["period"] + [
            col_map[r] for r in sel_rows if r != "날짜"
        ]
    else:
        group_cols = [col_map[r] for r in sel_rows]



    # (G) 본문 필터 UI 확장: 캠페인 → 광고그룹 → 광고소재 → 키워드
    mn_opts       = df_all.media_name.dropna().unique().tolist()
    src_opts      = df_all.utm_source.dropna().unique().tolist()
    med_opts      = df_all.utm_medium.dropna().unique().tolist()
    camp_opts     = df_all.campaign_name.dropna().unique().tolist()
    adgroup_opts  = df_all.adgroup_name.dropna().unique().tolist()      # ← 추가
    ad_opts       = df_all.ad_name.dropna().unique().tolist()           # ← 추가
    keyword_opts  = df_all.keyword_name.dropna().unique().tolist()      # ← 추가
    brand_opts   = df_all.brand_type.dropna().unique().tolist()   # ← 추가
    funnel_opts  = df_all.funnel_type.dropna().unique().tolist()  # ← 추가
    product_opts = df_all.product_type.dropna().unique().tolist() # ← 추가

    c1, c2, c3, c4, c8, c9, c10 = st.columns(7)
    with c1:
        sel_mn = st.multiselect("매체 선택", mn_opts, default=[], placeholder="전체", key="tbl_mn")
    with c2:
        sel_src = st.multiselect("소스 선택", src_opts, default=[], placeholder="전체", key="tbl_src")
    with c3:
        sel_med = st.multiselect("미디엄 선택", med_opts, default=[], placeholder="전체", key="tbl_med")
    with c4:
        sel_camp = st.multiselect("캠페인 선택", camp_opts, default=[], placeholder="전체", key="tbl_camp")
    with c8:
        sel_brand = st.multiselect("브랜드 구분 선택", brand_opts, default=[], placeholder="전체", key="tbl_brand")
    with c9:
        sel_product = st.multiselect("품목 유형 선택", product_opts, default=[], placeholder="전체", key="tbl_product")
    with c10:
        sel_funnel = st.multiselect("퍼널 구분 선택", funnel_opts, default=[], placeholder="전체", key="tbl_funnel")
    # c5, c6, c7, xx, yy = st.columns(5)
    # with c5:
    #     sel_adgroup = st.multiselect("광고그룹 선택", adgroup_opts, default=[], placeholder="전체", key="tbl_adgrp")
    # with c6:
    #     sel_ad = st.multiselect("광고소재 선택", ad_opts, default=[], placeholder="전체", key="tbl_ad")
    # with c7:
    #     sel_keyword = st.multiselect("키워드 선택", keyword_opts, default=[], placeholder="전체", key="tbl_kw")
    # with xx : pass
    # with yy : pass 


    # (G’) 고급 필터 토글
    show_advanced = st.checkbox("고급 필터 보기", value=False, key="tbl_show_adv")
    if show_advanced:
        c5, c6, c7, xx = st.columns([1,1,1,4])
        with c5:
            sel_adgroup = st.multiselect("광고그룹 선택", adgroup_opts, default=[], placeholder="전체", key="tbl_adgrp")
        with c6:
            sel_ad = st.multiselect("광고소재 선택", ad_opts, default=[], placeholder="전체", key="tbl_ad")
        with c7:
            sel_keyword = st.multiselect("키워드 선택", keyword_opts, default=[], placeholder="전체", key="tbl_kw")
        with xx : pass
    else:
        # 아무것도 선택하지 않았을 때, 변수 정의만 해두기
        sel_adgroup = []
        sel_ad      = []
        sel_keyword = []

    # 기본값 설정: 아무것도 선택 안 하면 전체
    filt_mn      = mn_opts      if not sel_mn      else sel_mn
    filt_src     = src_opts     if not sel_src     else sel_src
    filt_med     = med_opts     if not sel_med     else sel_med
    filt_camp    = camp_opts    if not sel_camp    else sel_camp
    filt_adgroup = adgroup_opts if not sel_adgroup else sel_adgroup
    filt_ad      = ad_opts      if not sel_ad      else sel_ad
    filt_kw      = keyword_opts if not sel_keyword  else sel_keyword
    filt_brand   = brand_opts   if not sel_brand   else sel_brand
    filt_funnel  = funnel_opts  if not sel_funnel  else sel_funnel
    filt_product = product_opts if not sel_product  else sel_product

    # (H) 필터 적용: 기존 필터에 광고그룹·광고소재·키워드 추가
    df_tbl = df_all[
        ( df_all.media_name.isin(filt_mn)  | df_all.media_name.isna() ) &
        ( df_all.utm_source.isin(filt_src) | df_all.utm_source.isna() ) &
        ( df_all.utm_medium.isin(filt_med) | df_all.utm_medium.isna() ) &
        ( df_all.campaign_name.isin(filt_camp) | 
          df_all.campaign_name.isna() ) &
        ( df_all.adgroup_name.isin(filt_adgroup) | df_all.adgroup_name.isna() ) &
        ( df_all.ad_name.isin(filt_ad) | df_all.ad_name.isna() ) &
        ( df_all.keyword_name.isin(filt_kw) | df_all.keyword_name.isna() )
        & (df_all.brand_type.isin(filt_brand)   | df_all.brand_type.isna())   \
        & (df_all.funnel_type.isin(filt_funnel) | df_all.funnel_type.isna()) \
        & (df_all.product_type.isin(filt_product) | df_all.product_type.isna())
    ].copy()


    
    df_tbl["flag_PDP조회"]  = (df_tbl.view_item > 0).astype(int)
    df_tbl["flag_PDPscr50"] = (df_tbl.product_page_scroll_50 > 0).astype(int)
    df_tbl["flag_가격표시"]  = (df_tbl.product_option_price > 0).astype(int)
    df_tbl["flag_쇼룸찾기"] = (df_tbl.find_nearby_showroom > 0).astype(int)
    df_tbl["flag_쇼룸10초"] = (df_tbl.showroom_10s > 0).astype(int)
    df_tbl["flag_장바구니"]  = (df_tbl.add_to_cart > 0).astype(int)
    df_tbl["flag_쇼룸예약"] = (df_tbl.showroom_leads > 0).astype(int)

    # (I) 그룹별 집계
    agg = (
        df_tbl
        .groupby(group_cols, dropna=False)
        .agg(
            전체_세션수      = ("pseudo_session_id", "sum"),
            PDP조회_세션수   = ("flag_PDP조회",      "sum"),
            PDPscr50_세션수  = ("flag_PDPscr50",     "sum"),
            가격표시_세션수   = ("flag_가격표시",      "sum"),
            쇼룸찾기_세션수  = ("flag_쇼룸찾기",     "sum"),
            쇼룸10초_세션수  = ("flag_쇼룸10초",     "sum"),
            장바구니_세션수   = ("flag_장바구니",      "sum"),
            쇼룸예약_세션수  = ("flag_쇼룸예약",     "sum"),
            광고비              = ("cost",                    "sum"),
            광고비_gross        = ("cost_gross",                    "sum"),
            노출수              = ("impressions",             "sum"),
            클릭수              = ("clicks",                  "sum"),
            평균_세션시간_밀리초 = ("engagement_time_msec_sum","sum")  # ← 추가
        )
        .reset_index()
    )



    # CPA 추가
    event_cols = [
        "전체_세션수","PDP조회_세션수","PDPscr50_세션수",
        "가격표시_세션수","쇼룸찾기_세션수","쇼룸10초_세션수",
        "장바구니_세션수","쇼룸예약_세션수"
    ]
    for col in event_cols:
        agg[f"{col}_CPA"] = agg.apply(
            lambda r: (r["광고비_gross"]/r[col]) if r[col]>0 else 0,
            axis=1
        )

    # (J) 파생 지표 계산 & 컬럼명 변환 & 날짜 포맷
    # —— CPC, CTR 계산 (클릭수=0 방어)
    agg["CPC"] = agg.apply(lambda r: r["광고비_gross"]/r["클릭수"] if r["클릭수"]>0 else 0, axis=1)
    agg["CTR"] = agg.apply(lambda r: r["클릭수"]/r["노출수"] if r["노출수"]>0 else 0, axis=1)

    # —— 값 포맷팅
    for c in ["광고비","광고비_gross","노출수","클릭수","CPC"]:
        agg[c] = agg[c].round(0).astype(int)
    agg["CTR"] = (agg["CTR"] * 100).round(2).astype(str) + "%"

    # —— 평균세션시간
    agg["평균세션시간_sec"] = agg["평균_세션시간_밀리초"] / agg["전체_세션수"] / 1000
    def format_hms(x):
        if pd.isna(x) or x == 0:
            return "-"
        total = int(round(x))
        hh = total // 3600
        mm = (total % 3600) // 60
        ss = total % 60
        return f"{hh:02d}:{mm:02d}:{ss:02d}"

    agg["평균세션시간"] = agg["평균세션시간_sec"].apply(format_hms)


    if "event_date" in agg.columns:
        agg["날짜"] = agg.event_date.dt.strftime("%Y-%m-%d")
        agg.drop(columns="event_date", inplace=True)
    rename_map = {v: k for k, v in col_map.items()}
    agg.rename(columns=rename_map, inplace=True)

    # (K) 컬럼 순서 & 정렬
    metrics = [
        "광고비","광고비_gross","노출수","클릭수","CPC","CTR","평균세션시간",
        "전체_세션수","PDP조회_세션수","PDPscr50_세션수",
        "가격표시_세션수","쇼룸찾기_세션수","쇼룸10초_세션수",
        "장바구니_세션수","쇼룸예약_세션수"
    ]
    # CPA
    metrics = []
    for col in event_cols:
        metrics += [col, f"{col}_CPA"]
    metrics += ["광고비","광고비_gross","노출수","클릭수","CPC","CTR","평균세션시간"]

    final_cols = []
    if show_period:
        final_cols.append("period")
    final_cols += sel_rows + metrics
    final_cols = [c for c in final_cols if c in agg.columns]
    agg = agg[final_cols]
    agg.sort_values("전체_세션수", ascending=False, inplace=True)





    # ──────────────────────────────────
    # (J) 파생 지표 계산 & 날짜 포맷 전 — CTR_raw 분리 생성
    # ──────────────────────────────────
    # CTR_raw: 합계행 계산용 숫자형
    agg["CTR_raw"] = agg.apply(
        lambda r: r["클릭수"] / r["노출수"] if r["노출수"] > 0 else 0,
        axis=1
    )
    # CTR: 그리드 표시용 문자열
    agg["CTR"] = (agg["CTR_raw"] * 100).round(2).astype(str) + "%"

    # ──────────────────────────────────
    # (J) 파생 지표 계산 & 날짜 포맷 전 — CTR_raw 분리 생성
    # ──────────────────────────────────
    # CTR_raw: 합계행 계산용 숫자형
    agg["CTR_raw"] = agg.apply(
        lambda r: r["클릭수"] / r["노출수"] if r["노출수"] > 0 else 0,
        axis=1
    )
    # CTR: 그리드 표시용 문자열
    agg["CTR"] = (agg["CTR_raw"] * 100).round(2).astype(str) + "%"



    # ──────────────────────────────────────────────────────
    # L) AgGrid 출력: 그룹 헤더 + 합계행 + 천단위 콤마 + 우측정렬 + 일의 자리
    # ──────────────────────────────────────────────────────
    # from st_aggrid import AgGrid, JsCode

    # 1) 합계행 계산
    bottom = { sel_rows[0]: "합계" }
    for col in agg.columns:
        if col in sel_rows:
            continue
        if pd.api.types.is_numeric_dtype(agg[col]):
            s = agg[col].sum()
            bottom[col] = s.item() if hasattr(s, "item") else s
        else:
            bottom[col] = ""


    # 2) 숫자형 자식 컬럼 정의 헬퍼 (콤마포맷 + 우측정렬 + 정수 표시)
    def make_num_child(header, field):
        return {
            "headerName": header,
            "field": field,
            "type": ["numericColumn","customNumericFormat"],
            "valueFormatter": JsCode(
                "function(params){"
                "  return params.value.toLocaleString(undefined,{maximumFractionDigits:0});"
                "}"
            )
        }

    # 3) GA 그룹 자식 정의
    #    ALL 그룹 하위에 '전체 세션수', '전체 CPA', '평균세션시간' 을 차례로 배치
    ga_children = [
        make_num_child("전체\n세션수",        "전체_세션수"),
        make_num_child("전체\nCPA",          "전체_세션수_CPA"),
        {
            "headerName": "평균\n세션시간",
            "field":      "평균세션시간",
            "cellStyle":  JsCode("params => ({ textAlign:'right' })"),
            "headerClass":"ag-right-aligned-header"       # 고맙다 지피티야
        }
    ]
    for evt in ["PDP조회","PDPscr50","가격표시","쇼룸찾기","쇼룸10초","장바구니","쇼룸예약"]:
        ga_children.append({
            "headerName": evt, "children": [
                make_num_child("Actual", f"{evt}_세션수"),
                make_num_child("CPA",    f"{evt}_세션수_CPA"),
            ]
        })


    # 4) MEDIA 그룹 자식 정의 (원본 라벨 유지, 숫자 포맷 적용)
    media_children = [
        make_num_child("광고비",        "광고비"),
        make_num_child("광고비(G)", "광고비_gross"),
        make_num_child("노출수",        "노출수"),
        make_num_child("클릭수",        "클릭수"),
        make_num_child("CPC",          "CPC"),
        make_num_child("CTR",          "CTR"),
    ]

    # # 5) columnDefs 구성 (headerClass 추가)
    # column_defs = [
    #     {
    #         "headerName": "구분",
    #         "children": [
    #             {
    #                 "headerName": r,
    #                 "field": r,
    #                 "cellStyle": JsCode("function(params){ return {'textAlign':'left'}; }")
    #             }
    #             for r in sel_rows
    #         ]
    #     },
    #     {
    #         "headerName": "MEDIA",
    #         "headerClass": "media-header",        # ← 여기
    #         "children": media_children
    #     },
    #     {
    #         "headerName": "GA",
    #         "headerClass": "ga-header",           # ← 여기
    #         "children": ga_children
    #     }
    # ]
        

    # 5) columnDefs 구성: “구분” 그룹에 period(기간) 추가  -  show_period=True 면 “기간” 을, 아니면 sel_rows 만
    group0_children = []

    if show_period:
        # “기간” 컬럼을 맨 앞에 추가
        group0_children.append({
            "headerName": "기간",
            "field": "period",
            "cellStyle": JsCode("function(params){ return {'textAlign':'left'}; }")
        })

    # 기존에 sel_rows 로 뽑던 부분
    for r in sel_rows:
        group0_children.append({
            "headerName": r,
            "field": r,
            "cellStyle": JsCode("function(params){ return {'textAlign':'left'}; }")
        })

    column_defs = [
        {
            "headerName": "구분",
            "children": group0_children
        },
        {
            "headerName": "MEDIA",
            "headerClass": "media-header",
            "children": media_children
        },
        {
            "headerName": "GA",
            "headerClass": "ga-header",
            "children": ga_children
        }
    ]

    # 6) gridOptions 설정 (기본 우측정렬, 좌측정렬은 위에서 덮어씀)
    grid_options = {
        "columnDefs": column_defs,
        "defaultColDef": {
            "sortable": True,
            "filter": True,
            "resizable": True,
            "wrapHeaderText": True,
            "autoHeaderHeight": True,
            # 나머지 모든 셀은 우측정렬
            "cellStyle": JsCode("function(params){ return {'textAlign':'right'}; }"),
            "width": 95
        },
        "pinnedBottomRowData": [bottom],
        "headerHeight": 30,
        "groupHeaderHeight": 30
    }

    # 7) 렌더링
    AgGrid(
        agg,
        gridOptions=grid_options,
        fit_columns_on_grid_load=False,
        height=510,
        theme="streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit",
        allow_unsafe_jscode=True
    )