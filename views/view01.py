# SEOHEE
# 2026-01-28 ver.

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import importlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery

import modules.ui_common as ui
importlib.reload(ui)


# ──────────────────────────────────
# CONFIG
# ──────────────────────────────────
CFG = {
    # 기본
    "TZ": "Asia/Seoul",
    "CACHE_TTL": 3600,
    "DEFAULT_LOOKBACK_DAYS": 14,
    "HEADER_UPDATE_AM": 850,
    "HEADER_UPDATE_PM": 1535,
    
    "TOPK_OPTS": [5, 10, 15, 20],
    
    "CSS_BLOCK_CONTAINER": """
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
    "CSS_TABS": """
        <style>
            [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """,
}


# ──────────────────────────────────
# HELPER
# ──────────────────────────────────
def _build_dt_map(w: pd.DataFrame) -> pd.DataFrame:
    # ※ dt_map
    return (
        w[["_period", "_period_dt"]]
        .assign(_period_dt=lambda x: pd.to_datetime(x["_period_dt"], errors="coerce"))
        .dropna(subset=["_period_dt"])
        .groupby("_period", as_index=False)["_period_dt"].min()
        .rename(columns={"_period": "기간"})
    )


def _safe_dim_series(df0: pd.DataFrame, col: str) -> pd.Series:
    if col in df0.columns:
        return df0[col].astype(str).replace("nan", "").fillna("").str.strip()
    return pd.Series([""] * len(df0), index=df0.index)


def _apply_topk_bucket(s: pd.Series, topk: int | None) -> pd.Series:
    if topk is None:
        return np.where(s != "", s, "기타")
    topk_vals = set(ui.get_topk_values(s, topk))
    return np.where((s != "") & s.isin(topk_vals), s, "기타")


def pivot_period_usersessions(
    df: pd.DataFrame,
    mode: str,
    group_cols: list[str] | None = None
) -> pd.DataFrame:
    w = ui.add_period_columns(df, "event_date", mode)
    cols = ["_period"] + (group_cols or [])

    out = (
        w.groupby(cols, as_index=False)
         .agg(
            유저수=("user_pseudo_id", "nunique"),
            세션수=("pseudo_session_id", "nunique"),
            신규방문=("_isUserNew_y", "sum"),
            재방문=("_isUserNew_n", "sum"),
         )
         .rename(columns={"_period": "기간"})
    )

    dt_map = _build_dt_map(w)
    out = out.merge(dt_map, on="기간", how="left")

    # ✅ 여기서 "기간당 1행" 보장 (섹션1에서 추가 groupby 필요없게)
    agg_cols = ["유저수", "세션수", "신규방문", "재방문"]
    out[agg_cols] = out[agg_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    out = (
        out.dropna(subset=["_period_dt"])
           .groupby(["기간", "_period_dt"], as_index=False)[agg_cols].sum()
           .sort_values("_period_dt")
           .reset_index(drop=True)
    )
    return out



EVENTS_META = [
    ("view_item", "PDP조회"),
    ("product_page_scroll_50", "PDPscr50"),
    ("product_option_price", "가격표시"),
    ("find_nearby_showroom", "쇼룸찾기"),
    ("showroom_10s", "쇼룸10초"),
    ("add_to_cart", "장바구니"),
    ("showroom_leads", "쇼룸예약"),
]

EVENT_GROUPS = {
    "🔍 제품탐색": ["view_item", "product_page_scroll_50"],
    "💓 관심표현": ["product_option_price", "find_nearby_showroom", "showroom_10s"],
    "🧺 전환의도": ["add_to_cart", "showroom_leads"],
}


def pivot_event_overview(df: pd.DataFrame, mode: str, metric_mode: str) -> pd.DataFrame:
    """
    metric_mode:
      - "유저수":    {event}>0 인 user_pseudo_id nunique
      - "세션수":    {event}>0 인 pseudo_session_id nunique
      - "이벤트수":  {event} 합
    """
    w = ui.add_period_columns(df, "event_date", mode)

    for ev, _ in EVENTS_META:
        w[ev] = pd.to_numeric(w.get(ev, 0), errors="coerce").fillna(0)

    dt_map = _build_dt_map(w)

    if metric_mode == "이벤트수":
        agg_map = {f"{label}_이벤트수": (ev, "sum") for ev, label in EVENTS_META}
        res = (
            w.groupby(["_period"], as_index=False)
             .agg(**agg_map)
             .rename(columns={"_period": "기간"})
        )
        res = res.merge(dt_map, on="기간", how="left").sort_values("_period_dt").reset_index(drop=True)
        return res

    res = (
        w[["_period"]]
        .drop_duplicates()
        .rename(columns={"_period": "기간"})
        .sort_values("기간")
        .reset_index(drop=True)
    )

    for ev, label in EVENTS_META:
        ww = w[w[ev] > 0]

        if metric_mode == "세션수":
            tmp = (
                ww.groupby(["_period"], as_index=False)
                  .agg(**{f"{label}_세션수": ("pseudo_session_id", "nunique")})
                  .rename(columns={"_period": "기간"})
            )
        else:  # "유저수"
            tmp = (
                ww.groupby(["_period"], as_index=False)
                  .agg(**{f"{label}_유저수": ("user_pseudo_id", "nunique")})
                  .rename(columns={"_period": "기간"})
            )

        res = res.merge(tmp, on="기간", how="left")

    for c in res.columns:
        if c != "기간":
            res[c] = pd.to_numeric(res[c], errors="coerce").fillna(0)

    res = res.merge(dt_map, on="기간", how="left").sort_values("_period_dt").reset_index(drop=True)
    return res


# ──────────────────────────────────
# main
# ──────────────────────────────────
def main():
    # ──────────────────────────────────
    # A) Layout / CSS
    # ──────────────────────────────────
    st.markdown(CFG["CSS_BLOCK_CONTAINER"], unsafe_allow_html=True)
    st.markdown(CFG["CSS_TABS"], unsafe_allow_html=True)

    # ──────────────────────────────────
    # B) Sidebar (기간)
    # ──────────────────────────────────
    st.sidebar.header("Filter")
    today = datetime.now().date()
    default_end = today - timedelta(days=1)
    default_start = today - timedelta(days=CFG["DEFAULT_LOOKBACK_DAYS"])

    start_date, end_date = st.sidebar.date_input(
        "기간 선택",
        value=[default_start, default_end],
        max_value=default_end
    )
    cs = start_date.strftime("%Y%m%d")
    ce = (end_date + timedelta(days=1)).strftime("%Y%m%d")

    # ──────────────────────────────────
    # C) Data Load
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def load_data(cs: str, ce: str):
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df = bq.get_data("tb_sleeper_psi")
        geo_map = bq.get_data("raw_geo_city_kr")

        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d", errors="coerce")

        # ✅ 선택기간으로 강제 필터 (ce는 exclusive)
        df = df[(df["event_date"] >= pd.to_datetime(cs)) & (df["event_date"] < pd.to_datetime(ce))]

        def _safe_str_col(colname: str) -> pd.Series:
            if colname in df.columns:
                s = df[colname]
            else:
                s = pd.Series([""] * len(df), index=df.index)
            return s.astype(str).replace("nan", "").fillna("").str.strip()

        # 유입 파생컬럼
        df["_source"] = _safe_str_col("collected_traffic_source__manual_source").replace("", "(not set)")
        df["_medium"] = _safe_str_col("collected_traffic_source__manual_medium").replace("", "(not set)")
        df["_campaign"] = _safe_str_col("collected_traffic_source__manual_campaign_name").replace("", "(not set)")
        df["_content"] = _safe_str_col("collected_traffic_source__manual_content").replace("", "(not set)")
        df["_term"] = _safe_str_col("collected_traffic_source__manual_term").replace("", "(not set)") # (26.05.15) TERM 추가 
        df["_sourceMedium"] = df["_source"] + " / " + df["_medium"]

        # 신규/재방문 파생컬럼
        fv = pd.to_numeric(df.get("first_visit", 0), errors="coerce").fillna(0)
        df["_isUserNew_y"] = (fv == 1).astype(int)
        df["_isUserNew_n"] = (fv == 0).astype(int)

        # 접속권역 파생컬럼
        # (GEO - 1) geo 컬럼 안전 전처리
        df["geo__city"]   = _safe_str_col("geo__city").replace("", "(not set)")
        df["geo__region"] = _safe_str_col("geo__region").replace("", "(not set)")

        # (GEO - 2) 1차: is_region=0 (city 단위)로 geo__city_kr 붙이기
        geo_city = (
            geo_map.loc[geo_map["is_region"].eq(0), ["geo__city", "geo__city_kr"]]
            .drop_duplicates(subset=["geo__city"], keep="first")
        )
        df = df.merge(geo_city, on="geo__city", how="left")
        df["geo__city_kr"] = df["geo__city_kr"].fillna("기타")

        # (GEO - 3) 2차: geo__city_kr == "기타" 인 것만, geo__region (is_region=1) 매핑으로 값이 있으면 교체
        geo_region_map = (
            geo_map.loc[geo_map["is_region"].eq(1), ["geo__city", "geo__city_kr"]]
            .drop_duplicates(subset=["geo__city"], keep="first")
            .set_index("geo__city")["geo__city_kr"]
        )

        m = df["geo__city_kr"].eq("기타")
        df.loc[m, "geo__city_kr"] = df.loc[m, "geo__region"].map(geo_region_map).fillna("기타")

        return df

    # ──────────────────────────────────
    # C-1) tb_max -> get max date
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def get_max():
        bq  = BigQuery(projectCode="sleeper") # 기간 인자 없이
        df  = bq.get_data("tb_max")
        date_max = df['max_date_psi'].iloc[0]
        date_max = pd.to_datetime(date_max).date()

        date_today = datetime.now().date()
        date_diff = (date_today - date_max).days
        
        return date_diff

    # ──────────────────────────────────
    # C-2) Progress Bar
    # ──────────────────────────────────
    spacer_placeholder = st.empty()
    progress_placeholder = st.empty()

    spacer_placeholder.markdown("<br>", unsafe_allow_html=True)
    progress_bar = progress_placeholder.progress(0, text="데이터베이스 연결 확인 중입니다...")
    
    import time
    time.sleep(0.2)
    
    for i in range(1, 80, 5):
        progress_bar.progress(i, text=f"데이터를 불러오고 있습니다...{i}%")
        time.sleep(0.1)
    
    df = load_data(cs, ce)
    
    progress_bar.progress(95, text="데이터 분석 및 시각화를 구성 중입니다...")
    time.sleep(0.4)
    
    progress_bar.progress(100, text="데이터 로드 완료!")
    time.sleep(0.6)

    progress_placeholder.empty()
    spacer_placeholder.empty()

    # ──────────────────────────────────
    # D) Header
    # ──────────────────────────────────
    st.subheader("트래픽 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px;line-height:1.5;">
            GA4(BigQuery) 데이터를 기반으로 <b>자사몰 트래픽 추이, 유입 경로, 주요 이벤트 현황</b>을 종합 분석하는 대시보드입니다.<br>
            </div>
            <div style="color:#6c757d;font-size:14px;line-height:2.0;">
            ※ 전일 데이터 업데이트 시점은 08:40 입니다. (유입매체 미분류시 15:15 재반영)
            </div>
            """,
            unsafe_allow_html=True
        )

    with col2:       
        # tb_max 
        date_diff = get_max()
        if date_diff <= 1:
            status_text = f"업데이트 정상"
            icon_name = "event_available"
            color = "#7FD0C4"
            bg_color = "#F3FBFA"
        else:
            status_text = f"업데이트 이전 (D-{date_diff})"
            icon_name = "event_busy"
            color = "#FF8080"
            bg_color = "#FFF4F4"
        
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:bottom;gap:9px;">
            <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20,400,0,0" rel="stylesheet" />
            
            <span style="
                display:flex;
                align-items:center;
                justify-content:center;
                gap: 4px;
                height:30px;
                padding:10px 10px;
                font-size:13px;
                font-weight:400;
                letter-spacing:-0.3px;
                color:{color};
                background-color:{bg_color};
                border:1.5px solid {color};
                border-radius:14px;
                white-space:nowrap;
                cursor:default;">
                <span class="material-symbols-outlined" style="font-size:15px;">{icon_name}</span>
                {status_text}
            </span>
            
            <a href="?refresh=1" title="사용자 캐시를 초기화하고 서버의 최신 데이터로 갱신합니다." style="text-decoration:none;vertical-align:middle;">
            <span style="
                display:flex;
                align-items:center;
                justify-content:center;
                gap: 4px;
                height:30px;
                padding:10px 10px;
                font-size:13px;
                font-weight:400;
                letter-spacing:-0.3px;
                color:#8B92A0;
                background-color:#FFFFFF;
                border:1.5px solid #D6D6D9;
                border-radius:14px;
                white-space:nowrap;
                cursor:pointer;
                transition:0.1s;">
                <span class="material-symbols-outlined" style="font-size:15px;">sync</span>
                강력 새로고침
            </span>
            </a>
            """,
            unsafe_allow_html=True
            )

    st.divider()

    # ──────────────────────────────────
    # 1) 트래픽 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>트래픽 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ트래픽의 전반적인 증감 추이와 방문 형태(신규/재방문) 비중 변화를 확인합니다.")

    with st.popover("🤔 유저 vs 세션 vs 이벤트, 뭐가 다른가요?"):
        st.markdown("""
        ##### 💡 한 눈에 이해하는 지표 가이드
        지표의 범위를 **쇼핑몰 방문**에 비유하면 이해가 빨라요!

        | 구분 | 개념 | 비유 |
        | :--- | :--- | :--- |
        | **유저수** | 방문한 **사람**의 수 | 쇼핑몰에 들어온 손님 수 |
        | **세션수** | 방문한 **횟수** | 손님이 문을 열고 들어온 횟수 |
        | **이벤트수** | 발생한 **행동**의 수 | 손님이 상품을 보거나 장바구니에 담은 횟수 |  
        
        #####  
        ##### 📊 실제 사례로 보기
        **손님 A**가 **오전**에 방문하여 `시그니처`를 조회하고, **오후**에 재방문하여 `시그니처`와 `허쉬`를 조회했다면?   
        
        | 지표 구분 | 집계 결과 | 이유 |
        | :--- | :--- | :--- |
        | **유저수** | **1** | 동일 인물(A)이므로 1명으로 집계 |
        | **세션수** | **2** | 오전/오후 방문이 나뉘어 2회로 집계 |
        | **이벤트수** | **3** | 상품 조회를 총 3번 수행하여 3건으로 집계 |

        ※ 한 유저가 여러 번 방문할 수 있기 때문에, 보통 **유저수 < 세션수 < 이벤트수** 순으로 수치가 커집니다.
        """)

    with st.expander("공통 Filter", expanded=True):
        r0_1, r0_2 = st.columns([1,4], vertical_alignment="bottom")
        with r0_1:
            mode_1 = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_1")
        with r0_2:
            sel_units_1 = st.pills(
                "집계 단위",
                ["유저수", "세션수"],
                selection_mode="multi",
                default=["유저수", "세션수"],
                key="units_1",
            )

    base1 = pivot_period_usersessions(df, mode=mode_1)
    x_dt = base1["_period_dt"].dt.to_pydatetime()
    tick_text = base1["기간"].astype(str).tolist()

    fig = go.Figure()

    # bar 데이터
    d_bar = base1.copy()
    d_bar["_bar_total"] = (d_bar["신규방문"] + d_bar["재방문"]).replace(0, np.nan)
    d_bar["_share_new"] = (d_bar["신규방문"] / d_bar["_bar_total"] * 100).round(1).fillna(0)
    d_bar["_share_ret"] = (d_bar["재방문"] / d_bar["_bar_total"] * 100).round(1).fillna(0)

    fig.add_bar(
        x=x_dt, y=d_bar["신규방문"], name="신규방문", opacity=0.6,
        customdata=np.stack([d_bar["_share_new"], d_bar["신규방문"]], axis=1),
        hovertemplate="신규방문<br>비중: %{customdata[0]}%<br>값: %{customdata[1]:,.0f}<extra></extra>"
    )
    fig.add_bar(
        x=x_dt, y=d_bar["재방문"], name="재방문", opacity=0.6,
        customdata=np.stack([d_bar["_share_ret"], d_bar["재방문"]], axis=1),
        hovertemplate="재방문<br>비중: %{customdata[0]}%<br>값: %{customdata[1]:,.0f}<extra></extra>"
    )

    # line 데이터
    for u in (sel_units_1 or []):
        if u in base1.columns:
            fig.add_scatter(
                x=x_dt,
                y=base1[u],
                name=u,
                mode="lines+markers",
                hovertemplate=f"{u}<br>값: %{{y:,.0f}}<extra></extra>",
            )

    # ✅ shading: 일별만
    if mode_1 == "일별":
        ui.add_weekend_shading(fig, base1["_period_dt"])

    # ✅ 여기서 "축 중복"을 끝내는 핵심: tick을 우리가 박아버림
    fig.update_xaxes(
        type="date",
        tickmode="array",
        tickvals=x_dt,
        ticktext=tick_text,
    )

    fig.update_layout(
        barmode="relative",
        height=360,
        xaxis_title=None,
        yaxis_title=None,
        bargap=0.5,
        bargroupgap=0.2,
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
        legend_title_text="",
        margin=dict(l=10, r=10, t=40, b=10),
    )

    st.plotly_chart(fig, use_container_width=True)

    # ------------------------------
    # 표 (pivot)
    tbl1 = base1.copy()

    den = pd.to_numeric(tbl1["세션수"], errors="coerce").replace(0, np.nan)
    tbl1["신규방문 비중(%)"] = (pd.to_numeric(tbl1["신규방문"], errors="coerce") / den * 100).round(1).fillna(0)
    tbl1["재방문 비중(%)"] = (pd.to_numeric(tbl1["재방문"], errors="coerce") / den * 100).round(1).fillna(0)

    u_den = pd.to_numeric(tbl1["유저수"], errors="coerce").replace(0, np.nan)
    tbl1["SPU (세션수/유저수)"] = (pd.to_numeric(tbl1["세션수"], errors="coerce") / u_den).round(2).fillna(0)

    show_metrics_1 = ["세션수", "유저수", "SPU (세션수/유저수)", "신규방문", "재방문", "신규방문 비중(%)", "재방문 비중(%)"]

    long1 = (
        tbl1[["기간"] + show_metrics_1]
        .melt(id_vars=["기간"], var_name="지표", value_name="값")
    )

    long1["지표"] = pd.Categorical(long1["지표"], categories=show_metrics_1, ordered=True)
    long1["값"] = pd.to_numeric(long1["값"], errors="coerce").fillna(0)

    pv1 = (
        long1
        .pivot_table(
            index="지표",
            columns="기간",
            values="값",
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    val_cols = pv1.columns[1:]
    pv1[val_cols] = pv1[val_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    styled = pv1.style.format("{:,.0f}", subset=val_cols)
    spu_mask = pv1["지표"].eq("SPU (세션수/유저수)")
    if spu_mask.any():
        spu_idx = pv1.index[spu_mask]
        styled = styled.format("{:.2f}", subset=pd.IndexSlice[spu_idx, val_cols])

    st.dataframe(styled, row_height=30, hide_index=True)


    # ──────────────────────────────────
    # 2) 트래픽 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>트래픽 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ유입 매체와 지리적 위치(지역/권역)에 따른 트래픽 분포 및 상세 비중을 비교 분석합니다.")

    def _select_opt(df0, col, label, key):
        s = _safe_dim_series(df0, col)
        vc = s[s != ""].value_counts(dropna=False)
        opts = ["전체"] + vc.index.astype(str).tolist()
        return st.selectbox(label, opts, index=0, key=key)

    def _get_src_dim(sel):
        if sel == "소스 / 매체": return "_sourceMedium", "소스/매체"
        if sel == "소스": return "_source", "소스"
        if sel == "매체": return "_medium", "매체"
        if sel == "캠페인": return "_campaign", "캠페인"
        if sel == "컨텐츠": return "_content", "컨텐츠"  # (26.05.15) TERM 추가 
        return "_content", "컨텐츠"

    def render_dim_trend(
        df_in: pd.DataFrame,
        mode: str,
        unit: str,
        dim_col: str,
        dim_label: str,
        topk: int | None,
        extra_filter: dict[str, str] | None = None
    ):
        df_f = df_in
        if extra_filter:
            for c, v in extra_filter.items():
                if v != "전체" and c in df_f.columns:
                    df_f = df_f[df_f[c] == v]

        tmp = ui.add_period_columns(df_f, "event_date", mode)
        dt_map = _build_dt_map(tmp)

        s = _safe_dim_series(tmp, dim_col)
        tmp["_dim2"] = _apply_topk_bucket(s, topk)

        grp = (
            tmp.groupby(["_period", "_dim2"], dropna=False)
            .agg(
                세션수=("pseudo_session_id", "nunique"),
                유저수=("user_pseudo_id", "nunique")
            )
            .reset_index()
            .rename(columns={"_period": "기간", "_dim2": dim_label})
        )

        grp = grp.merge(dt_map, on="기간", how="left")
        grp["_period_dt"] = pd.to_datetime(grp["_period_dt"], errors="coerce")
        grp = grp.dropna(subset=["_period_dt"]).sort_values("_period_dt").reset_index(drop=True)

        chart_key = f"stack::{dim_label}::{dim_col}::{mode}::{unit}::{topk}"
        if extra_filter:
            chart_key += "::" + "::".join([f"{k}={v}" for k, v in sorted(extra_filter.items())])

        if mode == "일별":
            x_base = (
                grp[["기간", "_period_dt"]]
                .drop_duplicates(subset=["기간"])
                .sort_values("_period_dt")
                .reset_index(drop=True)
            )
            x_dt = x_base["_period_dt"].dt.to_pydatetime()
            tick_text = x_base["기간"].astype(str).tolist()

            fig = go.Figure()

            dims = grp[dim_label].astype(str).unique().tolist()
            for d in dims:
                sub = grp[grp[dim_label].astype(str) == str(d)]
                fig.add_bar(
                    x=sub["_period_dt"].dt.to_pydatetime(),
                    y=pd.to_numeric(sub[unit], errors="coerce").fillna(0),
                    name=str(d),
                    opacity=0.6,
                )

            ui.add_weekend_shading(fig, x_base["_period_dt"])

            fig.update_xaxes(
                type="date",
                tickmode="array",
                tickvals=x_dt,
                ticktext=tick_text,
            )
            fig.update_layout(
                barmode="relative",
                height=360,
                xaxis_title=None,
                yaxis_title=None,
                bargap=0.5,
                bargroupgap=0.2,
                legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
                legend_title_text="",
                margin=dict(l=10, r=10, t=40, b=10),
            )
            st.plotly_chart(fig, use_container_width=True, key=chart_key)

        else:
            x_col = "기간"
            ui.render_stack_graph(grp, x=x_col, y=unit, color=dim_label, key=chart_key, show_value_in_hover=True)

        long = grp[["기간", dim_label, unit]].rename(columns={unit: "값"})
        pv = ui.build_pivot_table(long, index_col=dim_label, col_col="기간", val_col="값")

        styled = ui.style_format(pv, decimals_map={c: 0 for c in pv.columns if c != dim_label})
        st.dataframe(styled, row_height=30, hide_index=True)

    with st.expander("공통 Filter", expanded=True):
        cf1, cf2, cf3, _p = st.columns([1, 1, 1, 2], vertical_alignment="bottom")
        with cf1:
            traffic_mode = st.radio("기간 단위", ["일별", "주별"], index=0, horizontal=True, key="traffic_mode")
        with cf2:
            traffic_unit = st.radio("집계 단위", ["유저수", "세션수"], index=1, horizontal=True, key="traffic_unit")
        with cf3:
            traffic_topk = st.selectbox("표시 Top K", CFG["TOPK_OPTS"], index=1, key="traffic_topk")


    tab_geo_kr, tab_geo, tab_src, tab_mix, tab_dev = st.tabs(["접속권역", "접속지역", "유입매체", "매체X지역", "디바이스"])

    with tab_geo_kr:
        with st.expander("탭별 Filter", expanded=False):
            c1, _p = st.columns([1, 4], vertical_alignment="bottom")
            with c1:
                sel = _select_opt(df, "geo__city_kr", "권역 선택", "gk_s")
        render_dim_trend(df, traffic_mode, traffic_unit, "geo__city_kr", "접속권역", traffic_topk, {"geo__city_kr": sel})

    with tab_geo:
        with st.expander("탭별 Filter", expanded=False):
            c1, c2, _p = st.columns([1, 1, 3], vertical_alignment="bottom")
            with c1:
                sel_kr = _select_opt(df, "geo__city_kr", "권역 선택", "g_kr")
            with c2:
                sel = _select_opt(df, "geo__city", "지역 선택", "g_c")

        render_dim_trend(df, traffic_mode, traffic_unit, "geo__city", "접속지역", traffic_topk, {"geo__city_kr": sel_kr, "geo__city": sel})

    with tab_src:
        with st.expander("탭별 Filter", expanded=False):
            c1, c2, _p = st.columns([1, 1, 3], vertical_alignment="bottom")
            with c1:
                sel_dim = st.selectbox("유입 기준 선택", ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠", "키워드"], index=0, key="s_d")  # (26.05.15) TERM 추가 
            with c2:
                dim_col, dim_label = _get_src_dim(sel_dim)
                sel = _select_opt(df, dim_col, f"{dim_label} 선택", "s_v")

        extra = {} if sel == "전체" else {dim_col: sel}
        render_dim_trend(df, traffic_mode, traffic_unit, dim_col, dim_label, traffic_topk, extra)

    with tab_mix:
        with st.expander("탭별 Filter", expanded=False):
            c1, c2, c3, _p = st.columns([1, 1, 1, 2], vertical_alignment="bottom")
            with c1:
                sel_src = _select_opt(df, "_sourceMedium", "소스/매체 선택", "m_s")
            with c2:
                dim_mode = st.radio("권역/지역 선택", ["권역", "지역"], index=0, horizontal=True, key="m_d")
            with c3:
                dim_col, dim_label = ("geo__city_kr", "접속권역") if dim_mode == "권역" else ("geo__city", "접속지역")
                sel = _select_opt(df, dim_col, f"{'권역' if dim_mode == '권역' else '지역'} 선택", "m_v")

        extra = {"_sourceMedium": sel_src}
        if sel != "전체":
            extra[dim_col] = sel

        render_dim_trend(df, traffic_mode, traffic_unit, dim_col, dim_label, traffic_topk, extra)

    with tab_dev:
        with st.expander("탭별 Filter", expanded=False):
            c1, _p = st.columns([1, 4], vertical_alignment="bottom")
            with c1:
                sel = _select_opt(df, "device__category", "디바이스 선택", "d_v")
        render_dim_trend(df, traffic_mode, traffic_unit, "device__category", "디바이스", traffic_topk, {"device__category": sel})


    # ──────────────────────────────────
    # 3) 이벤트 추이
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>이벤트 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ제품 탐색부터 전환 의도까지, 사용자의 구매 여정별 핵심 행동 이벤트의 변화를 확인합니다.")

    with st.popover("🤔 유저 vs 세션 vs 이벤트, 뭐가 다른가요?"):
        st.markdown("""
        ##### 💡 한 눈에 이해하는 지표 가이드
        지표의 범위를 **쇼핑몰 방문**에 비유하면 이해가 빨라요!

        | 구분 | 개념 | 비유 |
        | :--- | :--- | :--- |
        | **유저수** | 방문한 **사람**의 수 | 쇼핑몰에 들어온 손님 수 |
        | **세션수** | 방문한 **횟수** | 손님이 문을 열고 들어온 횟수 |
        | **이벤트수** | 발생한 **행동**의 수 | 손님이 상품을 보거나 장바구니에 담은 횟수 |  
        
        #####  
        ##### 📊 실제 사례로 보기
        **손님 A**가 **오전**에 방문하여 `시그니처`를 조회하고, **오후**에 재방문하여 `시그니처`와 `허쉬`를 조회했다면?   
        
        | 지표 구분 | 집계 결과 | 이유 |
        | :--- | :--- | :--- |
        | **유저수** | **1** | 동일 인물(A)이므로 1명으로 집계 |
        | **세션수** | **2** | 오전/오후 방문이 나뉘어 2회로 집계 |
        | **이벤트수** | **3** | 상품 조회를 총 3번 수행하여 3건으로 집계 |

        ※ 한 유저가 여러 번 방문할 수 있기 때문에, 보통 **유저수 < 세션수 < 이벤트수** 순으로 수치가 커집니다.
        """)

    with st.expander("공통 Filter", expanded=True):
        c31, c32 = st.columns([1,4], vertical_alignment="bottom")
        with c31:
            mode_3 = st.radio("기간 단위", ["일별", "주별"], index=0, horizontal=True, key="mode_3")
        with c32:
            metric_mode_3 = st.radio(
                "집계 기준",
                ["유저수", "세션수", "이벤트수"],
                index=1,
                horizontal=True,
                key="metric_mode_3",
            )

    # metrics = pivot_event_overview(df, mode=mode_3, metric_mode=metric_mode_3)
    # metrics = metrics.dropna(subset=["_period_dt"])

    # def _cols_for(events: list[str]) -> list[str]:
    #     label_map = {ev: label for ev, label in EVENTS_META}
    #     cols = []
    #     for ev in events:
    #         label = label_map.get(ev, ev)
    #         if metric_mode_3 == "이벤트수":
    #             cols.append(f"{label}_이벤트수")
    #         elif metric_mode_3 == "세션수":
    #             cols.append(f"{label}_세션수")
    #         else:
    #             cols.append(f"{label}_유저수")
    #     return [c for c in cols if c in metrics.columns]

    # m2 = metrics.copy()
    # x_col_3 = "_period_dt" if mode_3 == "일별" else "기간"

    # col_a, col_b, col_c = st.columns(3)
    # with col_a:
    #     ui.render_line_graph(m2, x=x_col_3, y=_cols_for(EVENT_GROUPS["🔍 제품탐색"]), title="🔍 제품탐색")
    # with col_b:
    #     ui.render_line_graph(m2, x=x_col_3, y=_cols_for(EVENT_GROUPS["💓 관심표현"]), title="💓 관심표현")
    # with col_c:
    #     ui.render_line_graph(m2, x=x_col_3, y=_cols_for(EVENT_GROUPS["🧺 전환의도"]), title="🧺 전환의도")

    # # long3 = metrics.melt(id_vars=["기간"], var_name="지표", value_name="값")
    # long3 = metrics.drop(columns=["_period_dt"], errors="ignore").melt(id_vars=["기간"], var_name="지표", value_name="값")

    # pv3 = ui.build_pivot_table(long3, index_col="지표", col_col="기간", val_col="값")
    
    # styled = ui.style_format(pv3, decimals_map={c: 0 for c in pv3.columns if c != "지표"})
    # st.dataframe(styled, row_height=30, hide_index=True)
    def _render_event_trend(df_in: pd.DataFrame):
        metrics = pivot_event_overview(df_in, mode=mode_3, metric_mode=metric_mode_3)
        metrics = metrics.dropna(subset=["_period_dt"])

        def _cols_for(events: list[str]) -> list[str]:
            label_map = {ev: label for ev, label in EVENTS_META}
            cols = []
            for ev in events:
                label = label_map.get(ev, ev)
                if metric_mode_3 == "이벤트수":
                    cols.append(f"{label}_이벤트수")
                elif metric_mode_3 == "세션수":
                    cols.append(f"{label}_세션수")
                else:
                    cols.append(f"{label}_유저수")
            return [c for c in cols if c in metrics.columns]

        m2 = metrics.copy()
        x_col_3 = "_period_dt" if mode_3 == "일별" else "기간"

        col_a, col_b, col_c = st.columns(3)
        with col_a:
            ui.render_line_graph(m2, x=x_col_3, y=_cols_for(EVENT_GROUPS["🔍 제품탐색"]), title="🔍 제품탐색")
        with col_b:
            ui.render_line_graph(m2, x=x_col_3, y=_cols_for(EVENT_GROUPS["💓 관심표현"]), title="💓 관심표현")
        with col_c:
            ui.render_line_graph(m2, x=x_col_3, y=_cols_for(EVENT_GROUPS["🧺 전환의도"]), title="🧺 전환의도")

        long3 = metrics.drop(columns=["_period_dt"], errors="ignore").melt(id_vars=["기간"], var_name="지표", value_name="값")
        pv3 = ui.build_pivot_table(long3, index_col="지표", col_col="기간", val_col="값")

        styled = ui.style_format(pv3, decimals_map={c: 0 for c in pv3.columns if c != "지표"})
        st.dataframe(styled, row_height=30, hide_index=True)

    tab_evt_all, tab_evt_slpr, tab_evt_nouer, tab_evt_todz = st.tabs(["전체", "슬립퍼", "누어", "토들즈"])

    with tab_evt_all:
        _render_event_trend(df)

    with tab_evt_slpr:
        _render_event_trend(df[_safe_dim_series(df, "product_cat_a") == "슬립퍼"])

    with tab_evt_nouer:
        _render_event_trend(df[_safe_dim_series(df, "product_cat_a") == "누어"])

    with tab_evt_todz:
        _render_event_trend(df[_safe_dim_series(df, "product_cat_a") == "토들즈"])



    # ──────────────────────────────────
    # 4) 이벤트 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>이벤트 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ특정 이벤트가 어떤 매체 혹은 지역에서 집중적으로 발생하는지 상세 세그먼트별로 확인합니다.")

    # 이벤트 선택 옵션 (라벨 기준)
    ev_label_opts = [label for _, label in EVENTS_META]
    ev_label_to_col = {label: ev for ev, label in EVENTS_META}

    def pivot_event_dim_trend(
        df_in: pd.DataFrame,
        ev_col: str,
        mode: str,
        unit: str,                 # "유저수" | "세션수" | "이벤트수"
        dim_col: str,
        dim_label: str,
        topk: int | None,
        extra_filter: dict[str, str] | None = None,
    ) -> None:
        df_f = df_in

        # (1) 이벤트 발생 데이터만 대상으로 상세 현황 확인
        if ev_col in df_f.columns:
            df_f[ev_col] = pd.to_numeric(df_f[ev_col], errors="coerce").fillna(0)
            df_f = df_f[df_f[ev_col] > 0]
        else:
            df_f = df_f.iloc[0:0]

        if df_f.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
            return

        # (2) 탭별 추가 필터 적용
        if extra_filter:
            for c, v in extra_filter.items():
                if v != "전체" and c in df_f.columns:
                    df_f = df_f[df_f[c] == v]

        tmp = ui.add_period_columns(df_f, "event_date", mode)
        dt_map = _build_dt_map(tmp)

        # dim 컬럼 준비 + TopK + 기타 처리
        s = _safe_dim_series(tmp, dim_col)
        tmp["_dim2"] = _apply_topk_bucket(s, topk)

        # 집계
        if unit == "이벤트수":
            grp = (
                tmp.groupby(["_period", "_dim2"], dropna=False)
                .agg(이벤트수=(ev_col, "sum"))
                .reset_index()
                .rename(columns={"_period": "기간", "_dim2": dim_label})
            )
            grp["이벤트수"] = pd.to_numeric(grp["이벤트수"], errors="coerce").fillna(0)

        elif unit == "세션수":
            grp = (
                tmp.groupby(["_period", "_dim2"], dropna=False)
                .agg(세션수=("pseudo_session_id", "nunique"))
                .reset_index()
                .rename(columns={"_period": "기간", "_dim2": dim_label})
            )
            grp["세션수"] = pd.to_numeric(grp["세션수"], errors="coerce").fillna(0)

        else:  # "유저수"
            grp = (
                tmp.groupby(["_period", "_dim2"], dropna=False)
                .agg(유저수=("user_pseudo_id", "nunique"))
                .reset_index()
                .rename(columns={"_period": "기간", "_dim2": dim_label})
            )
            grp["유저수"] = pd.to_numeric(grp["유저수"], errors="coerce").fillna(0)

        grp = (
            grp.merge(dt_map, on="기간", how="left")
            .assign(_period_dt=lambda x: pd.to_datetime(x["_period_dt"], errors="coerce"))
            .dropna(subset=["_period_dt"])
            .sort_values("_period_dt")
            .reset_index(drop=True)
        )

        chart_key = f"event_stack::{ev_col}::{dim_label}::{dim_col}::{mode}::{unit}::{topk}"
        if extra_filter:
            chart_key += "::" + "::".join([f"{k}={v}" for k, v in sorted(extra_filter.items())])

        x_col = "_period_dt" if mode == "일별" else "기간"
        ui.render_stack_graph(grp, x=x_col, y=unit, color=dim_label, key=chart_key, show_value_in_hover=True)

        long = grp[["기간", dim_label, unit]].rename(columns={unit: "값"})
        pv = ui.build_pivot_table(long, index_col=dim_label, col_col="기간", val_col="값")

        styled = ui.style_format(pv, decimals_map={c: 0 for c in pv.columns if c != dim_label})
        st.dataframe(styled, row_height=30, hide_index=True)

    with st.expander("공통 Filter", expanded=True):
        cf1, cf2, cf3, cf4, _p = st.columns([1, 1, 1, 1, 1], vertical_alignment="bottom")
        with cf1:
            e4_mode = st.radio("기간 단위", ["일별", "주별"], index=0, horizontal=True, key="e4_mode")
        with cf2:
            e4_unit = st.radio("집계 단위", ["유저수", "세션수", "이벤트수"], index=1, horizontal=True, key="e4_unit")
        with cf3:
            e4_sel_ev_label = st.selectbox("이벤트 선택", ev_label_opts, index=0, key="e4_sel_ev")
        with cf4:
            e4_topk = st.selectbox("표시 Top K", CFG["TOPK_OPTS"], index=1, key="e4_topk")

    ev_col = ev_label_to_col.get(e4_sel_ev_label, "view_item")

    tab_e_geo_kr, tab_e_geo, tab_e_src = st.tabs(["접속권역", "접속지역", "유입매체"])

    with tab_e_geo_kr:
        with st.expander("탭별 Filter", expanded=False):
            c1, _p = st.columns([1, 4], vertical_alignment="bottom")
            with c1:
                sel = _select_opt(df, "geo__city_kr", "권역 선택", "e4_gk_s")

        pivot_event_dim_trend(
            df_in=df,
            ev_col=ev_col,
            mode=e4_mode,
            unit=e4_unit,
            dim_col="geo__city_kr",
            dim_label="접속권역",
            topk=e4_topk,
            extra_filter={"geo__city_kr": sel},
        )

    with tab_e_geo:
        with st.expander("탭별 Filter", expanded=False):
            c1, c2, _p = st.columns([1, 1, 3], vertical_alignment="bottom")
            with c1:
                sel_kr = _select_opt(df, "geo__city_kr", "권역 선택", "e4_g_kr")
            with c2:
                sel = _select_opt(df, "geo__city", "지역 선택", "e4_g_c")

        extra = {"geo__city_kr": sel_kr, "geo__city": sel}
        pivot_event_dim_trend(
            df_in=df,
            ev_col=ev_col,
            mode=e4_mode,
            unit=e4_unit,
            dim_col="geo__city",
            dim_label="접속지역",
            topk=e4_topk,
            extra_filter=extra,
        )

    with tab_e_src:
        with st.expander("탭별 Filter", expanded=False):
            c1, c2, _p = st.columns([1, 1, 3], vertical_alignment="bottom")
            with c1:
                sel_dim = st.selectbox("유입 기준 선택", ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠", "키워드"], index=0, key="e4_s_d")  # (26.05.15) TERM 추가 
            with c2:
                dim_col, dim_label = _get_src_dim(sel_dim)
                sel = _select_opt(df, dim_col, f"{dim_label} 선택", "e4_s_v")

        extra = {} if sel == "전체" else {dim_col: sel}
        pivot_event_dim_trend(
            df_in=df,
            ev_col=ev_col,
            mode=e4_mode,
            unit=e4_unit,
            dim_col=dim_col,
            dim_label=dim_label,
            topk=e4_topk,
            extra_filter=extra,
        )

if __name__ == "__main__":
    main()
