# views/view06.py
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

import modules.style
importlib.reload(sys.modules["modules.style"])
from modules.style import style_format, style_cmap  # style_cmap 미사용이어도 유지

# ✅ Streamlit reload 이슈 방지: ui_common은 반드시 "모듈 import -> reload"
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
    "TOPK_DEFAULT": 10,
    # 패딩
    "CSS_BLOCK_CONTAINER": """
        <style>
            .block-container {
                max-width: 100% !important;
                padding-top: 1rem;
                padding-bottom: 8rem;
                padding-left: 5rem;
                padding-right: 4rem;
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
# 추가 유틸 함수
# ──────────────────────────────────
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

    dt_map = (
        w[["_period", "_period_dt"]]
        .drop_duplicates()
        .rename(columns={"_period": "기간"})
    )
    out = out.merge(dt_map, on="기간", how="left")
    out = out.sort_values("_period_dt").reset_index(drop=True)
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
        if ev in w.columns:
            w[ev] = pd.to_numeric(w[ev], errors="coerce").fillna(0)
        else:
            w[ev] = 0

    # ✅ 기간 정렬/샤딩용 _period_dt 확보
    dt_map = (
        w[["_period", "_period_dt"]]
        .drop_duplicates()
        .rename(columns={"_period": "기간"})
    )

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
    ce_exclusive = (end_date + timedelta(days=1)).strftime("%Y%m%d")

    # ──────────────────────────────────
    # C) Data Load
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def load_data(cs: str, ce: str) -> tuple[pd.DataFrame, object]:
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df = bq.get_data("tb_sleeper_psi")
        last_updated_time = df["event_date"].max()
        geo_map = bq.get_data("geo_city_kr_raw")

        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d", errors="coerce")

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
        df["_sourceMedium"] = df["_source"] + " / " + df["_medium"]

        # 신규/재방문 파생컬럼
        fv = pd.to_numeric(df.get("first_visit", 0), errors="coerce").fillna(0)
        df["_isUserNew_y"] = (fv == 1).astype(int)
        df["_isUserNew_n"] = (fv == 0).astype(int)

        # 접속권역 파생컬럼 - geo__city 기준 조인
        df = df.merge(geo_map, on="geo__city", how="left", suffixes=("", "__geo"))
        df["geo__city_kr"] = df["geo__city_kr"].fillna("기타")

        return df, last_updated_time

    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df, last_updated_time = load_data(cs, ce_exclusive)

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
            GA 기준 <b>자사몰 트래픽 </b>추이와 <b>유입경로, 주요 이벤트</b> 추이를 종합적으로 확인할 수 있는 대시보드입니다.<br>
            </div>
            <div style="color:#6c757d;font-size:14px;line-height:2.0;">
            ※ GA D-1 데이터의 세션 수치는 <b>오전에 1차</b> 집계되나 , 세션의 유입출처는 <b>오후에 2차</b> 반영됩니다.
            </div>
            """,
            unsafe_allow_html=True
        )

    with col2:
        latest_dt = last_updated_time if isinstance(last_updated_time, pd.Timestamp) else pd.to_datetime(last_updated_time, errors="coerce")
        latest_dt = latest_dt.to_pydatetime() if hasattr(latest_dt, "to_pydatetime") else latest_dt

        latest_date = latest_dt.date() if latest_dt else (datetime.now().date() - timedelta(days=999))
        now_kst = datetime.now(ZoneInfo(CFG["TZ"]))
        today_kst = now_kst.date()
        delta_days = (today_kst - latest_date).days
        hm_ref = now_kst.hour * 100 + now_kst.minute

        msg = "집계 예정 (AM 08:50 / PM 15:35)"
        sub_bg = "#f8fafc"
        sub_bd = "#e2e8f0"
        sub_fg = "#475569"

        if delta_days >= 2:
            msg = "업데이트가 지연되고 있습니다"
            sub_bg = "#fef2f2"
            sub_bd = "#fee2e2"
            sub_fg = "#b91c1c"
        elif delta_days == 1:
            if hm_ref >= CFG["HEADER_UPDATE_PM"]:
                msg = "2차 업데이트 완료 (PM 15:35)"
                sub_bg = "#fff7ed"
                sub_bd = "#fdba74"
                sub_fg = "#c2410c"
            elif hm_ref >= CFG["HEADER_UPDATE_AM"]:
                msg = "1차 업데이트 완료 (AM 08:50)"

        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
            <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 8px;
                font-size:13px;line-height:1;
                color:{sub_fg};background:{sub_bg};border:1px solid {sub_bd};
                border-radius:10px;white-space:nowrap;">
                🔔 {msg}
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
    # 1) 트래픽 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>트래픽 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ트래픽의 증감 추이와 신규·재방문 비중 변화를 확인합니다.")

    with st.popover("🤔 유저 VS 세션 차이점"):
        st.markdown("""
        - **유저수** (user_pseudo_id) : 고유 사람수  
        - **세션수** (pseudo_session_id) : 방문 단위수  
        - 사람 A가 1월 1일 오전에 신규방문 후 이탈, 오후에 재방문했다면,  
          1월 1일의 **유저수**는 1, **세션수**는 2, **신규방문수**는 1, **재방문수**는 1 입니다.
        - 유저수 ≤ 세션수 입니다.
        """)

    with st.expander("Filter", expanded=False):
        r0_1, r0_2 = st.columns([1.3, 2.7], vertical_alignment="bottom")
        with r0_1:
            mode_1 = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_1")
        with r0_2:
            sel_units_1 = st.pills(
                "집계 단위",
                ["유저수", "세션수"],
                selection_mode="multi",
                default=["세션수"],
                key="units_1",
            )

    base1 = pivot_period_usersessions(df, mode=mode_1)
    base1 = base1.rename(columns={"기간": "x"})
    x_col_1 = "_period_dt" if mode_1 == "일별" else "x"

    fig = go.Figure()
    d_bar = base1.copy()
    for col in ["신규방문", "재방문"]:
        d_bar[col] = pd.to_numeric(d_bar[col], errors="coerce").fillna(0)

    d_bar["_bar_total"] = (d_bar["신규방문"] + d_bar["재방문"]).replace(0, np.nan)
    d_bar["_share_new"] = (d_bar["신규방문"] / d_bar["_bar_total"] * 100).round(1).fillna(0)
    d_bar["_share_ret"] = (d_bar["재방문"] / d_bar["_bar_total"] * 100).round(1).fillna(0)

    fig.add_bar(
        x=d_bar[x_col_1], y=d_bar["신규방문"], name="신규방문", opacity=0.6,
        customdata=np.stack([d_bar["_share_new"], d_bar["신규방문"]], axis=1),
        hovertemplate="신규방문<br>비중: %{customdata[0]}%<br>값: %{customdata[1]:,.0f}<extra></extra>"
    )
    fig.add_bar(
        x=d_bar[x_col_1], y=d_bar["재방문"], name="재방문", opacity=0.6,
        customdata=np.stack([d_bar["_share_ret"], d_bar["재방문"]], axis=1),
        hovertemplate="재방문<br>비중: %{customdata[0]}%<br>값: %{customdata[1]:,.0f}<extra></extra>"
    )

    for u in (sel_units_1 or []):
        if u in base1.columns:
            fig.add_scatter(
                x=base1[x_col_1],
                y=base1[u],
                name=u,
                mode="lines+markers",
                hovertemplate=f"{u}<br>값: %{{y:,.0f}}<extra></extra>",
            )

    # ✅ shading: 일별만 (주별 라벨/주별 dt는 스킵)
    if mode_1 == "일별":
        ui.add_weekend_shading(fig, base1["_period_dt"])

    fig.update_layout(
        barmode="relative",
        height=360,
        xaxis_title=None,
        yaxis_title=None,
        bargap=0.5,
        bargroupgap=0.2,
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
        legend_title_text="",  # ✅ 범례 제목 제거
        margin=dict(l=10, r=10, t=40, b=10),
    )
    if mode_1 == "일별":
        fig.update_xaxes(tickformat="%m월 %d일")
    st.plotly_chart(fig, use_container_width=True)

    tbl1 = base1.copy()

    den = pd.to_numeric(tbl1["세션수"], errors="coerce").replace(0, np.nan)
    tbl1["신규방문 비중(%)"] = (pd.to_numeric(tbl1["신규방문"], errors="coerce") / den * 100).round(1).fillna(0)
    tbl1["재방문 비중(%)"] = (pd.to_numeric(tbl1["재방문"], errors="coerce") / den * 100).round(1).fillna(0)

    u_den = pd.to_numeric(tbl1["유저수"], errors="coerce").replace(0, np.nan)
    tbl1["SPU (세션수/유저수)"] = (pd.to_numeric(tbl1["세션수"], errors="coerce") / u_den).round(2).fillna(0)

    show_metrics_1 = ["세션수", "유저수", "SPU (세션수/유저수)", "신규방문", "재방문", "신규방문 비중(%)", "재방문 비중(%)"]

    long1 = (
        tbl1[["x"] + show_metrics_1]
        .melt(id_vars=["x"], var_name="지표", value_name="값")
        .rename(columns={"x": "기간"})
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
    st.markdown(":gray-badge[:material/Info: Info]ㅤ트래픽의 지역 또는 매체별 비중을 확인합니다.")

    def _select_opt(df0, col, label, key):
        s = df0.get(col, pd.Series(dtype=str)).astype(str).replace("nan", "").fillna("").str.strip()
        vc = s[s != ""].value_counts(dropna=False)
        opts = ["전체"] + vc.index.astype(str).tolist()
        return st.selectbox(label, opts, index=0, key=key)

    def _get_src_dim(sel):
        if sel == "소스 / 매체": return "_sourceMedium", "소스/매체"
        if sel == "소스": return "_source", "소스"
        if sel == "매체": return "_medium", "매체"
        if sel == "캠페인": return "_campaign", "캠페인"
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
        df_f = df_in.copy()
        if extra_filter:
            for c, v in extra_filter.items():
                if v != "전체" and c in df_f.columns:
                    df_f = df_f[df_f[c] == v]

        tmp = ui.add_period_columns(df_f, "event_date", mode)

        # ✅ 기간 dt 매핑 추가(차트 shading/정렬용)
        dt_map = (
            tmp[["_period", "_period_dt"]]
            .drop_duplicates()
            .rename(columns={"_period": "기간"})
        )

        if dim_col in tmp.columns:
            s = tmp[dim_col].astype(str).replace("nan", "").fillna("").str.strip()
        else:
            s = pd.Series([""] * len(tmp), index=tmp.index)

        if topk is None:
            tmp["_dim2"] = np.where(s != "", s, "기타")
        else:
            topk_vals = set(ui.get_topk_values(s, topk))
            tmp["_dim2"] = np.where((s != "") & s.isin(topk_vals), s, "기타")

        grp = (
            tmp.groupby(["_period", "_dim2"], dropna=False)
            .agg(
                세션수=("pseudo_session_id", "nunique"),
                유저수=("user_pseudo_id", "nunique")
            )
            .reset_index()
            .rename(columns={"_period": "기간", "_dim2": dim_label})
        )

        grp = grp.merge(dt_map, on="기간", how="left").sort_values("_period_dt").reset_index(drop=True)

        chart_key = f"stack::{dim_label}::{dim_col}::{mode}::{unit}::{topk}"
        if extra_filter:
            chart_key += "::" + "::".join([f"{k}={v}" for k, v in sorted(extra_filter.items())])

        x_col = "_period_dt" if mode == "일별" else "기간"
        ui.render_stack_graph(grp, x=x_col, y=unit, color=dim_label, key=chart_key, show_value_in_hover=True)

        long = grp[["기간", dim_label, unit]].rename(columns={unit: "값"})
        pv = ui.build_pivot_table(long, index_col=dim_label, col_col="기간", val_col="값")
        ui.render_table(pv, index_col=dim_label, decimals=0)

    tab_geo_kr, tab_geo, tab_src, tab_mix, tab_dev = st.tabs(["접속권역", "접속지역", "유입매체", "매체X지역", "디바이스"])

    with tab_geo_kr:
        with st.expander("Filter", expanded=True):
            c1, c2, c3, _p = st.columns([1,1,1,2], vertical_alignment="bottom")
            with c1:
                mode = st.radio("기간 단위", ["일별","주별"], index=0, horizontal=True, key="gk_m")
            with c2:
                unit = st.radio("집계 단위", ["유저수","세션수"], index=1, horizontal=True, key="gk_u")
            with c3:
                sel = _select_opt(df, "geo__city_kr", "권역 선택", "gk_s")
            with _p:
                pass
        render_dim_trend(df, mode, unit, "geo__city_kr", "접속권역", CFG["TOPK_DEFAULT"], {"geo__city_kr": sel})

    with tab_geo:
        with st.expander("Filter", expanded=True):
            c1, c2, c3, c4, c5 = st.columns([1,1,1,1,1], vertical_alignment="bottom")
            with c1:
                mode = st.radio("기간 단위", ["일별","주별"], index=0, horizontal=True, key="g_m")
            with c2:
                unit = st.radio("집계 단위", ["유저수","세션수"], index=1, horizontal=True, key="g_u")
            with c3:
                sel_kr = _select_opt(df, "geo__city_kr", "권역 선택", "g_kr")
            with c4:
                sel = _select_opt(df, "geo__city", "지역 선택", "g_c")
            with c5:
                topk = st.selectbox("표시 Top K", CFG["TOPK_OPTS"], index=CFG["TOPK_OPTS"].index(CFG["TOPK_DEFAULT"]), key="g_k")

        render_dim_trend(df, mode, unit, "geo__city", "접속지역", topk, {"geo__city_kr": sel_kr, "geo__city": sel})

    with tab_src:
        with st.expander("Filter", expanded=True):
            c1, c2, c3, c4, c5 = st.columns([1,1,1,1,1], vertical_alignment="bottom")
            with c1:
                mode = st.radio("기간 단위", ["일별","주별"], index=0, horizontal=True, key="s_m")
            with c2:
                unit = st.radio("집계 단위", ["유저수","세션수"], index=1, horizontal=True, key="s_u")
            with c3:
                sel_dim = st.selectbox("유입 단위", ["소스 / 매체","소스","매체","캠페인","컨텐츠"], index=0, key="s_d")
            with c4:
                dim_col, dim_label = _get_src_dim(sel_dim)
                sel = _select_opt(df, dim_col, f"{dim_label} 선택", "s_v")
            with c5:
                topk = st.selectbox("표시 Top K", CFG["TOPK_OPTS"], index=CFG["TOPK_OPTS"].index(CFG["TOPK_DEFAULT"]), key="s_k")

        extra = {} if sel == "전체" else {dim_col: sel}
        render_dim_trend(df, mode, unit, dim_col, dim_label, topk, extra)

    with tab_mix:
        with st.expander("Filter", expanded=True):
            c1, c2, c3, _p, c4, c5, c6 = st.columns([1,1,1,0.2,1,1,1], vertical_alignment="bottom")
            with c1:
                mode = st.radio("기간 단위", ["일별","주별"], index=0, horizontal=True, key="m_m")
            with c2:
                unit = st.radio("집계 단위", ["유저수","세션수"], index=1, horizontal=True, key="m_u")
            with c3:
                sel_src = _select_opt(df, "_sourceMedium", "소스/매체 선택", "m_s")
            with _p:
                pass
            with c4:
                dim_mode = st.radio("권역/지역 선택", ["권역","지역"], index=0, horizontal=True, key="m_d")
            with c5:
                dim_col, dim_label = ("geo__city_kr","접속권역") if dim_mode=="권역" else ("geo__city","접속지역")
                sel = _select_opt(df, dim_col, f"{'권역' if dim_mode=='권역' else '지역'} 선택", "m_v")
            with c6:
                topk = st.selectbox("표시 Top K", CFG["TOPK_OPTS"], index=CFG["TOPK_OPTS"].index(CFG["TOPK_DEFAULT"]), key="m_k")

        extra = {"_sourceMedium": sel_src}
        if sel != "전체":
            extra[dim_col] = sel

        render_dim_trend(df, mode, unit, dim_col, dim_label, topk, extra)

    with tab_dev:
        with st.expander("Filter", expanded=True):
            c1, c2, c3, _p = st.columns([1,1,1,2], vertical_alignment="bottom")
            with c1:
                mode = st.radio("기간 단위", ["일별","주별"], index=0, horizontal=True, key="d_m")
            with c2:
                unit = st.radio("집계 단위", ["유저수","세션수"], index=1, horizontal=True, key="d_u")
            with c3:
                sel = _select_opt(df, "device__category", "디바이스 선택", "d_v")
            with _p:
                pass
        render_dim_trend(df, mode, unit, "device__category", "디바이스", None, {"device__category": sel})

    # ──────────────────────────────────
    # 3) 이벤트 추이
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>이벤트 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ주요 이벤트의 증감 추이를 확인합니다.")

    with st.popover("🤔 유저 VS 세션 VS 이벤트 차이점"):
        st.markdown("""
                    - **유저수** (user_pseudo_id) : 고유 사람수  
                    - **세션수** (pseudo_session_id) : 방문 단위수  
                    - **이벤트수** (view_item) : 방문 안에서 발생한 이벤트 총 횟수  
                    - 사람 A가 1월 1일 오전에 시그니처를 조회 후 이탈, 오후에 시그니처와 허쉬를 재조회했다면,  
                      1월 1일의 **유저수**는 1, **세션수**는 2, **이벤트수**는 3 입니다.
                    - 유저수 ≤ 세션수 ≤ 이벤트수 입니다.
                    """)

    with st.expander("Filter", expanded=True):
        c31, c32 = st.columns([1.3, 2.7], vertical_alignment="bottom")
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

    metrics = pivot_event_overview(df, mode=mode_3, metric_mode=metric_mode_3)

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

    # long3 = metrics.melt(id_vars=["기간"], var_name="지표", value_name="값")
    long3 = metrics.drop(columns=["_period_dt"], errors="ignore").melt(id_vars=["기간"], var_name="지표", value_name="값")
    
    pv3 = ui.build_pivot_table(long3, index_col="지표", col_col="기간", val_col="값")
    ui.render_table(pv3, index_col="지표", decimals=0)


    # ──────────────────────────────────
    # 4) 이벤트 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>이벤트 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ주요 이벤트의 지역 또는 매체별 비중을 확인합니다.")

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
        df_f = df_in.copy()

        # (1) 이벤트 발생 데이터만 대상으로 상세 현황 확인
        if ev_col in df_f.columns:
            df_f[ev_col] = pd.to_numeric(df_f[ev_col], errors="coerce").fillna(0)
            df_f = df_f[df_f[ev_col] > 0]
        else:
            df_f = df_f.iloc[0:0]

        if df_f.empty:
            st.info("선택한 이벤트의 발생 데이터가 없습니다.")
            return

        # (2) 탭별 추가 필터 적용
        if extra_filter:
            for c, v in extra_filter.items():
                if v != "전체" and c in df_f.columns:
                    df_f = df_f[df_f[c] == v]

        tmp = ui.add_period_columns(df_f, "event_date", mode)

        # ✅ 기간 dt 매핑(정렬/샤딩용)
        dt_map = (
            tmp[["_period", "_period_dt"]]
            .drop_duplicates()
            .rename(columns={"_period": "기간"})
        )

        # dim 컬럼 준비
        if dim_col in tmp.columns:
            s = tmp[dim_col].astype(str).replace("nan", "").fillna("").str.strip()
        else:
            s = pd.Series([""] * len(tmp), index=tmp.index)

        # TopK + 기타 처리
        if topk is None:
            tmp["_dim2"] = np.where(s != "", s, "기타")
        else:
            topk_vals = set(ui.get_topk_values(s, topk))
            tmp["_dim2"] = np.where((s != "") & s.isin(topk_vals), s, "기타")

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

        grp = grp.merge(dt_map, on="기간", how="left").sort_values("_period_dt").reset_index(drop=True)

        chart_key = f"event_stack::{ev_col}::{dim_label}::{dim_col}::{mode}::{unit}::{topk}"
        if extra_filter:
            chart_key += "::" + "::".join([f"{k}={v}" for k, v in sorted(extra_filter.items())])

        x_col = "_period_dt" if mode == "일별" else "기간"
        ui.render_stack_graph(grp, x=x_col, y=unit, color=dim_label, key=chart_key, show_value_in_hover=True)

        long = grp[["기간", dim_label, unit]].rename(columns={unit: "값"})
        pv = ui.build_pivot_table(long, index_col=dim_label, col_col="기간", val_col="값")
        ui.render_table(pv, index_col=dim_label, decimals=0)

    tab_e_geo_kr, tab_e_geo, tab_e_src = st.tabs(["접속권역", "접속지역", "유입매체"])

    with tab_e_geo_kr:
        with st.expander("Filter", expanded=True):
            c1, _p1, c2, c3, c4, _p2 = st.columns([1.9, 0.1, 1, 1.5, 1,2], vertical_alignment="bottom")
            with c1:
                sel_ev_label = st.selectbox("이벤트 선택", ev_label_opts, index=0, key="e4_ev_gk")
            with _p1:
                pass
            with c2:
                mode = st.radio("기간 단위", ["일별", "주별"], index=0, horizontal=True, key="e4_m_gk")
            with c3:
                unit = st.radio("집계 단위", ["유저수", "세션수", "이벤트수"], index=1, horizontal=True, key="e4_u_gk")
            with c4:
                sel = _select_opt(df, "geo__city_kr", "권역 선택", "e4_gk_s")
            with _p2:
                pass

        ev_col = ev_label_to_col.get(sel_ev_label, "view_item")
        pivot_event_dim_trend(
            df_in=df,
            ev_col=ev_col,
            mode=mode,
            unit=unit,
            dim_col="geo__city_kr",
            dim_label="접속권역",
            topk=CFG["TOPK_DEFAULT"],
            extra_filter={"geo__city_kr": sel},
        )

    with tab_e_geo:
        with st.expander("Filter", expanded=True):
            c1, _p1, c2, c3, c4, c5, c6 = st.columns([1.9, 0.1, 1, 1.5, 1, 1, 1], vertical_alignment="bottom")
            with c1:
                sel_ev_label = st.selectbox("이벤트 선택", ev_label_opts, index=0, key="e4_ev_g")
            with _p1:
                pass
            with c2:
                mode = st.radio("기간 단위", ["일별", "주별"], index=0, horizontal=True, key="e4_m_g")
            with c3:
                unit = st.radio("집계 단위", ["유저수", "세션수", "이벤트수"], index=1, horizontal=True, key="e4_u_g")
            with c4:
                sel_kr = _select_opt(df, "geo__city_kr", "권역 선택", "e4_g_kr")
            with c5:
                sel = _select_opt(df, "geo__city", "지역 선택", "e4_g_c")
            with c6:
                topk = st.selectbox(
                    "표시 Top K",
                    CFG["TOPK_OPTS"],
                    index=CFG["TOPK_OPTS"].index(CFG["TOPK_DEFAULT"]),
                    key="e4_g_k",
                )

        ev_col = ev_label_to_col.get(sel_ev_label, "view_item")
        extra = {"geo__city_kr": sel_kr, "geo__city": sel}
        pivot_event_dim_trend(
            df_in=df,
            ev_col=ev_col,
            mode=mode,
            unit=unit,
            dim_col="geo__city",
            dim_label="접속지역",
            topk=topk,
            extra_filter=extra,
        )

    with tab_e_src:
        with st.expander("Filter", expanded=True):
            c1, _p1, c2, c3, c4, c5, c6 = st.columns([1.9, 0.1, 1, 1.5, 1, 1, 1], vertical_alignment="bottom")
            with c1:
                sel_ev_label = st.selectbox("이벤트 선택", ev_label_opts, index=0, key="e4_ev_s")
            with _p1:
                pass
            with c2:
                mode = st.radio("기간 단위", ["일별", "주별"], index=0, horizontal=True, key="e4_m_s")
            with c3:
                unit = st.radio("집계 단위", ["유저수", "세션수", "이벤트수"], index=1, horizontal=True, key="e4_u_s")
            with c4:
                sel_dim = st.selectbox("유입 기준", ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"], index=0, key="e4_s_d")
            with c5:
                dim_col, dim_label = _get_src_dim(sel_dim)
                sel = _select_opt(df, dim_col, f"{dim_label} 선택", "e4_s_v")
            with c6:
                topk = st.selectbox(
                    "표시 Top K",
                    CFG["TOPK_OPTS"],
                    index=CFG["TOPK_OPTS"].index(CFG["TOPK_DEFAULT"]),
                    key="e4_s_k",
                )

        ev_col = ev_label_to_col.get(sel_ev_label, "view_item")
        extra = {} if sel == "전체" else {dim_col: sel}
        pivot_event_dim_trend(
            df_in=df,
            ev_col=ev_col,
            mode=mode,
            unit=unit,
            dim_col=dim_col,
            dim_label=dim_label,
            topk=topk,
            extra_filter=extra,
        )



if __name__ == "__main__":
    main()
