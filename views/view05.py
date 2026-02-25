# SEOHEE
# 2026-02-23 ver.

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import importlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys
import plotly.express as px

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery

import modules.ui_common as ui
importlib.reload(ui)

import modules.discrimination as di
importlib.reload(di)


# ──────────────────────────────────
# CONFIG
# ──────────────────────────────────
CFG = {
    "TZ": "Asia/Seoul",
    "CACHE_TTL": 3600,
    "DEFAULT_LOOKBACK_DAYS": 14,
    "HEADER_UPDATE_AM": 850,
    "HEADER_UPDATE_PM": 1535,
    
    "TOPK_OPTS": [5, 10, 15, 20],
    "OPTS_PERIOD": ["일별", "주별"],
    "OPTS_PATH": ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"],

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
    group_cols: list[str] | None = None,
    unit: str = "session",   # "session" | "user"
) -> pd.DataFrame:
    w = ui.add_period_columns(df, "event_date", mode)
    g = group_cols or []
    cols = ["_period"] + g

    # 전체 유저/세션 모수 (flag와 별도로 계산)
    base_cnt = (
        w.groupby(cols, as_index=False)
         .agg(
            유저수=("user_pseudo_id", "nunique"),
            세션수=("pseudo_session_id", "nunique"),
         )
    )

    # 이벤트행 → 세션행으로 압축 (flag)
    sess = (
        w.groupby(cols + ["pseudo_session_id"], as_index=False)
        .agg(
            user_pseudo_id=("user_pseudo_id", "first"),
            is_sessionStart=("is_sessionStart", "max"),
            is_viewOnly=("is_viewOnly", "max"),
            flag_type=("flag_type", "first"),  # 재사용
        )
    )

    # flag_type별 집계 단위 선택 (세션수 or 유저수)
    vcol = "pseudo_session_id" if unit == "session" else "user_pseudo_id"
    agg = (
        sess.groupby(cols + ["flag_type"], as_index=False)
            .agg(cnt=(vcol, "nunique"))
    )

    out = (
        agg.pivot_table(index=cols, columns="flag_type", values="cnt", aggfunc="sum", fill_value=0)
           .reset_index()
           .rename(columns={"_period": "기간"})
    )
    out = out.merge(base_cnt.rename(columns={"_period": "기간"}), on="기간", how="left")

    for c in ["CMP 랜딩+", "CMP 랜딩-", "CMP 경유", "기타"]:
        if c not in out.columns:
            out[c] = 0

    # 기간 정렬/표시
    dt_map = _build_dt_map(w).rename(columns={"_period": "기간"})
    out = out.merge(dt_map, on="기간", how="left").sort_values("_period_dt").reset_index(drop=True)

    return out


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
    # B) Sidebar / Filter
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
        df = bq.get_data("tb_sleeper_f_cmp")

        # 기간 필터
        df = df[(df["event_date"] >= pd.to_datetime(cs)) & (df["event_date"] < pd.to_datetime(ce))]

        def _safe_str_col(colname: str) -> pd.Series:
            if colname in df.columns:
                s = df[colname]
            else:
                s = pd.Series([""] * len(df), index=df.index)
            return s.astype(str).replace("nan", "").fillna("").str.strip()

        # 파생 컬럼 (1) 유입 경로
        df["_source"] = _safe_str_col("collected_traffic_source__manual_source").replace("", "(not set)")
        df["_medium"] = _safe_str_col("collected_traffic_source__manual_medium").replace("", "(not set)")
        df["_campaign"] = _safe_str_col("collected_traffic_source__manual_campaign_name").replace("", "(not set)")
        df["_content"] = _safe_str_col("collected_traffic_source__manual_content").replace("", "(not set)")
        df["_sourceMedium"] = df["_source"] + " / " + df["_medium"]

        return df

    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df = load_data(cs, ce)

    # ✅ (NEW) 세션 단위 flag_type 1회 생성 + df에 붙이기
    sess_flag = (
        df.groupby(["event_date", "pseudo_session_id"], as_index=False)
        .agg(
            user_pseudo_id=("user_pseudo_id", "first"),
            is_sessionStart=("is_sessionStart", "max"),
            is_viewOnly=("is_viewOnly", "max"),
        )
    )
    sess_flag["flag_type"] = np.select(
        [
            (sess_flag["is_sessionStart"] == 1) & (sess_flag["is_viewOnly"] == 1),
            (sess_flag["is_sessionStart"] == 1) & (sess_flag["is_viewOnly"] == 0),
            (sess_flag["is_sessionStart"] == 0) & (sess_flag["is_viewOnly"] == 1),
        ],
        ["CMP 랜딩+", "CMP 랜딩-", "CMP 경유"],
        default="기타",
    )
    df = df.merge(
        sess_flag[["event_date", "pseudo_session_id", "flag_type"]],
        on=["event_date", "pseudo_session_id"],
        how="left",
    )

    # ──────────────────────────────────
    # D) Header
    # ──────────────────────────────────
    st.subheader("CMP 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px; line-height:1.5;">
            GA 기준 <br>
            </div>
            <div style="color:#6c757d; font-size:14px; line-height:2.0;">
            ※ GA D-1 데이터의 세션 수치는 <b>오전에 1차</b> 집계되나 , 세션의 유입출처는 <b>오후에 2차</b> 반영됩니다.
            </div>
            """,
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
            <a href="?refresh=1" title="캐시 초기화" style="text-decoration:none;vertical-align:middle;">
                <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 8px;font-size:13px;line-height:1;
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
    # 1) CMP 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>CMP 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명")

    with st.popover("🤔 CMP 랜딩 VS CMP 경유"):
        st.markdown("""
    - **CMP 랜딩-**  
    세션이 **CMP 페이지에서 시작**해요.  
    이후 다른 페이지로 이동해 탐색하지만, **CMP로 다시 돌아오지는 않아요**.


    - **CMP 랜딩+**  
    세션이 **CMP 페이지에서 시작**해요.  
    다른 페이지를 둘러본 뒤, **CMP를 다시 한 번 방문**해요.


    - **CMP 경유**  
    세션은 **다른 페이지에서 시작**해요.  
    탐색 중에 **CMP 페이지를 한 번 이상 방문**해요.
    """)

    # 필터
    with st.expander("Filter", expanded=False):
        r0_1, r0_2 = st.columns([1.3, 2.7], vertical_alignment="bottom")
        with r0_1:
            mode_1 = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_1")
        with r0_2:
            sel_unit_1 = st.radio(
                "집계 단위",
                ["유저수", "세션수"],
                horizontal=True,
                index=1,
                key="unit_1",
            )
    unit_1 = "session" if sel_unit_1 == "세션수" else "user"
    base1 = pivot_period_usersessions(df, mode=mode_1, unit=unit_1)
    
    # ✅ CMP 유입수 컬럼을 base1에 먼저 생성 (line/bar/표 모두 재사용)
    base1["CMP 유입수"] = (
        base1[["CMP 랜딩-", "CMP 랜딩+", "CMP 경유"]]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .sum(axis=1)
    )

    x_dt = base1["_period_dt"].dt.to_pydatetime()
    tick_text = base1["기간"].astype(str).tolist()

    fig = go.Figure()
    
    # bar 데이터
    d_bar = base1.copy()
    d_bar["_bar_total"] = (d_bar["CMP 랜딩+"] + d_bar["CMP 랜딩-"] + d_bar["CMP 경유"]).replace(0, np.nan)
    d_bar["_share_both"] = (d_bar["CMP 랜딩+"] / d_bar["_bar_total"] * 100).round(1).fillna(0)
    d_bar["_share_land"] = (d_bar["CMP 랜딩-"]   / d_bar["_bar_total"] * 100).round(1).fillna(0)
    d_bar["_share_thru"] = (d_bar["CMP 경유"]   / d_bar["_bar_total"] * 100).round(1).fillna(0)

    fig.add_bar(
        x=x_dt, y=d_bar["CMP 랜딩-"], name="CMP 랜딩-", opacity=0.6,
        customdata=np.stack([d_bar["_share_land"], d_bar["CMP 랜딩-"]], axis=1),
        hovertemplate="CMP 랜딩-<br>비중: %{customdata[0]}%<br>값: %{customdata[1]:,.0f}<extra></extra>"
    )
    fig.add_bar(
        x=x_dt, y=d_bar["CMP 랜딩+"], name="CMP 랜딩+", opacity=0.6,
        customdata=np.stack([d_bar["_share_both"], d_bar["CMP 랜딩+"]], axis=1),
        hovertemplate="CMP 랜딩+<br>비중: %{customdata[0]}%<br>값: %{customdata[1]:,.0f}<extra></extra>"
    )
    fig.add_bar(
        x=x_dt, y=d_bar["CMP 경유"], name="CMP 경유", opacity=0.6,
        customdata=np.stack([d_bar["_share_thru"], d_bar["CMP 경유"]], axis=1),
        hovertemplate="CMP 경유<br>비중: %{customdata[0]}%<br>값: %{customdata[1]:,.0f}<extra></extra>"
    )

    # ✅ line 데이터: CMP 유입수 표시
    fig.add_scatter(
        x=x_dt,
        y=base1["CMP 유입수"],
        name="CMP 유입수",
        mode="lines+markers",
        hovertemplate="CMP 유입수<br>값: %{y:,.0f}<extra></extra>",
    )

    # shading: 일별만
    if mode_1 == "일별":
        ui.add_weekend_shading(fig, base1["_period_dt"])

    # 여기서 "축 중복"을 끝내는 핵심: tick을 우리가 박아버림
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

    den = pd.to_numeric(tbl1[sel_unit_1], errors="coerce").replace(0, np.nan)

    # ✅ 이미 base1에서 만든 CMP 유입수 재사용
    tbl1["CMP 유입수(%)"] = (pd.to_numeric(tbl1["CMP 유입수"], errors="coerce") / den * 100).round(1).fillna(0)

    show_metrics_1 = [
        sel_unit_1,
        "CMP 유입수", "CMP 유입수(%)",
        "CMP 랜딩-", "CMP 랜딩+", "CMP 경유",
    ]

    long1 = (
        tbl1[["기간"] + show_metrics_1]
        .melt(id_vars=["기간"], var_name="지표", value_name="값")
    )

    long1["지표"] = pd.Categorical(long1["지표"], categories=show_metrics_1, ordered=True)
    long1["값"] = pd.to_numeric(long1["값"], errors="coerce").fillna(0)

    pv1 = (
        long1
        .pivot_table(index="지표", columns="기간", values="값", aggfunc="sum", fill_value=0)
        .reset_index()
    )

    val_cols = pv1.columns[1:]
    pv1[val_cols] = pv1[val_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    styled = pv1.style.format("{:,.0f}", subset=val_cols)
    pct_mask = pv1["지표"].eq("CMP 유입수(%)")
    if pct_mask.any():
        styled = styled.format("{:.1f}", subset=pd.IndexSlice[pct_mask, val_cols])

    st.dataframe(styled, row_height=30, hide_index=True)


    # ──────────────────────────────────
    # 2) CMP 유입매체
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>CMP 유입매체</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명")
    
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
        return "_content", "컨텐츠"

    def render_dim_trend(
        df_in: pd.DataFrame,
        mode: str,
        unit: str,
        dim_col: str,
        dim_label: str,
        topk: int | None,
        extra_filter: dict[str, str] | None = None,
        flag_sel: list[str] | None = None,   # ✅ 추가
    ):
        df_f = df_in
        if extra_filter:
            for c, v in extra_filter.items():
                if v != "전체" and c in df_f.columns:
                    df_f = df_f[df_f[c] == v]

        tmp = ui.add_period_columns(df_f, "event_date", mode)
        dt_map = _build_dt_map(tmp)

        s_dim = _safe_dim_series(tmp, dim_col)
        tmp["_dim_raw"] = s_dim

        sess = (
            tmp.groupby(["_period", "pseudo_session_id"], dropna=False, as_index=False)
            .agg(
                user_pseudo_id=("user_pseudo_id", "first"),
                _dim_raw=("_dim_raw", "first"),
                is_sessionStart=("is_sessionStart", "max"),
                is_viewOnly=("is_viewOnly", "max"),
                flag_type=("flag_type", "first"),  # 재사용
            )
        )

        # ✅ (NEW) flag_type 멀티 선택 필터
        if flag_sel:
            sess = sess[sess["flag_type"].isin(flag_sel)]

        if dim_col == "flag_type":
            sess["_dim_raw"] = sess["flag_type"]

        sess["_dim2"] = _apply_topk_bucket(sess["_dim_raw"], topk)

        grp = (
            sess.groupby(["_period", "_dim2"], dropna=False)
                .agg(
                    세션수=("pseudo_session_id", "nunique"),
                    유저수=("user_pseudo_id", "nunique"),
                )
                .reset_index()
                .rename(columns={"_period": "기간", "_dim2": dim_label})
        )

        # 동일?
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
            ui.render_stack_graph(grp, x="기간", y=unit, color=dim_label, key=chart_key, show_value_in_hover=True)

        long = grp[["기간", dim_label, unit]].rename(columns={unit: "값"})
        pv = ui.build_pivot_table(long, index_col=dim_label, col_col="기간", val_col="값")

        styled = ui.style_format(pv, decimals_map={c: 0 for c in pv.columns if c != dim_label})
        st.dataframe(styled, row_height=30, hide_index=True)

    # 탭 없이 
    with st.expander("Filter", expanded=True):
        c1, c2, c3, c4, c5, c6 = st.columns([1, 1, 1.8, 1, 1, 1], vertical_alignment="bottom")
        with c1:
            mode = st.radio("기간 단위", ["일별", "주별"], index=0, horizontal=True, key="s_m")
        with c2:
            unit = st.radio("집계 단위", ["유저수", "세션수"], index=1, horizontal=True, key="s_u")
        with c3:
            flag_sel = st.multiselect(  # ✅ 추가
                "CMP 유형",
                options=["CMP 랜딩+", "CMP 랜딩-", "CMP 경유", "기타"],
                default=["CMP 랜딩+", "CMP 랜딩-", "CMP 경유"],
                key="s_flag",
            )
        with c4:
            sel_dim = st.selectbox("유입 단위", ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"], index=0, key="s_d")
        with c5:
            dim_col, dim_label = _get_src_dim(sel_dim)
            sel = _select_opt(df, dim_col, f"{dim_label} 선택", "s_v")
        with c6:
            topk = st.selectbox("표시 Top K", CFG["TOPK_OPTS"], index=0, key="s_k")


    extra = {} if sel == "전체" else {dim_col: sel}
    render_dim_trend(df, mode, unit, dim_col, dim_label, topk, extra, flag_sel=flag_sel)  # ✅ 인자 추가


    # ──────────────────────────────────
    # 3) 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>페이지 내 Action</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    # 필터
    with st.expander("Filter", expanded=True):
        f1, f2, f3, f4 = st.columns([1, 2, 1.2, 1.2], vertical_alignment="bottom")

        with f1:
            cmp_unit = st.radio(
                "집계 단위",
                ["유저수", "세션수"],
                index=1,  # ✅ 기본: 세션수
                horizontal=True,
                key="cmp_unit",
            )

        with f2:
            flag_sel_cmp = st.multiselect(
                "CMP 유형",
                options=["CMP 랜딩+", "CMP 랜딩-", "CMP 경유", "기타"],
                default=["CMP 랜딩+", "CMP 랜딩-", "CMP 경유"],
                key="cmp_flag_sel",
            )

        with f3:
            sel_dim_cmp = st.selectbox(
                "유입 단위",
                ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"],
                index=0,
                key="cmp_dim",
            )

        with f4:
            dim_col_cmp, dim_label_cmp = _get_src_dim(sel_dim_cmp)
            sel_cmp = _select_opt(df, dim_col_cmp, f"{dim_label_cmp} 선택", "cmp_dim_v")

    # CMP 세션 (sess_flag 재사용)
    sess_cmp = sess_flag[(sess_flag["is_sessionStart"] == 1) | (sess_flag["is_viewOnly"] == 1)][
        ["event_date", "pseudo_session_id", "user_pseudo_id", "flag_type"]
    ]
    if flag_sel_cmp:
        sess_cmp = sess_cmp[sess_cmp["flag_type"].isin(flag_sel_cmp)]

    # 유입 경로 필터 적용
    if sel_cmp != "전체":
        if dim_col_cmp in df.columns:
            base_key = df[["event_date", "pseudo_session_id", dim_col_cmp]].drop_duplicates()
            sess_cmp = sess_cmp.merge(base_key, on=["event_date", "pseudo_session_id"], how="left")
            sess_cmp = sess_cmp[sess_cmp[dim_col_cmp].astype(str) == str(sel_cmp)]
        else:
            sess_cmp = sess_cmp.iloc[0:0]

    tot_cmp_sess = int(sess_cmp["pseudo_session_id"].nunique())
    tot_cmp_user = int(sess_cmp["user_pseudo_id"].nunique())

    # 이벤트행(df) 중 CMP 세션만 (✅ 중복컬럼 제거: 키만으로 inner join 효과)
    df_cmp = df.merge(
        sess_cmp[["event_date", "pseudo_session_id"]],
        on=["event_date", "pseudo_session_id"],
        how="inner"
    )

    # ✅ 집계 키/분모 선택
    cnt_key = "pseudo_session_id" if cmp_unit == "세션수" else "user_pseudo_id"
    tot_cnt = tot_cmp_sess if cmp_unit == "세션수" else tot_cmp_user

    # 1~2. Scroll Depth
    m_scroll = df_cmp["event_name"].astype(str).str.match(
        r"^everyone_page_scroll_(10|20|30|40|50|60|70|80|90|100)$", na=False
    )
    d_scroll = df_cmp.loc[m_scroll, ["pseudo_session_id", "user_pseudo_id", "event_name"]]

    if tot_cmp_sess == 0:
        df_scroll_max = pd.DataFrame(columns=["pseudo_session_id", "user_pseudo_id", "max_depth"])
        df_scroll_dist = pd.DataFrame(columns=["max_depth", "집계수", "비중(%)"])
        df_scroll_funnel = pd.DataFrame(columns=["depth", "집계수", "비중(%)", "스크롤발생대비(%)", "직전구간이탈(%)"])
        scroll_event_sess = 0
        scroll_event_user = 0
    else:
        if d_scroll.empty:
            df_scroll_max = pd.DataFrame(columns=["pseudo_session_id", "user_pseudo_id", "max_depth"])
            scroll_event_sess = 0
            scroll_event_user = 0
        else:
            d_scroll["depth"] = (
                d_scroll["event_name"].astype(str)
                .str.extract(r"^everyone_page_scroll_(\d+)$", expand=False)
                .fillna("0").astype(int)
            )
            # ✅ 세션별 최대 depth + 해당 세션의 user_pseudo_id 유지
            df_scroll_max = (
                d_scroll.groupby("pseudo_session_id", as_index=False)
                        .agg(
                            user_pseudo_id=("user_pseudo_id", "first"),
                            max_depth=("depth", "max")
                        )
            )
            scroll_event_sess = int(df_scroll_max["pseudo_session_id"].nunique())
            scroll_event_user = int(df_scroll_max["user_pseudo_id"].nunique())

        # ✅ 분포: max_depth=0 제외 (요청 유지)
        if df_scroll_max.empty:
            df_scroll_dist = pd.DataFrame(columns=["max_depth", "집계수", "비중(%)"])
        else:
            df_scroll_dist = (
                df_scroll_max.groupby("max_depth", as_index=False)
                            .agg(집계수=(cnt_key, "nunique"))
                            .sort_values("max_depth")
                            .reset_index(drop=True)
            )
            df_scroll_dist["비중(%)"] = (df_scroll_dist["집계수"] / max(tot_cnt, 1) * 100).round(2)

        # Funnel: depth 이상 도달 집계수
        levels = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        if df_scroll_max.empty:
            df_scroll_funnel = pd.DataFrame({"depth": levels, "집계수": [0] * len(levels)})
        else:
            reach = [int(df_scroll_max.loc[df_scroll_max["max_depth"] >= lv, cnt_key].nunique()) for lv in levels]
            df_scroll_funnel = pd.DataFrame({"depth": levels, "집계수": reach})

        df_scroll_funnel["비중(%)"] = (df_scroll_funnel["집계수"] / max(tot_cnt, 1) * 100).round(2)

        # 스크롤발생대비 분모도 unit에 맞춤
        den_scroll = scroll_event_sess if cmp_unit == "세션수" else scroll_event_user
        df_scroll_funnel["스크롤발생대비(%)"] = (df_scroll_funnel["집계수"] / max(den_scroll, 1) * 100).round(2)

        prev = df_scroll_funnel["집계수"].shift(1)
        df_scroll_funnel["직전구간이탈(%)"] = (
            (prev - df_scroll_funnel["집계수"]) / prev.replace(0, np.nan) * 100
        ).round(2).fillna(0)

    # 3. CTA(click_cta_everyone): type/text별 집계
    base_total = tot_cmp_sess if cmp_unit == "세션수" else tot_cmp_user
    base_label = "세션" if cmp_unit == "세션수" else "유저"

    m_cta = df_cmp["event_name"].astype(str).eq("click_cta_everyone")
    d_cta = df_cmp.loc[m_cta, ["pseudo_session_id", "user_pseudo_id", "cta_type", "cta_text", "event_cnt"]]
    # d_cta["event_cnt"] = pd.to_numeric(d_cta["event_cnt"], errors="coerce").fillna(0)

    if d_cta.empty:
        df_cta_type = pd.DataFrame(columns=["cta_type", "집계수"])
        df_cta_text = pd.DataFrame(columns=["cta_text", "집계수"])
    else:
        d_cta["cta_type"] = d_cta["cta_type"].astype(str).replace("nan", "").fillna("").str.strip()
        d_cta["cta_text"] = d_cta["cta_text"].astype(str).replace("nan", "").fillna("").str.strip()
        d_cta.loc[d_cta["cta_type"].eq(""), "cta_type"] = "(not set)"
        d_cta.loc[d_cta["cta_text"].eq(""), "cta_text"] = "(not set)"

        df_cta_type = (
            d_cta.groupby("cta_type", as_index=False)
                .agg(집계수=(cnt_key, "nunique"))
                .sort_values("집계수", ascending=False)
                .reset_index(drop=True)
        )
        df_cta_type["비중(%)"] = (df_cta_type["집계수"] / max(base_total, 1) * 100).round(2)
        df_cta_type = df_cta_type[["cta_type", "집계수", "비중(%)"]]

    # 렌더링
    st.info(f"선택된 {base_label} 모수는ㅤ**{base_total:,}**ㅤ입니다. ") # 숫자 양쪽으로 넓은 공백 붙어있음 
    st.markdown(" ")
 
    c1, _p, c2 = st.columns([1, 0.03, 1], vertical_alignment="top")
    with c1:
        st.markdown("""
                    <h6 style="margin:0;">📊 Scroll 1. 최대 도달 뎁스</h6>
                    <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">사용자가 최종적으로 어디까지 내려서 확인했는지 보여줍니다.</p>
                    """,
                    unsafe_allow_html=True)    
        # 표!

        top2 = df_scroll_dist.nlargest(2, "비중(%)").sort_values("비중(%)", ascending=False)
        d1 = top2.iloc[0]["max_depth"]
        d2 = top2.iloc[1]["max_depth"]
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_scroll_dist["max_depth"],
            y=df_scroll_dist["비중(%)"],
            mode="markers+lines",
            fill="tozeroy",
            opacity=0.5,
            hovertemplate="Max Depth %{x}%<br>비중 %{y:.2f}%<extra></extra>"  # ← 추가
        ))
        # ── 추가 1) 세로 점선
        fig.add_vline(x=d1, line_dash="dot", line_width=1)
        fig.add_vline(x=d2, line_dash="dot", line_width=1)
        # ── 추가 2) 어노테이션
        fig.update_layout(
            annotations=[
                dict(x=d1, y=1.05, xref="x", yref="paper", text="도달 1위", showarrow=False, font=dict(size=11)),
                dict(x=d2, y=1.05, xref="x", yref="paper", text="도달 2위", showarrow=False, font=dict(size=11)),
            ]
        )
        fig.update_layout(height=150, margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)


        st.dataframe(df_scroll_dist, use_container_width=True, row_height=30, hide_index=True)
    with _p: pass
    with c2:
        st.markdown("""
                    <h6 style="margin:0;">📊 Scroll 2. 구간별 이탈률</h6>
                    <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">사용자가 더 보지 않고 어디에서 멈추는지 보여줍니다.</p>
                    """,
                    unsafe_allow_html=True)   
        # 표!
        fig, meta = di.build_scroll_exit_fig(
            df_scroll_funnel,
            col_depth="depth",
            col_rate="직전구간이탈(%)",
        )
        st.plotly_chart(fig, use_container_width=True)        
        st.dataframe(df_scroll_funnel, use_container_width=True, row_height=30, hide_index=True)

    c3, _p, c4 = st.columns([1, 0.03, 1], vertical_alignment="top")
    with c3:
        st.markdown("""
                    <h6 style="margin:0;">📊 CTA 1. TYPE별 클릭률</h6>
                    <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">CTA를 "유형" 기준으로 묶어, 반응도를 확인합니다.
                    (* 체크박스를 클릭하여 선택한 유형의 TEXT만 확인합니다.) </p>
                    """,
                    unsafe_allow_html=True)   

        # --- CTA TYPE 한글 라벨 매핑 (표시용) ---
        CTA_TYPE_KR = {
            "cta_nav"          : "네비게이터",
            "cta"              : "CTA 원형배너",
            "cta_product_sub"  : "MD픽",
            "cta_product_main" : "자세히보기 사각배너",
            
            "cta_showroom"      : "(이전) 쇼룸 예약하기",
            "cta_promo_event"   : "(이전) 프로모션 이벤트배너",
            "cta_promo_wedding" : "(이전) 프로모션 웨딩배너",
            "cta_promo_campaign": "(이전) 프로모션 캠페인배너",
        }
        CTA_TYPE_KR_REV = {v: k for k, v in CTA_TYPE_KR.items()}

        df_cta_type_edit = df_cta_type.copy()

        # ✅ 컬럼명은 cta_type 그대로, 값만 한글로 치환(표시용)
        df_cta_type_edit["cta_type"] = (
            df_cta_type_edit["cta_type"].astype(str)
            .map(CTA_TYPE_KR)
            .fillna(df_cta_type_edit["cta_type"].astype(str))
        )

        df_cta_type_edit["유형 선택"] = False

        edited = st.data_editor(
            df_cta_type_edit[["cta_type", "집계수", "비중(%)", "유형 선택"]],
            use_container_width=True,
            hide_index=True,
            row_height=30,
            key="cmp_cta_type_editor",
            column_config={
                "유형 선택": st.column_config.CheckboxColumn("유형 선택"),
            },
            disabled=["cta_type", "집계수", "비중(%)"],
        )

        # ✅ CTA2용: 체크된 한글 라벨 → 원본 key 로 복원
        sel_types_kr = edited.loc[edited["유형 선택"], "cta_type"].astype(str).tolist()
        sel_types = [CTA_TYPE_KR_REV.get(v, v) for v in sel_types_kr]

    # 유형 선택 type 추출 ✅ df_cta_text는 항상 정의되게 (유형 선택 없으면 전체)
    if sel_types:   # ✅ 위에서 이미 "원본키"로 복원된 sel_types 사용
        _src = d_cta[d_cta["cta_type"].isin(sel_types)]
    else:
        _src = d_cta

    # ✅ df_cta_text는 항상 정의되게 (유형 선택 없으면 전체)
    if sel_types:
        _src = d_cta[d_cta["cta_type"].isin(sel_types)]
    else:
        _src = d_cta

    df_cta_text = (
        _src.groupby("cta_text", as_index=False)
            .agg(
                집계수=(cnt_key, "nunique"),
                이벤트수=("event_cnt", "sum")
            )
            .sort_values("집계수", ascending=False)
            .reset_index(drop=True)
    )

    # 전체 대비 비중
    df_cta_text["비중(%)"] = (
        df_cta_text["집계수"] / max(base_total, 1) * 100
    ).round(2)

    # Type 내 비중
    type_total = df_cta_text["집계수"].sum()
    df_cta_text["유형내비중(%)"] = (
        df_cta_text["집계수"] / max(type_total, 1) * 100
    ).round(2)

    # 평균 클릭수
    df_cta_text["평균클릭수"] = (
        df_cta_text["이벤트수"] / df_cta_text["집계수"].replace(0, np.nan)
    ).round(2).fillna(0)

    # 컬럼 정리
    df_cta_text = df_cta_text[
        ["cta_text", "집계수", "비중(%)", "유형내비중(%)", "이벤트수", "평균클릭수"]
    ]    
    # -------------------------

    with _p: pass
    with c4:
        st.markdown("""
                    <h6 style="margin:0;">📊 CTA 2. TEXT별 클릭률</h6>
                    <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">유형별 "문구" 기준으로 풀어, 반응도를 확인합니다.
                    (* 체크박스를 클릭하여 선택한 유형의 TEXT만 확인합니다.)</p>
                    """,
                    unsafe_allow_html=True)   

        # 표!

        st.dataframe(df_cta_text, use_container_width=True, hide_index=True, row_height=30, height=216)
        



    # ──────────────────────────────────
    # 4) 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>이동 페이지 Action (TBD)</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤFlow without sequence", unsafe_allow_html=True)
    st.header(" ");st.header(" ");st.header(" ")

    # m_every_scroll = df_cmp["event_name"].astype(str).str.match(
    #     r"^everyone_page_scroll_(10|20|30|40|50|60|70|80|90|100)$", na=False
    # )
    # d_evt = df_cmp.loc[~m_every_scroll, ["event_name", "pseudo_session_id", "event_cnt", "product_no", "page_location"]]
    # d_evt["event_cnt"] = pd.to_numeric(d_evt["event_cnt"], errors="coerce").fillna(0)

    # df_evt_top = (
    #     d_evt.groupby("event_name", as_index=False)
    #         .agg(
    #             세션수=("pseudo_session_id", "nunique"),
    #             이벤트수=("event_cnt", "sum"),
    #         )
    #         .sort_values(["세션수", "이벤트수"], ascending=False)
    #         .reset_index(drop=True)
    # )

    # d_prod = d_evt.copy()
    # d_prod["product_no"] = d_prod["product_no"].astype(str).replace("nan", "").fillna("").str.strip()
    # d_prod = d_prod[d_prod["product_no"].ne("")]

    # df_prod_top = (
    #     d_prod.groupby("product_no", as_index=False)
    #         .agg(
    #             세션수=("pseudo_session_id", "nunique"),
    #             이벤트수=("event_cnt", "sum"),
    #         )
    #         .sort_values(["세션수", "이벤트수"], ascending=False)
    #         .reset_index(drop=True)
    # )

    # # (추가) 관심 제품 vs 전환 제품 (event_name 기준 분리)
    # evt_interest = ["view_item", "select_item", "view_item_list", "product_option_price", "product_page_scroll_50"]
    # evt_convert = ["add_to_cart", "view_cart", "begin_checkout", "purchase_button_click", "purchase"]

    # d_prod_evt = d_prod.copy()
    # d_prod_evt["event_name"] = d_prod_evt["event_name"].astype(str)

    # d_interest = d_prod_evt[d_prod_evt["event_name"].isin(evt_interest)]
    # d_convert = d_prod_evt[d_prod_evt["event_name"].isin(evt_convert)]

    # df_prod_interest = (
    #     d_interest.groupby("product_no", as_index=False)
    #             .agg(
    #                 세션수=("pseudo_session_id", "nunique"),
    #                 이벤트수=("event_cnt", "sum"),
    #             )
    #             .sort_values(["세션수", "이벤트수"], ascending=False)
    #             .reset_index(drop=True)
    # )

    # df_prod_convert = (
    #     d_convert.groupby("product_no", as_index=False)
    #             .agg(
    #                 세션수=("pseudo_session_id", "nunique"),
    #                 이벤트수=("event_cnt", "sum"),
    #             )
    #             .sort_values(["세션수", "이벤트수"], ascending=False)
    #             .reset_index(drop=True)
    # )

    # c1, c2 = st.columns([1, 1], vertical_alignment="top")
    # with c1:
    #     st.markdown("**CMP 세션 내 이벤트 Top (스크롤 제외)**")
    #     st.dataframe(df_evt_top.head(50), use_container_width=True, hide_index=True)
    # with c2:
    #     st.markdown("**CMP 세션 내 product_no Top (전체)**")
    #     st.dataframe(df_prod_top.head(50), use_container_width=True, hide_index=True)

    # c3, c4 = st.columns([1, 1], vertical_alignment="top")
    # with c3:
    #     st.markdown("**관심 제품 Top (view_item/option/scroll 등)**")
    #     st.dataframe(df_prod_interest.head(50), use_container_width=True, hide_index=True)
    # with c4:
    #     st.markdown("**전환 제품 Top (add_to_cart/view_cart/purchase 등)**")
    #     st.dataframe(df_prod_convert.head(50), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()