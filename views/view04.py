# SEOHEE
# 2026-03-10 ver. (Refactored & Split Layout for Funnel)

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go 
import json

import modules.ui_common as ui
importlib.reload(ui)

from google.oauth2.service_account import Credentials
import gspread


# ──────────────────────────────────
# CONFIG
# ──────────────────────────────────
CFG = {
    "TZ": "Asia/Seoul",
    "CACHE_TTL": 3600,
    "DEFAULT_LOOKBACK_DAYS": 14,
    
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

COLS_CATEGORICAL = [
    "shrm_type", "shrm_region", "shrm_branch", "demo_gender", "demo_age", "awareness_type", "purchase_purpose", "visit_type"]

COLS_SHOWROOM = [
    "event_date", "shrm_name", "shrm_type", "shrm_region", "shrm_branch"]

COLS_BASICSRC = [
    "look_cnt", "bookreq_cnt", "res_cnt"]

COLS_TOTALSRC = [
    "look_cnt", "bookreq_cnt", "res_cnt", "visit_reserved", "visit_walkin"]

COLS_TOTAL    = [
    "날짜", "look_cnt", "bookreq_cnt", "bookreq_rate", "res_cnt","visit_total", "visit_reserved", "visit_reserved_rate", "noshow_rate","visit_walkin", "visit_walkin_rate",]

COLS_LABELMAP = {
    "look_cnt": "조회수",
    "bookreq_cnt": "예약신청수",
    "bookreq_rate": "예약신청률 (%)",
    "res_cnt": "예약이용수",
    "visit_total": "방문수",
    "visit_reserved": "방문수(예약)",
    "visit_walkin": "방문수(워크인)",
    "visit_reserved_rate": "예약 비중 (%)",
    "visit_walkin_rate": "워크인 비중 (%)",
    "noshow_rate": "노쇼 비중 (%)",}


# ──────────────────────────────────
# HELPER (Common)
# ──────────────────────────────────
def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())

def _safe_rate(n: float, d: float) -> float:
    return (n / d * 100.0) if d > 0 else 0.0

def _standardize_df(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """필요한 컬럼만 추출하여 문자열 정제 및 공백 제거를 수행합니다."""
    if df is None or df.empty: 
        return pd.DataFrame()
    
    valid_cols = [c for c in cols if c in df.columns]
    d = df[valid_cols].copy()
    
    for c in valid_cols:
        if c == "weight":
            d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)
        else:
            d[c] = d[c].astype(str).replace(["None", "nan", "<NA>"], "").str.strip()
            
    return d

def _get_age_order(vals: list[str]) -> list[str]:
    base = ["20대", "30대", "40대", "50대", "60대 이상"]
    base_in = [x for x in base if x in vals]
    return base_in + [x for x in sorted(vals) if x not in base_in]

def parse_shrm(df: pd.DataFrame) -> pd.DataFrame:
    if "shrm_name" in df.columns:
        ss = df["shrm_name"].astype("string").fillna("").astype(str).str.strip()
        parts = ss.str.split("_", n=2, expand=True)
        df["shrm_type"] = parts[0].fillna("").str.strip()
        df["shrm_branch"] = parts[1].fillna("").str.strip()
        df["shrm_region"] = parts[2].fillna("").str.strip()
    else:
        df["shrm_type"], df["shrm_branch"], df["shrm_region"] = "", "", ""
    return df

def dim_options(df: pd.DataFrame, col: str, *, all_label: str = "전체") -> list[str]:
    if df is None or df.empty or col not in df.columns:
        return [all_label]
    s = df[col].astype("string").fillna("").astype(str).str.strip()
    s = s[s != ""]
    unique_vals = s.unique().tolist()
    sorted_vals = sorted([str(x) for x in unique_vals])
    return [all_label] + sorted_vals

def filter_by_dim(df: pd.DataFrame, dim_col: str, dim_val, *, all_label: str = "전체") -> pd.DataFrame:
    if df is None or df.empty: return df
    if dim_col not in df.columns: return df

    s = df[dim_col].astype("string").fillna("").astype(str).str.strip()

    if isinstance(dim_val, (list, tuple, set)):
        vals = [str(x).strip() for x in dim_val if str(x).strip() and str(x).strip() != all_label]
        if not vals: return df
        return df[s.isin(vals)].copy()

    if dim_val == all_label or dim_val in [None, ""]:
        return df

    return df[s == str(dim_val).strip()].copy()

def filter_by_date(df: pd.DataFrame, date_rng: tuple | list, col: str = "event_date", def_s=None, def_e=None) -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns: return df
    if isinstance(date_rng, (list, tuple)) and len(date_rng) == 2:
        start_dt, end_dt = date_rng
    else:
        start_dt, end_dt = def_s, def_e
    return df[(df[col] >= pd.to_datetime(start_dt)) & (df[col] <= pd.to_datetime(end_dt))].copy()


# ──────────────────────────────────
# 1번 영역 (Daily / Funnel 공통)
# ──────────────────────────────────
def _build_long_df1(df1: pd.DataFrame) -> pd.DataFrame:
    base = [c for c in COLS_SHOWROOM if c in df1.columns]
    out = df1.loc[:, base + ["visit_type"]].copy()
    out["visit_type"] = out["visit_type"].astype("string").fillna("").str.strip()
    out = out[out["visit_type"].isin(["예약", "워크인"])]
    out["event_type"] = out["visit_type"].map({"예약": "visit_reserved", "워크인": "visit_walkin"})
    out["cnt"] = 1
    return out.groupby(base + ["event_type"], dropna=False, as_index=False)["cnt"].sum()

def _build_long_df2(df2: pd.DataFrame) -> pd.DataFrame:
    base = [c for c in COLS_SHOWROOM if c in df2.columns]
    m_cols = [c for c in COLS_BASICSRC if c in df2.columns]
    out = df2.loc[:, base + m_cols].copy()
    if not m_cols:
        return pd.DataFrame(columns=base + ["event_type", "cnt"])
    out[m_cols] = out[m_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    out = out.melt(id_vars=base, value_vars=m_cols, var_name="event_type", value_name="cnt")
    out["cnt"] = pd.to_numeric(out["cnt"], errors="coerce").fillna(0)
    return out

def get_funnel_long_df(df1: pd.DataFrame, df2: pd.DataFrame, sel_mode: str) -> pd.DataFrame:
    df2_tmp = df2.copy()
    if sel_mode == "취소 제외":
        if "rescancel_cnt" in df2_tmp.columns:
            df2_tmp["res_cnt"] = (df2_tmp["res_cnt"] - df2_tmp["rescancel_cnt"]).clip(lower=0)
        if "bookcancel_cnt" in df2_tmp.columns:
            df2_tmp["bookreq_cnt"] = (df2_tmp["bookreq_cnt"] - df2_tmp["bookcancel_cnt"]).clip(lower=0)

    df_total = pd.concat([_build_long_df1(df1), _build_long_df2(df2_tmp)], ignore_index=True)
    df_total["event_date"] = pd.to_datetime(df_total["event_date"], errors="coerce").dt.normalize()
    df_total["cnt"] = pd.to_numeric(df_total["cnt"], errors="coerce").fillna(0)
    return df_total.dropna(subset=["event_date"])

def build_daily(df: pd.DataFrame, start_dt, end_dt, dim_col: str | None = None, dim_val: str | None = None) -> pd.DataFrame:
    daily = filter_by_date(df, (start_dt, end_dt))
    if dim_col and dim_col in daily.columns:
        s = daily[dim_col].astype("string").fillna("").astype(str).str.strip()
        if isinstance(dim_val, (list, tuple, set)):
            vals = [str(x).strip() for x in dim_val if str(x).strip() and str(x).strip() != "전체"]
            if vals: daily = daily[s.isin(vals)]
        elif dim_val not in [None, "", "전체"]:
            daily = daily[s == str(dim_val).strip()]

    daily = (daily.groupby(["event_date", "event_type"], dropna=False)["cnt"]
             .sum().reset_index().pivot(index="event_date", columns="event_type", values="cnt")
             .fillna(0).reset_index().rename(columns={"event_date": "날짜"}).sort_values("날짜").reset_index(drop=True))

    for c in COLS_TOTALSRC:
        if c not in daily.columns: daily[c] = 0

    daily[COLS_TOTALSRC] = daily[COLS_TOTALSRC].apply(pd.to_numeric, errors="coerce").fillna(0)
    daily["visit_total"] = daily["visit_reserved"] + daily["visit_walkin"]
    daily["visit_walkin_rate"] = np.where(daily["visit_total"] > 0, daily["visit_walkin"] / daily["visit_total"] * 100, 0)
    daily["visit_reserved_rate"] = np.where(daily["visit_total"] > 0, daily["visit_reserved"] / daily["visit_total"] * 100, 0)
    daily["noshow_rate"] = np.where(daily["res_cnt"] > 0, 100 - (daily["visit_reserved"] / daily["res_cnt"] * 100), 0)
    daily["bookreq_rate"] = np.where(daily["look_cnt"] > 0, daily["bookreq_cnt"] / daily["look_cnt"] * 100, 0)
    
    return daily[[c for c in COLS_TOTAL if c in daily.columns]]

def render_daily_card(df: pd.DataFrame):
    st.markdown(
        """
        <style>
        .flow-kpi-card{background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:14px 16px;}
        .flow-kpi-title {
            font-size: 14px;
            color: #64748b;
            margin: 0 0 8px;
            display: flex;
            align-items: center;
            gap: 7px;
            line-height: 1;
        }
        .flow-kpi-title::before {
            content: "";
            display: block;
            width: 12px;
            height: 12px;
            border-radius: 2px;
            flex-shrink: 0;
            background: var(--kpi-color, #e2e8f0); 
        }
        .flow-kpi-value{font-size:25px;font-weight:500;line-height:1.05;margin:0;white-space:nowrap}
        .flow-kpi-main{display:flex;align-items:flex-end;justify-content:space-between;gap:10px;}
        .flow-kpi-meta{display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end;}
        .flow-kpi-meta-item{font-size:13px;color:#6c757d;white-space:nowrap;line-height:1.2;}
        </style>
        """, unsafe_allow_html=True,
    )
    
    kpi = {
        "look_cnt": _safe_sum(df, "look_cnt"),
        "bookreq_cnt": _safe_sum(df, "bookreq_cnt"),
        "res_cnt": _safe_sum(df, "res_cnt"),
        "visit_reserved": _safe_sum(df, "visit_reserved"),
        "visit_walkin": _safe_sum(df, "visit_walkin"),
    }
    kpi["visit_total"] = kpi["visit_reserved"] + kpi["visit_walkin"]
    kpi["bookreq_rate"] = _safe_rate(kpi["bookreq_cnt"], kpi["look_cnt"])
    kpi["visit_reserved_rate"] = _safe_rate(kpi["visit_reserved"], kpi["visit_total"])
    kpi["noshow_rate"] = df["noshow_rate"].mean() if not df.empty else 0.0
    kpi["visit_walkin_rate"] = _safe_rate(kpi["visit_walkin"], kpi["visit_total"])

    cards = [
        {"title": "조회수", "value": f'{kpi["look_cnt"]:,.0f}', "meta": [], "color": "#F2EEFB"},
        {"title": "예약신청수", "value": f'{kpi["bookreq_cnt"]:,.0f}', "meta": [f'평균 예약신청률 {kpi["bookreq_rate"]:.1f}%'], "color": "#C4B5FD"},
        {"title": "예약이용수", "value": f'{kpi["res_cnt"]:,.0f}', "meta": [f'평균 노쇼 비중 {kpi["noshow_rate"]:.1f}%'], "color": "#cfdcf0"},
        {"title": "방문수", "value": f'{kpi["visit_total"]:,.0f}', "meta": [], "color": "linear-gradient(to bottom, #3b82f6 50%, #10b981 50%)"},
        {"title": "방문수(예약)", "value": f'{kpi["visit_reserved"]:,.0f}', "meta": [f'평균 예약 비중 {kpi["visit_reserved_rate"]:.1f}%'], "color": "#3b82f6"},
        {"title": "방문수(워크인)", "value": f'{kpi["visit_walkin"]:,.0f}', "meta": [f'평균 워크인 비중 {kpi["visit_walkin_rate"]:.1f}%'], "color": "#10b981"},
    ]

    cols = st.columns(6, vertical_alignment="top")
    for col, card in zip(cols, cards):
        with col:
            meta_html = "".join([f'<span class="flow-kpi-meta-item">{txt}</span>' for txt in card["meta"]])
            kpi_color = card.get('color', '#e2e8f0')
            html = f"""
            <div class="flow-kpi-card">
                <div class="flow-kpi-title" style="--kpi-color: {kpi_color};">
                    {card["title"]}
                </div>
                <div class="flow-kpi-main">
                    <div class="flow-kpi-value">{card["value"]}</div>
                    <div class="flow-kpi-meta">{meta_html}</div>
                </div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)
    st.markdown(" ")

def render_daily_graph(df: pd.DataFrame, tab_key: str, view_type: str):
    fig = go.Figure()

    if view_type == "방문수 추이 (예약 & 워크인)":
        fig.add_trace(go.Bar(x=df["날짜"], y=df["visit_reserved"], name="방문수(예약)", marker_color="#3b82f6"))
        fig.add_trace(go.Bar(x=df["날짜"], y=df["visit_walkin"], name="방문수(워크인)", marker_color="#10b981", text=df["visit_total"], textposition="outside", cliponaxis=False, hovertemplate="방문수(워크인): %{y:,.0f}<extra></extra>"))
        fig.update_layout(barmode="stack", height=330, yaxis_title="방문수", hovermode="x unified")

    elif view_type == "방문수 비중 (예약 vs 워크인)":
        fig.add_trace(go.Bar(x=df["날짜"], y=df["visit_reserved_rate"], name="방문수(예약)", marker_color="#3b82f6", texttemplate="%{y:.0f}%", textposition="inside", hovertemplate="%{y:.0f}%"))
        fig.add_trace(go.Bar(x=df["날짜"], y=df["visit_walkin_rate"], name="방문수(워크인)", marker_color="#10b981", texttemplate="%{y:.0f}%", textposition="inside", hovertemplate="%{y:.0f}%"))
        fig.update_layout(barmode="stack", height=330, yaxis_title="방문수", hovermode="x unified")

    elif view_type == "노쇼 추이 (예약이용 中 예약)":
        def make_noshow_text(row):
            noshow_cnt = row['res_cnt'] - row['visit_reserved']
            return f"노쇼 ({noshow_cnt:.0f}건)" if (row['res_cnt'] > 0 and noshow_cnt > 0) else "👍"
        
        noshow_text = df.apply(make_noshow_text, axis=1)
        fig.add_trace(go.Bar(x=df["날짜"], y=df["res_cnt"], name="예약이용수", marker_color="#cfdcf0", text=noshow_text, textposition="outside", cliponaxis=False, hovertemplate="예약이용수: %{y:,.0f}<extra></extra>"))
        fig.add_trace(go.Bar(x=df["날짜"], y=df["visit_reserved"], name="방문수(예약)", marker_color="#3b82f6"))
        fig.update_layout(barmode="overlay", height=330, yaxis_title="예약이용수 · 방문수", hovermode="x unified")
    
    elif view_type == "조회 및 예약 추이 (조회수 中 예약신청)":
        fig.add_trace(go.Bar(x=df["날짜"], y=df["look_cnt"], name="조회수", marker_color="#F2EEFB", yaxis="y1", hovertemplate="조회수: %{y:,.0f}건<extra></extra>"))
        fig.add_trace(go.Bar(x=df["날짜"], y=df["bookreq_cnt"], name="예약신청수", marker_color="#C4B5FD", yaxis="y1", text=df["bookreq_cnt"].map(lambda x: f"{x:,.0f}" if x > 0 else ""), textposition="outside", cliponaxis=False, hovertemplate="예약신청수: %{y:,.0f}건<extra></extra>"))
        fig.add_trace(go.Scatter(x=df["날짜"], y=df["bookreq_rate"], mode="lines+markers", name="예약신청률", yaxis="y2", line=dict(color="#8B5CF6", width=2.5), hovertemplate="예약신청률: %{y:.1f}%<extra></extra>"))
        fig.update_layout(barmode="overlay", height=330, yaxis=dict(title="조회수 · 예약신청수"), yaxis2=dict(title="예약신청률", overlaying="y", side="right", showgrid=False, ticksuffix="%"), hovermode="x unified")


    fig.update_traces(marker_opacity=0.8)
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), bargap=0.5, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None))
    st.plotly_chart(fig, use_container_width=True, key=f"fig_{view_type[:2]}_{tab_key}")

def render_daily_table(df: pd.DataFrame):
    df = df[['날짜', 'look_cnt', 'bookreq_cnt', 'bookreq_rate', 'res_cnt', 'visit_total', 'visit_reserved', 'visit_walkin', 'visit_reserved_rate', 'visit_walkin_rate', 'noshow_rate']]
    df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.set_index("날짜").T.reset_index().rename(columns={"index": "지표"})
    df["event_type"] = df["event_type"].map(COLS_LABELMAP).fillna(df["event_type"])
    st.dataframe(df, use_container_width=True, row_height=30, hide_index=True)


# ──────────────────────────────────
# 2번 영역 (Funnel New) 좌우 분할 컴포넌트
# ──────────────────────────────────
def render_funnel_dim_table(df_raw: pd.DataFrame, start_dt, end_dt, dim_col: str, dim_val=None, target_event="look_cnt"):
    d = filter_by_date(df_raw, (start_dt, end_dt))
    d = filter_by_dim(d, dim_col, dim_val)
    d["event_type"] = d["event_type"].astype("string").fillna("").astype(str).str.strip()
    d["cnt"] = pd.to_numeric(d["cnt"], errors="coerce").fillna(0)
    d = d[d["event_type"] == target_event]

    pt = (
        d.pivot_table(
            index=dim_col,
            columns="event_date",
            values="cnt",
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    dt_cols = [c for c in pt.columns if c != dim_col]
    rename_map = {c: pd.to_datetime(c).strftime("%Y-%m-%d") for c in dt_cols}
    pt = pt.rename(columns=rename_map)

    dt_cols = [c for c in pt.columns if c != dim_col]
    if dt_cols:
        pt["합계"] = pt[dt_cols].sum(axis=1)
        pt = pt[[dim_col, "합계"] + dt_cols]
        pt = pt.sort_values(["합계", dim_col], ascending=[False, True]).reset_index(drop=True)

    styled = ui.style_cmap(
        pt,
        gradient_rules=[
            {
                "cols": [c for c in pt.columns if c not in [dim_col, "합계"]],
                "cmap": "YlOrBr",
                "cmap_span": (0.0, 0.3)
            }
        ]
    )
    st.dataframe(styled, use_container_width=True, height=315, row_height=30, hide_index=True)


def render_funnel_graph_left(df: pd.DataFrame, tab_key: str):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["날짜"], y=df["look_cnt"], name="조회수", marker_color="#F2EEFB",
        text=df["look_cnt"].map(lambda x: f"{x:,.0f}" if x > 0 else ""),
        textposition="outside", cliponaxis=False, hovertemplate="조회수: %{y:,0f}<extra></extra>", showlegend=True
    ))
    fig.update_layout(
        height=330, yaxis_title=None, hovermode="x unified",
        margin=dict(l=10, r=10, t=10, b=10), bargap=0.4,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None)
    )
    st.plotly_chart(fig, use_container_width=True, key=f"funnel_left_{tab_key}")

def render_funnel_graph_right(df: pd.DataFrame, tab_key: str):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["날짜"], y=df["bookreq_cnt"], name="예약신청수", marker_color="#C4B5FD",
        text=df["bookreq_cnt"].map(lambda x: f"{x:,.0f}" if x > 0 else ""),
        textposition="outside", cliponaxis=False, hovertemplate="예약신청수: %{y:,0f}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        x=df["날짜"], y=df["bookreq_rate"], name="예약신청률", mode="lines+markers+text",
        text=df["bookreq_rate"].map(lambda x: f"{x:.1f}%" if pd.notnull(x) and x > 0 else ""),
        textposition="top center", yaxis="y2", hovertemplate="예약신청률: %{y:.1f}%<extra></extra>",
        line=dict(color="#8B5CF6", width=2)
    ))
    fig.update_layout(
        height=330, yaxis_title=None,
        # yaxis2=dict(title=None, overlaying="y", side="right", ticksuffix="%"),
        yaxis2=dict(title=None, overlaying="y", side="right", ticksuffix="%", showline=False, showgrid=False, zeroline=False, showticklabels=False),
        hovermode="x unified", margin=dict(l=10, r=10, t=10, b=10), bargap=0.4,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None)
    )
    st.plotly_chart(fig, use_container_width=True, key=f"funnel_right_{tab_key}")


# ──────────────────────────────────
# 2번 영역
# ──────────────────────────────────
def build_resv(df: pd.DataFrame, start_dt, end_dt, dim_col: str | None = None, dim_val: str | None = None) -> pd.DataFrame:
    resv = filter_by_date(df, (start_dt, end_dt))
    if dim_col and dim_col in resv.columns:
        s = resv[dim_col].astype("string").fillna("").astype(str).str.strip()
        if isinstance(dim_val, (list, tuple, set)):
            vals = [str(x).strip() for x in dim_val if str(x).strip() and str(x).strip() != "전체"]
            if vals:
                resv = resv[s.isin(vals)]
        elif dim_val not in [None, "", "전체"]:
            resv = resv[s == str(dim_val).strip()]

    if dim_col not in resv.columns:
        return pd.DataFrame(columns=["날짜", dim_col, "res_cnt"])

    resv[dim_col] = resv[dim_col].astype("string").fillna("").astype(str).str.strip()
    resv["cnt"] = pd.to_numeric(resv["cnt"], errors="coerce").fillna(0)

    out = (
        resv.groupby(["event_date", dim_col], dropna=False, as_index=False)["cnt"]
        .sum()
        .rename(columns={"event_date": "날짜", "cnt": "res_cnt"})
        .sort_values(["날짜", dim_col])
        .reset_index(drop=True)
    )
    return out

def filter_df3(df: pd.DataFrame, start_dt, end_dt, dim_col: str | None = None, dim_val: str | None = None):
    d = df.copy()
    d["register_date"] = pd.to_datetime(d["regDateTime"], errors="coerce").dt.normalize()
    d["이용일"] = pd.to_datetime(d["startDate"], errors="coerce").dt.normalize()
    s_dt, e_dt = pd.to_datetime(start_dt).normalize(), pd.to_datetime(end_dt).normalize()
    
    d = d[(d["이용일"] >= s_dt) & (d["이용일"] <= e_dt)]
    
    if dim_col and dim_col in d.columns:
        s = d[dim_col].astype("string").fillna("").astype(str).str.strip()
        if isinstance(dim_val, (list, tuple, set)):
            vals = [str(x).strip() for x in dim_val if str(x).strip() and str(x).strip() != "전체"]
            if vals: d = d[s.isin(vals)]
        elif dim_val and dim_val != "전체":
            d = d[s == str(dim_val).strip()]
    return d

def render_resv_graph(df: pd.DataFrame, dim_col: str, key_tag: str):
    g = df.groupby("날짜", dropna=False, as_index=False)["res_cnt"].sum().sort_values("날짜")
    h = df.groupby(["날짜", dim_col], dropna=False, as_index=False)["res_cnt"].sum()
    h = h[h["res_cnt"] > 0]

    hover_map = (
        h.assign(txt=h[dim_col].astype(str) + ": " + h["res_cnt"].map(lambda x: f"{x:,.0f}"))
        .groupby("날짜")["txt"].apply(lambda s: "<br>".join(s.tolist())).to_dict()
    )
    g["hover_txt"] = g["날짜"].map(hover_map).fillna("")

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=g["날짜"], y=g["res_cnt"], name="예약이용수", showlegend=True, marker_color="#cfdcf0",
        text=g["res_cnt"].map(lambda x: f"{x:,.0f}" if x > 0 else ""), textposition="outside",
        cliponaxis=False, customdata=g["hover_txt"], hovertemplate="%{customdata}<extra></extra>",
    ))
    fig.update_traces(marker_opacity=0.8)
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_layout(
        barmode="stack",
        height=330,
        hovermode="x unified",
        margin=dict(l=10, r=10, t=10, b=10),
        bargap=0.4,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None)
    )
    st.plotly_chart(fig, use_container_width=True, key=f"resv_fig_{key_tag}")

def render_resv_table(df: pd.DataFrame, dim_col: str):
    df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce").dt.strftime("%Y-%m-%d")

    pt = (
        df.pivot_table(
            index=dim_col,
            columns="날짜",
            values="res_cnt",
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    date_cols = sorted([c for c in pt.columns if c != dim_col])
    if date_cols:
        pt["합계"] = pt[date_cols].sum(axis=1)
        pt = pt[[dim_col, "합계"] + date_cols]
        pt = pt.sort_values(["합계", dim_col], ascending=[False, True]).reset_index(drop=True)

    styled = ui.style_cmap(
        pt,
        gradient_rules=[
            {
                "cols": [c for c in pt.columns if c not in [dim_col, "합계"]],
                "cmap": "YlOrBr",
                "cmap_span": (0.0, 0.3)
            }
        ]
    )
    st.dataframe(styled, use_container_width=True, height=315, row_height=30, hide_index=True)
    

def render_lead_table(df3: pd.DataFrame, start_dt, end_dt, dim_col: str | None = None, dim_val: str | None = None):
    df3_v = filter_df3(df3, start_dt, end_dt, dim_col, dim_val)
    all_use_dates = pd.date_range(pd.to_datetime(start_dt), pd.to_datetime(end_dt), freq="D")

    pt = (
        df3_v.pivot_table(
            index="register_date",
            columns="이용일",
            values="bizItemName",
            aggfunc="count",
            fill_value=0
        )
        .reindex(columns=all_use_dates, fill_value=0)
    )

    pt = pt.sort_index(ascending=False)
    pt.columns = [d.strftime("%Y-%m-%d") for d in pt.columns]
    pt = pt.reset_index().rename(columns={"index": "register_date"})
    pt["register_date"] = pd.to_datetime(pt["register_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    date_cols = [c for c in pt.columns if c != "register_date"]
    if date_cols:
        pt["합계"] = pt[date_cols].sum(axis=1)
        pt = pt[["register_date", "합계"] + date_cols]
        pt = pt.sort_values(["register_date"], ascending=[False]).reset_index(drop=True)

    styled = ui.style_cmap(
        pt,
        gradient_rules=[
            {
                "cols": [c for c in pt.columns if c not in ["register_date", "합계"]],
                "cmap": "YlOrBr",
                "cmap_span": (0.0, 0.3)
            }
        ]
    )
    st.dataframe(styled, hide_index=True, height=315, row_height=30, use_container_width=True)
    

def render_lead_graph(df3: pd.DataFrame, start_dt, end_dt, dim_col: str | None = None, dim_val: str | None = None, base_col: str = "register_date"):
    df3_v = filter_df3(df3, start_dt, end_dt, dim_col, dim_val)
    d = df3_v.copy()
    d["register_date"] = pd.to_datetime(d["register_date"], errors="coerce").dt.normalize()
    d["이용일"] = pd.to_datetime(d["이용일"], errors="coerce").dt.normalize()
    d["lead_days"] = (d["이용일"] - d["register_date"]).dt.days
    d = d[d["lead_days"].notna()]
    d = d[d["lead_days"] >= 0]
    d["lead_label"] = np.where(d["lead_days"] >= 7, "D7+", "D" + d["lead_days"].astype(int).astype(str))

    x_col = "이용일" if base_col == "이용일" else "register_date"
    pt = d.groupby([x_col, "lead_label"], as_index=False).size().rename(columns={"size": "cnt"})
    
    all_dates = pd.date_range(pd.to_datetime(start_dt).normalize(), pd.to_datetime(end_dt).normalize(), freq="D")
    lead_labels = [f"D{i}" for i in range(7)] + ["D7+"]
    base = pd.MultiIndex.from_product([all_dates, lead_labels], names=[x_col, "lead_label"]).to_frame(index=False)
    
    pt = base.merge(pt, on=[x_col, "lead_label"], how="left").fillna({"cnt": 0})
    pt["cnt"] = pt["cnt"].astype(int)
    pt[x_col] = pd.to_datetime(pt[x_col], errors="coerce")
    pt["dt_lbl"] = pt[x_col].dt.strftime("%Y-%m-%d")

    fig = px.bar(pt, x="dt_lbl", y="cnt", color="lead_label", category_orders={"lead_label": lead_labels})
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_traces(marker_opacity=0.8, hovertemplate="%{fullData.name}: %{y}건<extra></extra>")
    fig.update_layout(barmode="stack", xaxis_title=None, yaxis_title=None, hovermode="x unified", bargap=0.4, height=330, margin=dict(l=10, r=10, t=10, b=10), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None))
    st.plotly_chart(fig, use_container_width=True)


# ──────────────────────────────────
# 3번 영역
# ──────────────────────────────────
def render_d1_bar(df_base: pd.DataFrame, col: str, key: str, weight_col: str | None = None):
    target_cols = [col] + ([weight_col] if weight_col else [])
    d = _standardize_df(df_base, target_cols)
    d = d[d[col] != ""]
    if d.empty: return
    
    g = d.groupby(col, dropna=False)[weight_col].sum().reset_index(name="cnt") if weight_col else d.groupby(col, dropna=False).size().reset_index(name="cnt")
    g["pct"] = g["cnt"] / g["cnt"].sum() * 100
    if col == "demo_age":
        g[col] = pd.Categorical(g[col], categories=_get_age_order(g[col].astype(str).unique().tolist()), ordered=True)
        g = g.sort_values(col, ascending=False)
    else:
        g = g.sort_values("pct", ascending=True)

    fig = px.bar(g, y=col, x="pct", orientation="h", text="pct", custom_data=[col, "cnt", "pct"])
    fig.update_traces(marker_opacity=0.8, texttemplate="%{x:.1f}%", textposition="outside", hovertemplate="%{customdata[0]}<br>값=%{customdata[1]:,.0f}<br>비중=%{customdata[2]:.1f}%<extra></extra>")
    fig.update_layout(height=200, margin=dict(l=10, r=30, t=10, b=10), showlegend=False, xaxis_title=None, yaxis_title=None, xaxis=dict(ticksuffix="%"))
    st.plotly_chart(fig, use_container_width=True, key=key)

def render_d2_stack(df_base: pd.DataFrame, row_col: str, col_col: str, key: str, weight_col: str | None = None):
    target_cols = [row_col, col_col] + ([weight_col] if weight_col else [])
    d = _standardize_df(df_base, target_cols)
    d = d[(d[row_col] != "") & (d[col_col] != "")]
    if d.empty: return
    
    g = d.groupby([row_col, col_col], dropna=False)[weight_col].sum().reset_index(name="cnt") if weight_col else d.groupby([row_col, col_col], dropna=False).size().reset_index(name="cnt")
    
    row_order = _get_age_order(g[row_col].astype(str).unique().tolist()) if row_col == "demo_age" else g.groupby(row_col)["cnt"].sum().sort_values(ascending=True).index.tolist()
    col_order = _get_age_order(g[col_col].astype(str).unique().tolist()) if col_col == "demo_age" else g.groupby(col_col)["cnt"].sum().sort_values(ascending=False).index.tolist()

    pt = g.pivot_table(index=row_col, columns=col_col, values="cnt", aggfunc="sum", fill_value=0).reindex(row_order).fillna(0)
    for c in col_order:
        if c not in pt.columns: pt[c] = 0
    pt = pt[col_order]

    share = pt.div(pt.sum(axis=1).replace(0, np.nan), axis=0).fillna(0) * 100
    plot_df = share.reset_index().melt(id_vars=row_col, var_name=col_col, value_name="pct").merge(
        pt.reset_index().melt(id_vars=row_col, var_name=col_col, value_name="cnt"), on=[row_col, col_col], how="left"
    )

    plot_df[row_col] = pd.Categorical(plot_df[row_col], categories=row_order, ordered=True)
    plot_df[col_col] = pd.Categorical(plot_df[col_col], categories=col_order, ordered=True)

    fig = px.bar(plot_df, y=row_col, x="pct", color=col_col, orientation="h", barmode="stack", custom_data=[col_col, "cnt", "pct"], category_orders={row_col: row_order, col_col: col_order})
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_traces(marker_opacity=0.8, hovertemplate="%{customdata[0]}<br>값=%{customdata[1]:,.0f}<br>비중=%{customdata[2]:.1f}%<extra></extra>")
    fig.update_layout(height=200, margin=dict(l=10, r=10, t=10, b=10), legend_title=None, xaxis_title=None, yaxis_title=None, xaxis=dict(range=[0, 100], ticksuffix="%"))
    fig.update_yaxes(categoryorder="array", categoryarray=row_order[::-1] if row_col == "demo_age" else row_order)
    st.plotly_chart(fig, use_container_width=True, key=key)

def render_prof_insight(df_base: pd.DataFrame, df_aw_base: pd.DataFrame):
    if df_base is None or df_base.empty: return
    
    age_c = df_base[df_base["demo_age"] != ""]["demo_age"].value_counts()
    gen_c = df_base[df_base["demo_gender"] != ""]["demo_gender"].value_counts()
    purp_c = df_base[df_base["purchase_purpose"] != ""]["purchase_purpose"].value_counts()
    
    top_age = age_c.idxmax() if not age_c.empty else "-"
    top_gender = gen_c.idxmax() if not gen_c.empty else "-"
    top_purp = purp_c.idxmax() if not purp_c.empty else "-"

    top_channel_b = "-"
    top_channel_a = "-"
    channel_display = "none"
    
    if df_aw_base is not None and not df_aw_base.empty:
        aw_grp = df_aw_base[df_aw_base["awareness_type_b"] != ""].groupby(["awareness_type_a", "awareness_type_b"])["weight"].sum().reset_index()
        if not aw_grp.empty:
            top_row = aw_grp.loc[aw_grp["weight"].idxmax()]
            top_channel_a = top_row["awareness_type_a"]
            top_channel_b = top_row["awareness_type_b"]
            channel_display = "block"

    st.markdown(
        f"""
        <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 14px; padding: 16px 18px; margin-bottom: 20px;">
            <div style="font-size:14px; margin-bottom:12px; color:#64748b; line-height:1.8;">🎯 Insight Summary</div>
            <div style="font-size:15px; font-weight:400; color:#1e293b; line-height:1.2;">
                선택한 조건에서 주요 방문객은 <b style="color:#3b82f6;font-weight:700;">{top_age} {top_gender}</b>이며, 
                <b style="color:#3b82f6;font-weight:700;">{top_purp}</b>을(를) 목적으로 매장을 방문합니다.
                <div style="display:{channel_display}; margin-top:6px;">
                    이들은 주로 <b style="color:#3b82f6;font-weight:700;">{top_channel_b}</b>을(를) 통해,
                    브랜드를 <b style="color:#3b82f6;font-weight:700;">{top_channel_a}</b>하여 유입되는 것으로 분석됩니다.
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def render_prof_block(df_base: pd.DataFrame, df_aw_base: pd.DataFrame, key_tag: str):
    render_prof_insight(df_base, df_aw_base)
    def _icon(icon_name, title):
        return f"""
            <div style="display: flex; align-items: flex-start; gap: 6px;">
                <span class="material-symbols-outlined" style="font-size: 17px; color: #475569;padding-top: 6px;">{icon_name}</span>
                <h6 style="margin: 0;">{title}</h6>
            </div>
        """

    c1, c2, c3, c4 = st.columns(4, vertical_alignment="top")
    with c1: st.markdown(_icon("bed", "연령대"), unsafe_allow_html=True); render_d1_bar(df_base, "demo_age", f"age::{key_tag}")
    with c2: st.markdown(_icon("bed", "연령대 × 성별"), unsafe_allow_html=True); render_d2_stack(df_base, "demo_age", "demo_gender", f"age_gen::{key_tag}")
    with c3: st.markdown(_icon("bed", "구매목적"), unsafe_allow_html=True); render_d1_bar(df_base, "purchase_purpose", f"purp::{key_tag}")
    with c4: st.markdown(_icon("bed", "구매목적 × 연령대"), unsafe_allow_html=True); render_d2_stack(df_base, "purchase_purpose", "demo_age", f"purp_age::{key_tag}")

    st.markdown(" ")
    c5, c6, c7, c8 = st.columns(4, vertical_alignment="top")
    with c5: st.markdown(_icon("bed", "인지단계"), unsafe_allow_html=True); render_d1_bar(df_aw_base, "awareness_type_a", f"aw_a::{key_tag}", "weight")
    with c6: st.markdown(_icon("bed", "인지단계 × 인지채널"), unsafe_allow_html=True); render_d2_stack(df_aw_base, "awareness_type_a", "awareness_type_b", f"aw_ab::{key_tag}", "weight")
    with c7: st.markdown(_icon("bed", "구매목적 × 인지채널"), unsafe_allow_html=True); render_d2_stack(df_aw_base, "purchase_purpose", "awareness_type_b", f"purp_aw::{key_tag}", "weight")
    with c8: st.markdown(_icon("bed", "연령대 × 인지채널"), unsafe_allow_html=True); render_d2_stack(df_aw_base, "demo_age", "awareness_type_b", f"age_aw::{key_tag}", "weight")


# ──────────────────────────────────
# 4번 영역
# ──────────────────────────────────
def render_trend_graph(df_plot, x_col, color_col, val_col, key):        
    if df_plot is None or df_plot.empty: st.warning("데이터가 없습니다."); return
    grp = df_plot.groupby([x_col, color_col], dropna=False)[val_col].sum().reset_index()
    grp = grp[grp[color_col].astype(str).str.strip() != ""].sort_values(x_col)
    if grp.empty: st.warning("데이터가 없습니다."); return
    fig = px.bar(grp, x=x_col, y=val_col, color=color_col, barmode="stack", custom_data=[color_col])
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_traces(hovertemplate="%{x}<br>%{customdata[0]}: %{y:,.1f}<extra></extra>")
    fig.update_layout(height=330, margin=dict(l=10, r=10, t=10, b=10), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None), xaxis_title=None, yaxis_title=None)
    st.plotly_chart(fig, use_container_width=True, key=key)


def main():
    # ──────────────────────────────────
    # A) Layout / CSS
    # ──────────────────────────────────
    st.markdown(CFG["CSS_BLOCK_CONTAINER"], unsafe_allow_html=True)
    st.markdown(CFG["CSS_TABS"], unsafe_allow_html=True)

    # ────────────────────────────────────────────────────────────────
    # B) Sidebar
    # ────────────────────────────────────────────────────────────────
    st.sidebar.header("Filter")
    st.sidebar.caption("영역별로 기간을 조정하세요.")
    
    # ──────────────────────────────────
    # C) Data Load
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def load_data():
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        try: creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
        except:
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str): sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1g2HWpm3Le3t3P3Hb9nm2owoiaxywaXv--L0SHEDx3rQ/edit")
        
        df1 = pd.DataFrame(sh.worksheet("shrm_data").get_all_records())
        df2 = pd.DataFrame(sh.worksheet("shrm_nplace").get_all_records())
        df3 = pd.DataFrame(sh.worksheet("shrm_reservation").get_all_records())[["bizItemName", "bookingStatusCode", "startDate", "regDateTime"]]
        _df = pd.DataFrame(sh.worksheet("shrm_list").get_all_records())[["shrm_name", "name_raw2"]]
        
        df1 = parse_shrm(df1); df1 = df1[df1["shrm_name"].astype("string").fillna("").str.strip() != ""]
        df2 = parse_shrm(df2); df2 = df2[df2["shrm_name"].astype("string").fillna("").str.strip() != ""]

        for col in ["look_cnt", "res_cnt", "bookreq_cnt", "rescancel_cnt", "bookcancel_cnt"]:
            if col in df2.columns: df2[col] = pd.to_numeric(df2[col], errors="coerce").fillna(0)

        for d in [df1, df2]:
            for c in COLS_CATEGORICAL:
                if c in d.columns: d[c] = d[c].astype("category")
        
        if "event_date" in df1.columns: df1["event_date"] = pd.to_datetime(df1["event_date"], errors="coerce").dt.normalize()
        if "event_date" in df2.columns: df2["event_date"] = pd.to_datetime(df2["event_date"], errors="coerce").dt.normalize()

        df_aw = pd.DataFrame()
        if not df1.empty and "awareness_type" in df1.columns:
            d_temp = df1.copy()
            _rid = np.arange(len(d_temp))
            s = d_temp["awareness_type"].astype("string").fillna("").astype(str)
            lst = s.apply(lambda x: [t.strip() for t in str(x).split(",") if t.strip()])
            lst = lst.apply(lambda x: x if len(x) > 0 else [])
            d_temp = d_temp.assign(_rid=_rid, awareness_type_list=lst, _n=lst.apply(len).astype(float))
            d_temp = d_temp[d_temp["_n"] > 0].explode("awareness_type_list", ignore_index=True)
            d_temp["awareness_type"] = d_temp["awareness_type_list"].astype(str).str.strip()
            df_aw = d_temp[d_temp["awareness_type"] != ""].copy()
            df_aw["weight"] = (1.0 / df_aw["_n"]).astype(float)
            df_aw["awareness_type_a"] = df_aw["awareness_type"].astype(str).str.extract(r"\((.*?)\)", expand=False).fillna("").str.strip()
            df_aw["awareness_type_b"] = df_aw["awareness_type"].astype(str).str.replace(r"\(.*?\)", "", regex=True).str.strip()

        df3["bizItemName"] = df3["bizItemName"].astype(str).str.replace("예약", "", regex=False).str.replace("쇼룸", "", regex=False).str.replace(" ", "", regex=False)
        df3 = pd.merge(df3, _df, left_on='bizItemName', right_on='name_raw2', how='left')
        df3 = parse_shrm(df3); df3 = df3[df3["shrm_name"].astype("string").fillna("").str.strip() != ""]
        
        return df1, df2, df_aw, df3

    try:
        df1, df2, df_aw, df3 = load_data()
    except KeyError as k:
        # 특정 컬럼을 찾을 수 없는 경우 (데이터 업데이트 중 시트가 비워진 상태)
        st.info("ㅤ현재 구글 시트 데이터가 갱신 중입니다.ㅤ잠시 후 다시 접속해주세요.", icon="🔄")
        st.markdown(k)
        st.stop()
    except Exception as e:
        # 그 외 일시적인 연동 오류가 발생한 경우
        st.info("ㅤ현재 구글 시트 데이터가 갱신 중입니다.ㅤ잠시 후 다시 접속해주세요.", icon="🔄")
        st.stop()
        
    # ──────────────────────────────────
    # D) Header
    # ──────────────────────────────────
    st.subheader("쇼룸 대시보드")
    
    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px; line-height:1.5;">
            쇼룸 시트와 네이버 Place 데이터를 기반으로 <b>조회부터 방문까지의 퍼널 및 고객 상세 프로파일</b>을 확인하는 대시보드입니다.<br> 
            </div>
            <div style="color:#6c757d; font-size:14px; line-height:2.0;">
            ※ 전일 데이터 업데이트 시점은 10시~11시 입니다.
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:       
        st.markdown(
            """
            <div style="display:flex;justify-content:flex-end;align-items:bottom;gap:9px;">
            <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20,400,0,0" rel="stylesheet" />
            
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
    # 날짜 전처리 공통 영역
    # ──────────────────────────────────
    today = datetime.now().date()
    _def_s, _def_e = today - timedelta(days=14), today - timedelta(days=1)
    _def_s2, _def_e2 = today, today + timedelta(days=13)
    _min_d, _max_d = today - timedelta(days=365), today + timedelta(days=365)


    # ──────────────────────────────────
    # 1) 
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>제목 1</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 1", unsafe_allow_html=True)

    with st.expander("공통 Filter", expanded=True):
        f1, f2, f3 = st.columns([2, 2, 2], vertical_alignment="bottom")
        with f1:
            date_default = st.date_input("기간 선택", value=[_def_s, _def_e], min_value=_min_d, max_value=_max_d, key="daily_dd")
        with f2:
            sel_chart = st.selectbox("그래프 선택", ["방문수 추이 (예약 & 워크인)", "방문수 비중 (예약 vs 워크인)", "노쇼 추이 (예약이용 中 예약)", "조회 및 예약 추이 (조회수 中 예약신청)"], key="daily_cv")
        with f3:
            sel_mode = st.radio("예약 집계 선택", ["취소 제외", "취소 포함"], horizontal=True, key="daily_sm")

    if isinstance(date_default, (list, tuple)) and len(date_default) == 2:
        def_s, def_e = date_default[0], date_default[1]
    else:
        def_s, def_e = _def_s, _def_e
        
    df_total = get_funnel_long_df(df1, df2, sel_mode)

    tabs1 = st.tabs(["지점별", "권역별", "유형별"])
    with tabs1[0]:
        branch_opts = [x for x in dim_options(df_total, "shrm_branch") if x != "전체"]
        use_custom = st.checkbox(f"지점 개별 선택 (전체 {len(branch_opts)}개 중)", value=False, key="daily_sb_toggle")
        sel_branch = st.multiselect("", options=branch_opts, default=branch_opts, label_visibility="collapsed", key="daily_sb") if use_custom else branch_opts
        
        daily = build_daily(df_total, def_s, def_e, "shrm_branch", sel_branch)
        render_daily_card(daily)
        render_daily_graph(daily, f"daily_brh_{sel_branch}", sel_chart)
        render_daily_table(daily)
        
    with tabs1[1]:
        opts = [x for x in dim_options(df_total, "shrm_region") if x != "전체"]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    daily = build_daily(df_total, def_s, def_e, "shrm_region", opt)
                    render_daily_card(daily)
                    render_daily_graph(daily, f"daily_reg_{opt}", sel_chart)
                    render_daily_table(daily)
                    
    with tabs1[2]:
        opts = [x for x in dim_options(df_total, "shrm_type") if x != "전체"]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    daily = build_daily(df_total, def_s, def_e, "shrm_type", opt)
                    render_daily_card(daily)
                    render_daily_graph(daily, f"daily_typ_{opt}", sel_chart)
                    render_daily_table(daily)


    # ──────────────────────────────────
    # 2)
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>제목 2</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 2", unsafe_allow_html=True)

    with st.expander("공통 Filter", expanded=True):
        nf1, nf2 = st.columns([2, 4], vertical_alignment="bottom")
        with nf1:
            date_default_new = st.date_input("기간 선택", value=[_def_s, _def_e], min_value=_min_d, max_value=_max_d, key="funnel_dd")
        with nf2:
            sel_mode_new = st.radio("예약 집계 선택", ["취소 제외", "취소 포함"], horizontal=True, key="funnel_sm")

    if isinstance(date_default_new, (list, tuple)) and len(date_default_new) == 2:
        def_s_new, def_e_new = date_default_new[0], date_default_new[1]
    else:
        def_s_new, def_e_new = _def_s, _def_e
        
    df_funnel = get_funnel_long_df(df1, df2, sel_mode_new)
    
    tabs_f = st.tabs(["지점별", "권역별", "유형별"])
    with tabs_f[0]:
        branch_opts = [x for x in dim_options(df_funnel, "shrm_branch") if x != "전체"]
        use_custom = st.checkbox(f"지점 개별 선택 (전체 {len(branch_opts)}개 중)", value=False, key="funnel_sb_toggle")
        sel_branch = st.multiselect("", options=branch_opts, default=branch_opts, label_visibility="collapsed", key="funnel_sb") if use_custom else branch_opts
        
        daily = build_daily(df_funnel, def_s_new, def_e_new, "shrm_branch", sel_branch)
        
        g1, _p, g2 = st.columns([1, 0.03, 1], vertical_alignment="top")
        with g1:
            st.markdown("""<h6 style="margin:0;">🔔 플레이스 조회 추이</h6>""", unsafe_allow_html=True)
            render_funnel_graph_left(daily, f"funnel_brh_left_{sel_branch}")
            render_funnel_dim_table(df_funnel, def_s_new, def_e_new, "shrm_branch", sel_branch, target_event="look_cnt")
        with g2:
            st.markdown("""<h6 style="margin:0;">🔔 예약 신청 추이</h6>""", unsafe_allow_html=True)
            render_funnel_graph_right(daily, f"funnel_brh_right_{sel_branch}")
            render_funnel_dim_table(df_funnel, def_s_new, def_e_new, "shrm_branch", sel_branch, target_event="bookreq_cnt")
        
    with tabs_f[1]:
        opts = [x for x in dim_options(df_funnel, "shrm_region") if x != "전체"]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    daily = build_daily(df_funnel, def_s_new, def_e_new, "shrm_region", opt)
                    
                    g1, _p, g2 = st.columns([1, 0.03, 1], vertical_alignment="top")
                    with g1:
                        st.markdown("""<h6 style="margin:0;">🔔 플레이스 조회 추이</h6>""", unsafe_allow_html=True)
                        render_funnel_graph_left(daily, f"funnel_reg_left_{opt}")
                        render_funnel_dim_table(df_funnel, def_s_new, def_e_new, "shrm_region", opt, target_event="look_cnt")
                    with g2:
                        st.markdown("""<h6 style="margin:0;">🔔 예약 신청 추이</h6>""", unsafe_allow_html=True)
                        render_funnel_graph_right(daily, f"funnel_reg_right_{opt}")
                        render_funnel_dim_table(df_funnel, def_s_new, def_e_new, "shrm_region", opt, target_event="bookreq_cnt")
                    
    with tabs_f[2]:
        opts = [x for x in dim_options(df_funnel, "shrm_type") if x != "전체"]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    daily = build_daily(df_funnel, def_s_new, def_e_new, "shrm_type", opt)
                    
                    g1, _p, g2 = st.columns([1, 0.03, 1], vertical_alignment="top")
                    with g1:
                        st.markdown("""<h6 style="margin:0;">🔔 플레이스 조회 추이</h6>""", unsafe_allow_html=True)
                        render_funnel_graph_left(daily, f"funnel_typ_left_{opt}")
                        render_funnel_dim_table(df_funnel, def_s_new, def_e_new, "shrm_type", opt, target_event="look_cnt")
                    with g2:
                        st.markdown("""<h6 style="margin:0;">🔔 예약 신청 추이</h6>""", unsafe_allow_html=True)
                        render_funnel_graph_right(daily, f"funnel_typ_right_{opt}")
                        render_funnel_dim_table(df_funnel, def_s_new, def_e_new, "shrm_type", opt, target_event="bookreq_cnt")


    # ──────────────────────────────────
    # 3) 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>제목 3</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 3", unsafe_allow_html=True)
    
    with st.expander("공통 Filter", expanded=True):
        f1, f2 = st.columns([2, 4], vertical_alignment="bottom")
        with f1:
            date_default_resv = st.date_input("기간 선택", value=[_def_s2, _def_e2], min_value=_min_d, max_value=_max_d, key="resv_dd")
        with f2:
            sel_mode_resv = st.radio("예약 집계 선택", ["취소 제외", "취소 포함"], horizontal=True, key="resv_sm")

    df2_resv_tmp = df2.copy()
    if sel_mode_resv == "취소 제외" and "rescancel_cnt" in df2_resv_tmp.columns:
        df2_resv_tmp["res_cnt"] = (df2_resv_tmp["res_cnt"] - df2_resv_tmp["rescancel_cnt"]).clip(lower=0)
    
    df3_resv_tmp = df3.copy()
    if sel_mode_resv == "취소 제외":
        df3_resv_tmp = df3_resv_tmp[df3_resv_tmp["bookingStatusCode"].isin(["RC03", "RC08"])]

    df_resv = pd.concat([_build_long_df1(df1), _build_long_df2(df2_resv_tmp)], ignore_index=True)
    df_resv["event_date"] = pd.to_datetime(df_resv["event_date"], errors="coerce").dt.normalize()
    df_resv = df_resv[df_resv["event_type"] == "res_cnt"]

    if isinstance(date_default_resv, (list, tuple)) and len(date_default_resv) == 2:
        def_s_resv, def_e_resv = date_default_resv[0], date_default_resv[1]
    else:
        def_s_resv, def_e_resv = _def_s2, _def_e2
        
    tabs_r = st.tabs(["지점별", "권역별", "유형별"])
    with tabs_r[0]:
        branch_opts = [x for x in dim_options(df_resv, "shrm_branch") if x != "전체"]
        use_custom = st.checkbox(f"지점 개별 선택 (전체 {len(branch_opts)}개 중)", value=False, key="resv_sb_toggle")
        sel_branch = st.multiselect("", options=branch_opts, default=branch_opts, label_visibility="collapsed", key="resv_sb") if use_custom else branch_opts
        
        resv = build_resv(df_resv, def_s_resv, def_e_resv, "shrm_branch", sel_branch)
        g1, _p, g2 = st.columns([1, 0.03, 1], vertical_alignment="top")
        with g1:
            st.markdown("""<h6 style="margin:0;">👀 미래예약</h6>""", unsafe_allow_html=True)
            render_resv_graph(resv, "shrm_branch", f"resv_brh_{sel_branch}")
            render_resv_table(resv, "shrm_branch")
        with g2:
            st.markdown("""<h6 style="margin:0;">👀 리드타임</h6>""", unsafe_allow_html=True)
            render_lead_graph(df3_resv_tmp, def_s_resv, def_e_resv, "shrm_branch", sel_branch, base_col="이용일")
            render_lead_table(df3_resv_tmp, def_s_resv, def_e_resv, "shrm_branch", sel_branch)
            
    with tabs_r[1]:
        opts = [x for x in dim_options(df_resv, "shrm_region") if x != "전체"]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    resv = build_resv(df_resv, def_s_resv, def_e_resv, "shrm_region", opt)
                    g1, _p, g2 = st.columns([1, 0.03, 1], vertical_alignment="top")
                    with g1:
                        st.markdown("""<h6 style="margin:0;">👀 미래예약</h6>""", unsafe_allow_html=True)
                        render_resv_graph(resv, "shrm_region", f"resv_reg_{opt}")
                        render_resv_table(resv, "shrm_region")
                    with g2:
                        st.markdown("""<h6 style="margin:0;">👀 리드타임</h6>""", unsafe_allow_html=True)
                        render_lead_graph(df3_resv_tmp, def_s_resv, def_e_resv, "shrm_region", opt, base_col="이용일")
                        render_lead_table(df3_resv_tmp, def_s_resv, def_e_resv, "shrm_region", opt)
                        
    with tabs_r[2]:
        opts = [x for x in dim_options(df_resv, "shrm_type") if x != "전체"]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    resv = build_resv(df_resv, def_s_resv, def_e_resv, "shrm_type", opt)
                    g1, _p, g2 = st.columns([1, 0.03, 1], vertical_alignment="top")
                    with g1:
                        st.markdown("""<h6 style="margin:0;">👀 미래예약</h6>""", unsafe_allow_html=True)
                        render_resv_graph(resv, "shrm_type", f"resv_typ_{opt}")
                        render_resv_table(resv, "shrm_type")
                    with g2:
                        st.markdown("""<h6 style="margin:0;">👀 리드타임</h6>""", unsafe_allow_html=True)
                        render_lead_graph(df3_resv_tmp, def_s_resv, def_e_resv, "shrm_type", opt, base_col="이용일")
                        render_lead_table(df3_resv_tmp, def_s_resv, def_e_resv, "shrm_type", opt)


    # ──────────────────────────────────
    # 4) 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>제목 4</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 4", unsafe_allow_html=True)
    
    with st.expander("공통 Filter", expanded=True):
        f1, f2, _p = st.columns([2, 2, 2], vertical_alignment="bottom")
        with f1:
            date_default_prof = st.date_input("기간 선택", value=[_def_s, _def_e], min_value=_min_d, max_value=_max_d, key="prof_dd")
        with f2:
            sel_visit = st.selectbox("예약 VS 워크인 선택", dim_options(df1, "visit_type"), key="prof_sv")

    if isinstance(date_default_prof, (list, tuple)) and len(date_default_prof) == 2:
        def_s_prof, def_e_prof = date_default_prof[0], date_default_prof[1]
    else:
        def_s_prof, def_e_prof = _def_s, _def_e
        
    df_prof_tt = filter_by_dim(filter_by_date(df1, (def_s_prof, def_e_prof)), "visit_type", sel_visit)
    df_prof_aw = filter_by_dim(filter_by_date(df_aw, (def_s_prof, def_e_prof)), "visit_type", sel_visit)

    tabs_p = st.tabs(["지점별", "권역별", "유형별"])
    with tabs_p[0]:
        branch_opts = [x for x in dim_options(df_prof_tt, "shrm_branch") if x != "전체"]
        use_custom = st.checkbox(f"지점 개별 선택 (전체 {len(branch_opts)}개 중)", value=False, key="prof_sb_toggle")
        sel_branch = st.multiselect("", options=branch_opts, default=branch_opts, label_visibility="collapsed", key="prof_sb") if use_custom else branch_opts
        
        d_main = filter_by_dim(df_prof_tt, "shrm_branch", sel_branch)
        d_aux = filter_by_dim(df_prof_aw, "shrm_branch", sel_branch)
        render_prof_block(d_main, d_aux, f"prof_brh_{sel_branch}")
        
    with tabs_p[1]:
        opts = [x for x in dim_options(df_prof_tt, "shrm_region") if x != "전체"]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    d_main = filter_by_dim(df_prof_tt, "shrm_region", opt)
                    d_aux = filter_by_dim(df_prof_aw, "shrm_region", opt)
                    render_prof_block(d_main, d_aux, f"prof_reg_{opt}")
                    
    with tabs_p[2]:
        opts = [x for x in dim_options(df_prof_tt, "shrm_type") if x != "전체"]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    d_main = filter_by_dim(df_prof_tt, "shrm_type", opt)
                    d_aux = filter_by_dim(df_prof_aw, "shrm_type", opt)
                    render_prof_block(d_main, d_aux, f"prof_typ_{opt}")

if __name__ == "__main__":
    main()