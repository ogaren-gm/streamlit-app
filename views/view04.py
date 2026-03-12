# SEOHEE
# 2026-03-10 ver. (Refactored)

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
    "HEADER_UPDATE_AM": 850,
    "HEADER_UPDATE_PM": 1535,
    
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

COLS_CATEGORICAL = ["shrm_type", "shrm_region", "shrm_branch", "demo_gender", "demo_age", "awareness_type", "purchase_purpose", "visit_type"]
COLS_SHOWROOM = ["event_date", "shrm_name", "shrm_type", "shrm_region", "shrm_branch"]
COLS_BASICSRC = ["look_cnt", "bookreq_cnt", "res_cnt"]
COLS_TOTALSRC = ["look_cnt", "bookreq_cnt", "res_cnt", "visit_reserved", "visit_walkin"]
COLS_TOTAL    = ["날짜", "look_cnt", "bookreq_cnt", "bookreq_rate",
                "res_cnt","visit_total", "visit_reserved", "visit_reserved_rate", "noshow_rate","visit_walkin", "visit_walkin_rate",]

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
    "noshow_rate": "노쇼 비중 (%)",
}

ALL_LABEL = "전체"


# ──────────────────────────────────
# HELPER
# ──────────────────────────────────
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

def get_dim_options(df: pd.DataFrame, col: str, *, all_label: str = "전체") -> list[str]:
    if df is None or df.empty or col not in df.columns:
        return [all_label]
    s = df[col].astype("string").fillna("").astype(str).str.strip()
    s = s[s != ""]
    return [all_label] + s.value_counts(dropna=False).index.astype(str).tolist()

def filter_dim(df: pd.DataFrame, dim_col: str, dim_val: str, *, all_label: str = "전체") -> pd.DataFrame:
    if df is None or df.empty or dim_val == all_label:
        return df
    if dim_col not in df.columns:
        return df
    s = df[dim_col].astype("string").fillna("").astype(str).str.strip()
    return df[s == str(dim_val).strip()].copy()

def filter_by_date(df: pd.DataFrame, date_rng: tuple | list, col: str = "event_date", def_s=None, def_e=None) -> pd.DataFrame:
    if df is None or df.empty or col not in df.columns:
        return df
    if isinstance(date_rng, (list, tuple)) and len(date_rng) == 2:
        start_dt, end_dt = date_rng
    else:
        start_dt, end_dt = def_s, def_e
    return df[(df[col] >= pd.to_datetime(start_dt)) & (df[col] <= pd.to_datetime(end_dt))].copy()

def _safe_sum(_df: pd.DataFrame, col: str) -> float:
    if _df is None or _df.empty or col not in _df.columns:
        return 0.0
    return float(pd.to_numeric(_df[col], errors="coerce").fillna(0).sum())

def _safe_rate(n: float, d: float) -> float:
    return (n / d * 100.0) if d > 0 else 0.0

# --- PART 1: Funnel & Performance Helpers ---
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

def build_total_table(df: pd.DataFrame, start_dt, end_dt, dim_col: str | None = None, dim_val: str | None = None) -> pd.DataFrame:
    daily = filter_by_date(df, (start_dt, end_dt))
    daily = filter_dim(daily, dim_col, dim_val, all_label=ALL_LABEL)

    daily = (daily.groupby(["event_date", "event_type"], dropna=False)["cnt"]
             .sum().reset_index().pivot(index="event_date", columns="event_type", values="cnt")
             .fillna(0).reset_index().rename(columns={"event_date": "날짜"}).sort_values("날짜").reset_index(drop=True))

    for c in COLS_TOTALSRC:
        if c not in daily.columns:
            daily[c] = 0

    daily[COLS_TOTALSRC] = daily[COLS_TOTALSRC].apply(pd.to_numeric, errors="coerce").fillna(0)
    daily["visit_total"] = daily["visit_reserved"] + daily["visit_walkin"]
    daily["visit_walkin_rate"] = np.where(daily["visit_total"] > 0, daily["visit_walkin"] / daily["visit_total"] * 100, 0)
    daily["visit_reserved_rate"] = np.where(daily["visit_total"] > 0, daily["visit_reserved"] / daily["visit_total"] * 100, 0)
    daily["noshow_rate"] = np.where(daily["res_cnt"] > 0, 100 - (daily["visit_reserved"] / daily["res_cnt"] * 100), 0)
    daily["bookreq_rate"] = np.where(daily["look_cnt"] > 0, daily["bookreq_cnt"] / daily["look_cnt"] * 100, 0)
    
    return daily[[c for c in COLS_TOTAL if c in daily.columns]]

def render_total_card(df: pd.DataFrame):
    # 카드 마크다운
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
            gap: 7px; /* 사각형과 글자 사이 간격 */
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
        {"title": "조회수", "value": f'{kpi["look_cnt"]:,.0f}', "meta": [], "color": "#e4dbd7"},
        {"title": "예약신청수", "value": f'{kpi["bookreq_cnt"]:,.0f}', "meta": [f'평균 예약신청률 {kpi["bookreq_rate"]:.1f}%'], "color": "#f97316"},
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


def render_selected_graph(daily: pd.DataFrame, tab_key: str, view_type: str):
    if daily.empty:
        st.warning("데이터가 없습니다.")
        return

    fig = go.Figure()

    if view_type == "1. 전체 방문 추이":
        fig.add_trace(go.Bar(
            x=daily["날짜"],
            y=daily["visit_reserved"],
            name="방문수(예약)",
            marker_color="#3b82f6"))
        fig.add_trace(go.Bar(
            x=daily["날짜"],
            y=daily["visit_walkin"],
            name="방문수(워크인)",
            marker_color="#10b981",
            text=daily["visit_total"], textposition="outside", cliponaxis=False,
            hovertemplate="방문수(워크인): %{y:,.0f}<extra></extra>"))
        fig.update_layout(barmode="stack", height=330, yaxis_title="방문수", hovermode="x unified")

    elif view_type == "2. 예약 VS 워크인 비중":
        fig.add_trace(go.Bar(
            x=daily["날짜"],
            y=daily["visit_reserved_rate"],
            name="방문수(예약)", 
            marker_color="#3b82f6",
            texttemplate="%{y:.0f}%",
            textposition="inside",
            hovertemplate="%{y:.0f}%"))
        fig.add_trace(go.Bar(
            x=daily["날짜"], 
            y=daily["visit_walkin_rate"], 
            name="방문수(워크인)", 
            marker_color="#10b981",
            texttemplate="%{y:.0f}%",
            textposition="inside",
            hovertemplate="%{y:.0f}%"))
        fig.update_layout(barmode="stack", height=330, yaxis_title="방문수", hovermode="x unified")

    elif view_type == "4. 조회 대비 예약":
        fig.add_trace(go.Bar(
            x=daily["날짜"], 
            y=daily["look_cnt"], 
            name="조회수",
            yaxis="y1", 
            marker_color="#e4dbd7"))
        fig.add_trace(go.Scatter(
            x=daily["날짜"], 
            y=daily["bookreq_rate"], 
            mode="lines+markers", 
            name="예약신청률", 
            yaxis="y2", 
            line=dict(color="#f97316", width=2),
            hovertemplate="%{y:.0f}%"))
        fig.update_layout(height=330, yaxis=dict(title="조회수"), yaxis2=dict(title="예약신청수", overlaying="y", side="right", showgrid=False), hovermode="x unified")

    elif view_type == "3. 방문 VS 노쇼 비중":
        # 노쇼 건수 및 비중 텍스트 계산 ('노쇼(N건, NN%)' 형태)
        def make_noshow_text(row):
            noshow_cnt = row['res_cnt'] - row['visit_reserved']
            if row['res_cnt'] > 0 and noshow_cnt > 0: # 예약이 있고, 노쇼가 1건이라도 발생한 경우에만 텍스트 표시
                return f"노쇼 ({noshow_cnt:.0f}건)"
            else:
                return "👍"
        noshow_text = daily.apply(make_noshow_text, axis=1)
        
        fig.add_trace(go.Bar(
            x=daily["날짜"], 
            y=daily["res_cnt"], 
            name="예약이용수", 
            marker_color="#cfdcf0",
            text=noshow_text, textposition="outside", cliponaxis=False,
            hovertemplate="예약이용수: %{y:,.0f}<extra></extra>"))
        fig.add_trace(go.Bar(
            x=daily["날짜"], 
            y=daily["visit_reserved"], 
            name="방문수(예약)", 
            marker_color="#3b82f6"))
        fig.update_layout(barmode="overlay", height=330, yaxis_title="방문수", hovermode="x unified")
    
    fig.update_traces(marker_opacity=0.8)
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), bargap=0.5, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None))
    st.plotly_chart(fig, use_container_width=True, key=f"fig_{view_type[:2]}_{tab_key}")

def render_total_table(daily: pd.DataFrame):
    tbl = daily.copy()
    tbl = tbl[['날짜', 'look_cnt', 'bookreq_cnt', 'bookreq_rate', 'res_cnt', 'visit_total', 'visit_reserved', 'visit_walkin', 'visit_reserved_rate', 'visit_walkin_rate', 'noshow_rate']]
    tbl["날짜"] = pd.to_datetime(tbl["날짜"], errors="coerce").dt.strftime("%Y-%m-%d")
    tbl = tbl.set_index("날짜").T.reset_index().rename(columns={"index": "지표"})
    tbl["event_type"] = tbl["event_type"].map(COLS_LABELMAP).fillna(tbl["event_type"])
    st.dataframe(tbl, use_container_width=True, row_height=30, hide_index=True)


# --- PART 2: Customer & Target Helpers ---
def _clean_dim(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    d = df[cols].copy()
    for c in cols:
        if c == "weight": d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)
        else: d[c] = d[c].astype("string").fillna("").astype(str).str.strip()
    return d

def _get_age_order(vals: list[str]) -> list[str]:
    base = ["20대", "30대", "40대", "50대", "60대 이상"]
    base_in = [x for x in base if x in vals]
    return base_in + [x for x in sorted(vals) if x not in base_in]


def _render_1d_bar(df_base: pd.DataFrame, col: str, key: str, weight_col: str | None = None):
    d = _clean_dim(df_base, [col] + ([weight_col] if weight_col else []))
    d = d[d[col] != ""]
    if d.empty:
        st.warning("데이터 없음"); return
    
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


def _render_2d_stack(df_base: pd.DataFrame, row_col: str, col_col: str, key: str, weight_col: str | None = None):
    d = _clean_dim(df_base, [row_col, col_col] + ([weight_col] if weight_col else []))
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


def _render_dynamic_insight(df_base: pd.DataFrame, df_aw_base: pd.DataFrame):
    if df_base is None or df_base.empty: return
    
    # 데이터 추출 및 분석 로직
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
        # awareness_type_b를 기준으로 그룹화하여 가중치 합산
        aw_grp = df_aw_base[df_aw_base["awareness_type_b"] != ""].groupby(["awareness_type_a", "awareness_type_b"])["weight"].sum().reset_index()
        
        if not aw_grp.empty:
            # 가중치 합이 가장 높은 행 추출
            top_row = aw_grp.loc[aw_grp["weight"].idxmax()]
            top_channel_a = top_row["awareness_type_a"]
            top_channel_b = top_row["awareness_type_b"]
            channel_display = "block"

    # 카드 마크다운
    st.markdown(
        f"""
        <div style="
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 14px;
            padding: 16px 18px;
            margin-bottom: 20px;
        ">
            <div style="font-size:14px; margin-bottom:12px; color:#64748b; line-height:1.8;">
                🎯 Insight Summary
            </div>
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

def render_profile_block(df_base: pd.DataFrame, df_aw_base: pd.DataFrame, key_tag: str):
    _render_dynamic_insight(df_base, df_aw_base)
    
    # 아이콘과 제목을 결합한 헬퍼 함수 (가독성을 위해)
    def section_title(icon_name, title):
        return f"""
            <div style="display: flex; align-items: flex-start; gap: 6px;">
                <span class="material-symbols-outlined" style="font-size: 17px; color: #475569;padding-top: 6px;">{icon_name}</span>
                <h6 style="margin: 0;">{title}</h6>
            </div>
        """

    c1, c2, c3, c4 = st.columns(4, vertical_alignment="top")
    with c1: 
        st.markdown(section_title("bed", "연령대"), unsafe_allow_html=True)
        _render_1d_bar(df_base, "demo_age", f"age::{key_tag}")
    with c2: 
        st.markdown(section_title("bed", "연령대 × 성별"), unsafe_allow_html=True)
        _render_2d_stack(df_base, "demo_age", "demo_gender", f"age_gen::{key_tag}")
    with c3: 
        st.markdown(section_title("bed", "구매목적"), unsafe_allow_html=True)
        _render_1d_bar(df_base, "purchase_purpose", f"purp::{key_tag}")
    with c4: 
        st.markdown(section_title("bed", "구매목적 × 연령대"), unsafe_allow_html=True)
        _render_2d_stack(df_base, "purchase_purpose", "demo_age", f"purp_age::{key_tag}")

    st.markdown(" ")
    c5, c6, c7, c8 = st.columns(4, vertical_alignment="top")
    with c5: 
        st.markdown(section_title("bed", "인지단계"), unsafe_allow_html=True)
        _render_1d_bar(df_aw_base, "awareness_type_a", f"aw_a::{key_tag}", "weight")
    with c6: 
        st.markdown(section_title("bed", "인지단계 × 인지채널"), unsafe_allow_html=True)
        _render_2d_stack(df_aw_base, "awareness_type_a", "awareness_type_b", f"aw_ab::{key_tag}", "weight")
    with c7: 
        st.markdown(section_title("bed", "구매목적 × 인지채널"), unsafe_allow_html=True)
        _render_2d_stack(df_aw_base, "purchase_purpose", "awareness_type_b", f"purp_aw::{key_tag}", "weight")
    with c8: 
        st.markdown(section_title("bed", "연령대 × 인지채널"), unsafe_allow_html=True)
        _render_2d_stack(df_aw_base, "demo_age", "awareness_type_b", f"age_aw::{key_tag}", "weight")


def _render_trend_chart(df_plot, x_col, color_col, val_col, key):        
    if df_plot is None or df_plot.empty: st.warning("데이터가 없습니다."); return
    grp = df_plot.groupby([x_col, color_col], dropna=False)[val_col].sum().reset_index()
    grp = grp[grp[color_col].astype(str).str.strip() != ""].sort_values(x_col)
    if grp.empty: st.warning("데이터가 없습니다."); return
    fig = px.bar(grp, x=x_col, y=val_col, color=color_col, barmode="stack", custom_data=[color_col])
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_traces(hovertemplate="%{x}<br>%{customdata[0]}: %{y:,.1f}<extra></extra>")
    fig.update_layout(height=330, margin=dict(l=10, r=10, t=10, b=10), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None), xaxis_title=None, yaxis_title=None)
    st.plotly_chart(fig, use_container_width=True, key=key)


# ──────────────────────────────────
# main
# ──────────────────────────────────
def main():
    # ──────────────────────────────────
    # A) Layout / CSS
    # ──────────────────────────────────
    st.markdown(CFG["CSS_BLOCK_CONTAINER"], unsafe_allow_html=True)
    st.markdown(CFG["CSS_TABS"], unsafe_allow_html=True)

    # ────────────────────────────────────────────────────────────────
    # B) Sidebar
    # ────────────────────────────────────────────────────────────────
    # 기간
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
        
        df1 = parse_shrm(df1); df1 = df1[df1["shrm_name"].astype("string").fillna("").str.strip() != ""]
        df2 = parse_shrm(df2); df2 = df2[df2["shrm_name"].astype("string").fillna("").str.strip() != ""]

        for d in [df1, df2]:
            for c in COLS_CATEGORICAL:
                if c in d.columns: d[c] = d[c].astype("category")

        if "event_date" in df1.columns: df1["event_date"] = pd.to_datetime(df1["event_date"], errors="coerce").dt.normalize()
        if "event_date" in df2.columns: df2["event_date"] = pd.to_datetime(df2["event_date"], errors="coerce").dt.normalize()

        # 인지채널 가중치 데이터 (df_aw) 사전 전처리 일원화
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

        return df1, df2, df_aw

    df1, df2, df_aw = load_data()

    # ──────────────────────────────────
    # C-1) tb_max -> get max date
    # ──────────────────────────────────
    # (pass)


    # ──────────────────────────────────
    # C-2) Progress Bar
    # ──────────────────────────────────
    # (pass)


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
            f"""
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
    # 날짜 전처리
    # ──────────────────────────────────
    today = datetime.now().date()
    _def_s, _def_e = today - timedelta(days=14), today - timedelta(days=1)
    _min_d, _max_d = today - timedelta(days=365), today + timedelta(days=365)


    # ──────────────────────────────────
    # 1) 
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>제목 1</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 1", unsafe_allow_html=True)

    # 롱포맷 통합 데이터 생성
    df_flow = pd.concat([_build_long_df1(df1), _build_long_df2(df2)], ignore_index=True)
    df_flow["event_date"] = pd.to_datetime(df_flow["event_date"], errors="coerce").dt.normalize()
    df_flow["cnt"] = pd.to_numeric(df_flow["cnt"], errors="coerce").fillna(0)
    df_flow = df_flow.dropna(subset=["event_date"])

    # 공통 필터 및 차트 뷰 선택 (통합)
    with st.expander("공통 Filter", expanded=True):
        f1, f2, _p = st.columns([1.5, 1, 1], vertical_alignment="bottom")
        with f1:
            flow_date = st.date_input("기간 선택", value=[_def_s, _def_e], min_value=_min_d, max_value=_max_d, key="flow_date_rng")
        with f2:
            chart_view = st.selectbox(
                "그래프 선택", 
                [
                    "1. 전체 방문 추이", 
                    "2. 예약 VS 워크인 비중", 
                    "3. 방문 VS 노쇼 비중",
                    "4. 조회 대비 예약"
                ],
                key="flow_chart_view"
            )
    
    if isinstance(flow_date, (list, tuple)) and len(flow_date) == 2: flow_s, flow_e = flow_date
    else: flow_s, flow_e = _def_s, _def_e

    top_tabs = st.tabs(["지점별", "권역별", "유형별"])
    with top_tabs[0]:
        sel_branch = st.selectbox("", get_dim_options(df_flow, "shrm_branch"), label_visibility="collapsed", key="flow_branch")
        daily = build_total_table(df_flow, flow_s, flow_e, "shrm_branch", sel_branch)
        render_total_card(daily)
        render_selected_graph(daily, f"br_{sel_branch}", chart_view)
        render_total_table(daily)
    with top_tabs[1]:
        opts = [x for x in get_dim_options(df_flow, "shrm_region") if x != ALL_LABEL]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    daily = build_total_table(df_flow, flow_s, flow_e, "shrm_region", opt)
                    render_total_card(daily)
                    render_selected_graph(daily, f"reg_{opt}", chart_view)
                    render_total_table(daily)
    with top_tabs[2]:
        opts = [x for x in get_dim_options(df_flow, "shrm_type") if x != ALL_LABEL]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    daily = build_total_table(df_flow, flow_s, flow_e, "shrm_type", opt)
                    render_total_card(daily)
                    render_selected_graph(daily, f"typ_{opt}", chart_view)
                    render_total_table(daily)


    # ──────────────────────────────────
    # 2) 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>제목 2</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 2", unsafe_allow_html=True)
    st.warning("예약 현황 및 방문 트래픽 예측")
    st.warning("과거 패턴 기반 방문 예측 모델 구축 중")


    # ──────────────────────────────────
    # 3) 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>제목 3</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 3", unsafe_allow_html=True)

    with st.expander("공통 Filter", expanded=True):
        f1, f2, _p = st.columns([1.5, 1, 1], vertical_alignment="bottom")
        with f1: prof_date = st.date_input("기간 선택", value=[_def_s, _def_e], min_value=_min_d, max_value=_max_d, key="prof_date")
        with f2: sel_vt = st.selectbox("예약 VS 워크인 선택", get_dim_options(df1, "visit_type"), key="prof_vt")

    df1_p = filter_dim(filter_by_date(df1, prof_date), "visit_type", sel_vt)
    df_aw_p = filter_dim(filter_by_date(df_aw, prof_date), "visit_type", sel_vt)

    top_tabs_p = st.tabs(["지점별", "권역별", "유형별"])
    with top_tabs_p[0]:
        sel_br = st.selectbox("", get_dim_options(df1_p, "shrm_branch"), label_visibility="collapsed", key="prof_br")
        render_profile_block(filter_dim(df1_p, "shrm_branch", sel_br), filter_dim(df_aw_p, "shrm_branch", sel_br), f"br_{sel_br}")
    with top_tabs_p[1]:
        opts = [x for x in get_dim_options(df1_p, "shrm_region") if x != ALL_LABEL]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    render_profile_block(filter_dim(df1_p, "shrm_region", opt), filter_dim(df_aw_p, "shrm_region", opt), f"reg_{opt}")
    with top_tabs_p[2]:
        opts = [x for x in get_dim_options(df1_p, "shrm_type") if x != ALL_LABEL]
        if opts:
            sub_tabs = st.tabs(opts)
            for i, opt in enumerate(opts):
                with sub_tabs[i]:
                    render_profile_block(filter_dim(df1_p, "shrm_type", opt), filter_dim(df_aw_p, "shrm_type", opt), f"typ_{opt}")

    # ──────────────────────────────────
    # 4)
    # ──────────────────────────────────
    st.header(" ")
    st.divider(); st.markdown("여기서부턴 의견청취 + 추가기획용")
    st.markdown("<h5 style='margin:0'>제목 4</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 4", unsafe_allow_html=True)

    with st.expander("공통 Filter", expanded=True):
        t_f1, t_f2, _p = st.columns([1.5, 1, 1], vertical_alignment="bottom")
        with t_f1: trend_date = st.date_input("기간 선택", value=[_def_s, _def_e], min_value=_min_d, max_value=_max_d, key="t_date")
        with t_f2: sel_t_vt = st.selectbox("예약 VS 워크인 선택", get_dim_options(df1, "visit_type"), key="t_vt")

        t_f4, t_f3, t_f5, t_f7, t_f6, t_f8 = st.columns(6, vertical_alignment="bottom")
        with t_f4: sel_t_age = st.selectbox("연령대 선택", get_dim_options(df1, "demo_age"), key="t_age")
        with t_f3: sel_t_gender = st.selectbox("성별 선택", get_dim_options(df1, "demo_gender"), key="t_gender")
        with t_f5: sel_t_purp = st.selectbox("구매목적 선택", get_dim_options(df1, "purchase_purpose"), key="t_purp")
        with t_f7: sel_t_branch = st.selectbox("쇼룸 지점 선택", get_dim_options(df1, "shrm_branch"), key="t_branch")
        with t_f6: sel_t_region = st.selectbox("쇼룸 권역 선택", get_dim_options(df1, "shrm_region"), key="t_region")
        with t_f8: sel_t_type = st.selectbox("쇼룸 유형 선택", get_dim_options(df1, "shrm_type"), key="t_type")


    df1_t, df_aw_t = filter_by_date(df1, trend_date), filter_by_date(df_aw, trend_date)
    
    for col, val in {"visit_type": sel_t_vt, "demo_gender": sel_t_gender, "demo_age": sel_t_age, "purchase_purpose": sel_t_purp, "shrm_region": sel_t_region, "shrm_branch": sel_t_branch, "shrm_type": sel_t_type}.items():
        df1_t = filter_dim(df1_t, col, val)
        df_aw_t = filter_dim(df_aw_t, col, val)

    if not df1_t.empty: df1_t["cnt"] = 1

    trend_tabs = st.tabs(["인지단계", "인지채널", "연령대", "성별", "구매목적", "지점별", "권역별", "유형별"])
    
    with trend_tabs[0]: _render_trend_chart(df_aw_t, "event_date", "awareness_type_a", "weight", "tr_aw_stg") #인지단계
    with trend_tabs[1]: _render_trend_chart(df_aw_t, "event_date", "awareness_type_b", "weight", "tr_aw_chn") #인지채널
    with trend_tabs[2]: _render_trend_chart(df1_t, "event_date", "demo_age", "cnt", "tr_age") # 연령대
    with trend_tabs[3]: _render_trend_chart(df1_t, "event_date", "demo_gender", "cnt", "tr_gen") # 성별
    with trend_tabs[4]: _render_trend_chart(df1_t, "event_date", "purchase_purpose", "cnt", "tr_purp") #구매목적
    with trend_tabs[5]: _render_trend_chart(df1_t, "event_date", "shrm_branch", "cnt", "tr_brn") #지점
    with trend_tabs[6]: _render_trend_chart(df1_t, "event_date", "shrm_region", "cnt", "tr_reg") #권역
    with trend_tabs[7]: _render_trend_chart(df1_t, "event_date", "shrm_type", "cnt", "tr_typ") #유형


if __name__ == "__main__":
    main()