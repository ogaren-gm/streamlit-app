# 하드코딩

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import importlib, json
from datetime import datetime, timedelta, date
import gspread
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo
import math
import json
import html

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

    "TOPK_OPTS": [10, 15, 20, 25, 30, 35, 40],

    # 하드코딩
    "BRAND_LIST": ["슬립퍼", "누어", "토들즈"],
    "BRAND_SHEET": ["QUERY_SLP", "QUERY_NOR", "QUERY_TDS"],
    "BRAND_COLOR": {"슬립퍼": "#EF4444", "누어": "#2563EB", "토들즈": "#10B981"},
    "divided_days" : 10,

    "CSS_BLOCK_CONTAINER": """
        <style>
            .block-container {
                max-width: 100% !important;
                padding-top: 0rem;
                padding-bottom: 8rem;
                padding-left: 5rem;
                padding-right: 4.5rem;
            }
            /* 컬럼 찢어짐 완벽 방어 */
            [data-testid="column"] {
                min-width: 0 !important;
            }
        </style>
    """,
    "CSS_TABS": """
        <style>
            [role="tablist"] [role="tab"] { margin-right: 1rem; }
            /* 탭 개수가 많아 화면을 오른쪽으로 미는 현상 방지 (자동 줄바꿈) */
            [data-testid="stTabs"] [role="tablist"] {
                flex-wrap: wrap !important;
            }
        </style>
    """,
}


# ──────────────────────────────────
# HELPER
# ──────────────────────────────────
# 공통 함수
def _fmt_money(v):
    if pd.isna(v):
        return "-"
    try:
        return f"{int(float(v)):,}"
    except:
        return str(v)


def _safe_str(v):
    return "" if pd.isna(v) else str(v)


def build_channels_by_brand(ppl_list: pd.DataFrame) -> dict:
    channel_brand = (
        ppl_list.loc[:, ["채널명", "브랜드", "order"]]
        .dropna(subset=["채널명", "브랜드"])
        .assign(order=lambda d: pd.to_numeric(d["order"], errors="coerce"))
        .assign(order=lambda d: d["order"].fillna(float("-inf")))
        .sort_values(["브랜드", "order"], ascending=[True, False])
        .drop_duplicates(subset=["브랜드", "채널명"], keep="first")
    )
    return {
        b: g["채널명"].tolist()
        for b, g in channel_brand.groupby("브랜드", sort=False)
    }


# A. 채널별 성과 확인 (ENG)
def render_eng_card(row: pd.Series, brand_color_map: dict) -> None: #✅카드
    c_title = html.escape(_safe_str(row.get("채널명")), quote=True)
    c_brand = html.escape(_safe_str(row.get("브랜드")).strip(), quote=True)
    c_money = html.escape(_fmt_money(row.get("금액")), quote=True)
    c_date = html.escape(_safe_str(row.get("업로드 날짜")), quote=True)
    _url = _safe_str(row.get("컨텐츠 URL")).strip()
    c_link = (
        f"<a href='{html.escape(_url, quote=True)}' target='_blank' style='text-decoration:underline; color:#2563EB;'>바로가기</a>"
        if _url else
        "<span style='color:#71717a;'>링크없음</span>"
    )

    b_color = brand_color_map[c_brand]

    import base64
    try:
        path = f"static/{row.get('채널명')}.png"
        with open(path, "rb") as f:
            c_img = f"data:image/png;base64,{base64.b64encode(f.read()).decode()}"
    except:
        c_img = "https://cdn-icons-png.flaticon.com/512/149/149071.png"

    st.markdown(f"""
        <div style="
            border: 1px solid #e5e5e5;
            border-radius: 12px;
            padding: 40px;
            background: #ffffff;
            height: 220px;
            display: flex;
            align-items: center;
            gap: 43px;
        ">
            <div style="
                width: 90px;
                height: 90px;
                border-radius: 50%;
                overflow: hidden;
                border: 2px solid #f3f4f6;
                flex-shrink: 0;
            ">
                <img src="{c_img}" style="width: 100%;
                height: 100%;
                object-fit: cover;
                ">
            </div>
            <div style="flex-grow: 1;">
                <div style="font-size: 20px; font-weight: 600; margin-bottom: 10px; color: #111827;">{c_title}</div>
                <div style="display: inline-block; background: {b_color}; color: #ffffff; font-size: 13px; font-weight: 600; padding: 3px 10px; border-radius: 20px; margin-bottom: 15px;">{c_brand}</div>
                <div style="font-size: 13px; font-weight: 400; line-height: 1.7;"> 💰 집행 금액ㅤㅤ₩ {c_money}</div>
                <div style="font-size: 13px; font-weight: 400; line-height: 1.7;"> 📢 게재 일자ㅤㅤ{c_date}</div>
                <div style="font-size: 13px; font-weight: 400; line-height: 1.7;"> 🔗 영상 링크ㅤㅤ{c_link}</div>
            </div>
        </div>
    """, unsafe_allow_html=True)


def _preprocess_engdf(df: pd.DataFrame, select_option: int = 1) -> pd.DataFrame:
    base_cols_info = [
        ("날짜", ("기본정보", "날짜")),
        ("채널명", ("기본정보", "채널명")),
        ("Cost", ("COST", "일할비용")),
        ("조회수", ("ENGAGEMENT", "조회수")),
        ("좋아요수", ("ENGAGEMENT", "좋아요수")),
        ("댓글수", ("ENGAGEMENT", "댓글수")),
        ("브랜드언급량", ("ENGAGEMENT", "브랜드언급량")),
        ("링크클릭수", ("ENGAGEMENT", "링크클릭수")),
        ("session_count", ("GA", "유입세션수")),
        ("avg_session_duration_sec", ("GA", "평균체류(초)"))
    ]

    metrics_info = [
        ("view_item_list_sessions", "view_item_list", "PLP조회"),
        ("view_item_sessions", "view_item", "PDP조회"),
        ("scroll_50_sessions", "scroll_50", "PDPscr50"),
        ("product_option_price_sessions", "product_option_price", "가격표시"),
        ("find_showroom_sessions", "find_nearby_showroom", "쇼룸찾기"),
        ("add_to_cart_sessions", "add_to_cart", "장바구니"),
        ("sign_up_sessions", "sign_up", "회원가입"),
        ("showroom_10s_sessions", "showroom_10s", "쇼룸10초"),
        ("showroom_leads_sessions", "showroom_leads", "쇼룸예약")
    ]

    required_cols = [c[0] for c in base_cols_info] + [m[0] for m in metrics_info]
    missing_cols = {col: 0 for col in required_cols if col not in df.columns}
    if missing_cols:
        df = df.assign(**missing_cols)

    num_cols = [col for col in required_cols if col not in ["날짜", "채널명"]]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    selected_flat_cols = [c[0] for c in base_cols_info]
    selected_multi_cols = [c[1] for c in base_cols_info]

    for raw_col, prefix, multi_name in metrics_info:
        selected_flat_cols.append(raw_col)
        selected_multi_cols.append((multi_name, "Actual"))

        cvr_col = f"{prefix}_CVR"
        df[cvr_col] = (df[raw_col] / df["session_count"] * 100).round(2)
        if select_option in (2, 4):
            selected_flat_cols.append(cvr_col)
            selected_multi_cols.append((multi_name, "CVR"))

        cpa_col = f"{prefix}_CPA"
        df[cpa_col] = ((df["Cost"] / CFG["divided_days"]) / df[raw_col] * 100).round(0)
        if select_option in (3, 4):
            selected_flat_cols.append(cpa_col)
            selected_multi_cols.append((multi_name, "CPA"))

    df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce").dt.strftime("%Y-%m-%d")
    numeric_cols = df.select_dtypes(include=["number"]).columns
    df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan).fillna(0)

    df = df[selected_flat_cols]
    df.columns = pd.MultiIndex.from_tuples(selected_multi_cols, names=["그룹", "지표"])

    return df


def render_eng_df(target_df: pd.DataFrame, select_option: int) -> None: #✅표
    df = _preprocess_engdf(target_df, select_option)

    decimals_map = {}
    suffix_map = {}

    for col in df.columns:
        group, metric = col
        if metric == "CVR":
            decimals_map[col] = 1
            suffix_map[col] = " %"
        elif metric in ["Actual", "CPA"] or group in ["COST", "ENGAGEMENT", "GA"]:
            decimals_map[col] = 0

    styled = ui.style_format(df, decimals_map=decimals_map, suffix_map=suffix_map)
    st.dataframe(styled, use_container_width=True, row_height=30, height=315, hide_index=True)


def render_eng_graph(df: pd.DataFrame, channel_name: str, target_metric: str) -> None: #✅그래프
    df1 = df[df["채널명"] == channel_name].copy()
    df1["날짜"] = pd.to_datetime(df1["날짜"])
    df1 = df1.sort_values("날짜")

    if not df1.empty and df1[target_metric].sum() > 0:
        fig = px.line(
            df1,
            x="날짜",
            y=target_metric,
            markers=True
        )

        m_colors = {
            "조회수": "#2563EB",
            "좋아요수": "#EF4444",
            "댓글수": "#10B981",
            "브랜드언급량": "#F59E0B",
            "링크클릭수": "#8B5CF6"
        }
        line_color = m_colors.get(target_metric, "#2563EB")

        fig.update_traces(
            line_color=line_color,
            fill="tozeroy",
            opacity=0.1,
            name=target_metric,
            showlegend=True
        )

        fig.update_layout(
            height=220,
            margin=dict(l=15, r=10, t=50, b=10),
            xaxis_title=None,
            yaxis_title=None,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                title_text=None,
            )
        )
        fig.update_xaxes(tickformat="%m월 %d일")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.markdown(
            f"""
            <div style="
                height:220px;
                width:100%;
                background:#F3F4F6;
                border-radius:12px;
                display:flex;
                align-items:center;
                justify-content:center;
                text-align:center;
                color:#6B7280;
                font-size:15px;
                font-weight:300;
                line-height:1.6;
            ">
                {html.escape(str(target_metric))} 데이터가 없어, <br>
                그래프가 미표시됩니다.
            </div>
            """,
            unsafe_allow_html=True,
        )


# B. 채널별 검색량 기여도 (CTB)
def build_ctb_rawdf( # 브랜드별 / 검색량 기여도 DF 원본 생성
    brand_name: str,
    query_sum_df: pd.DataFrame,
    ppl_df: pd.DataFrame,
    channels_by_brand: dict
) -> pd.DataFrame:
    channels = channels_by_brand.get(brand_name, [])

    df = ppl_df.merge(query_sum_df[["날짜", "검색량"]], on="날짜", how="outer")

    for ch in channels:
        if ch not in df.columns:
            df[ch] = 0

    cols_to_int = channels + ["검색량"]
    if cols_to_int:
        df[cols_to_int] = df[cols_to_int].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
    else:
        df["검색량"] = pd.to_numeric(df.get("검색량", 0), errors="coerce").fillna(0).astype(int)

    df["기본 검색량"] = (df["검색량"] - df[channels].sum(axis=1)).clip(lower=0)

    for col in (channels + ["기본 검색량"]):
        df[f"{col}_비중"] = np.where(
            df["검색량"] > 0,
            (df[col] / df["검색량"] * 100),
            0.0
        ).round(2)

    ordered_cols = (
        ["날짜", "검색량", "기본 검색량", "기본 검색량_비중"] +
        [c for pair in [(ch, f"{ch}_비중") for ch in channels] for c in pair]
    )
    df = df[ordered_cols].sort_values("날짜", ascending=True)

    return df


def render_ctb_df(target_df: pd.DataFrame, channels_dict: dict, brand: str) -> None:  # ✅표
    channels = channels_dict.get(brand, [])

    base_required = ["날짜", "검색량", "기본 검색량"]
    dyn_required = channels

    missing_cols = {col: 0 for col in (base_required + dyn_required) if col not in target_df.columns}
    if missing_cols:
        target_df = target_df.assign(**missing_cols)

    ordered_cols = base_required + dyn_required
    df = target_df[ordered_cols].copy()

    df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce").dt.strftime("%Y-%m-%d")

    num_cols = df.columns.difference(["날짜"])
    df[num_cols] = (
        df[num_cols]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    # long → pivot
    long = df.melt(
        id_vars="날짜",
        var_name="구분",
        value_name="값"
    )

    pv = (
        long
        .pivot_table(
            index="구분",
            columns="날짜",
            values="값",
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )
    pv["__sum__"] = pv.iloc[:, 1:].sum(axis=1)
    pv = pv.sort_values("__sum__", ascending=False).drop(columns="__sum__")

    val_cols = [c for c in pv.columns if c != "구분"]
    styled = pv.style.format("{:,.0f}", subset=val_cols)
    st.dataframe(styled, use_container_width=True, row_height=30, height=315, hide_index=True)


def render_ctb_graph( #✅그래프
    df: pd.DataFrame,
    x: str,
    y,
    color: str,
    fixed_label: str = "기본 검색량",
    fixed_color: str = "#D5DAE5",
) -> None:
    palette = px.colors.qualitative.Plotly
    color_map = {fixed_label: fixed_color}

    if isinstance(y, (list, tuple)):
        y_cols = list(y)
        df[y_cols] = df[y_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

        if color is not None and color in df.columns:
            long_df = df.melt(
                id_vars=[x, color],
                value_vars=y_cols,
                var_name="__series__",
                value_name="__value__"
            )
            cats = long_df["__series__"].dropna().astype(str).unique().tolist()
            if fixed_label in cats:
                cats = [c for c in cats if c != fixed_label] + [fixed_label]

            fig = px.bar(
                long_df,
                x=x,
                y="__value__",
                color="__series__",
                opacity=0.6,
                color_discrete_map=color_map,
                color_discrete_sequence=palette,
                category_orders={"__series__": cats},
            )
        else:
            fig = px.bar(
                df,
                x=x,
                y=y_cols,
                opacity=0.6,
                color_discrete_sequence=palette
            )

    else:
        df[y] = pd.to_numeric(df[y], errors="coerce").fillna(0)

        order = None
        if color and color in df.columns:
            cats = df[color].dropna().astype(str).unique().tolist()
            if fixed_label in cats:
                cats = [c for c in cats if c != fixed_label] + [fixed_label]
            order = {color: cats}

        df["_total"] = df.groupby(x)[y].transform("sum")
        df["_share"] = np.where(df["_total"] > 0, df[y] / df["_total"] * 100, 0)

        fig = px.bar(
            df,
            x=x,
            y=y,
            color=color,
            opacity=0.6,
            color_discrete_map=color_map,
            color_discrete_sequence=palette,
            category_orders=order,
        )
        
        for tr in fig.data:
            nm = tr.name
            sub = df[df[color].astype(str) == str(nm)].copy()
            sub = sub.sort_values(x)
            tr.customdata = np.stack([sub["_share"]], axis=1)
            tr.hovertemplate = (
                f"{nm}"
                "<br>날짜: %{x}"
                "<br>검색량: %{y:,.0f}"
                "<br>비중: %{customdata[0]:.1f}%"
                "<extra></extra>"
            )

    fig.update_layout(barmode="relative")
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_layout(
        height=330,
        bargap=0.1,
        bargroupgap=0.2,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title=None,
        yaxis_title=None,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02,
            title_text=None,
        )
    )
    fig.update_xaxes(tickformat="%m월 %d일")
    st.plotly_chart(fig, use_container_width=True)


# C. 브랜드별 키워드 검색량 (KW)
def build_kwd_plotdf(df: pd.DataFrame, is_all: bool = False) -> pd.DataFrame:
    grp_col = "브랜드" if is_all else "키워드"

    plot_df = (
        df.groupby(["날짜_dt", grp_col], as_index=False)["검색량"]
        .sum()
    )

    if plot_df.empty:
        return plot_df

    min_date = plot_df["날짜_dt"].min()
    max_date = plot_df["날짜_dt"].max()
    all_x = pd.date_range(min_date, max_date)
    all_grp = plot_df[grp_col].astype(str).dropna().unique().tolist()

    idx = pd.MultiIndex.from_product([all_x, all_grp], names=["날짜_dt", grp_col])
    plot_df = (
        plot_df
        .set_index(["날짜_dt", grp_col])["검색량"]
        .reindex(idx, fill_value=0)
        .reset_index()
    )
    return plot_df


def render_kwd_graph(plot_df: pd.DataFrame, chart_type: str, is_all: bool = False) -> None:
    color_col = "브랜드" if is_all else "키워드"
    
    plot_df = plot_df.copy()
    plot_df["_total"] = plot_df.groupby("날짜_dt")["검색량"].transform("sum")
    plot_df["_share"] = np.where(plot_df["_total"] > 0, plot_df["검색량"] / plot_df["_total"] * 100, 0)

    if chart_type == "누적막대":
        fig = px.bar(
            plot_df,
            x="날짜_dt",
            y="검색량",
            color=color_col,
            barmode="relative",
        )
        for tr in fig.data:
            nm = tr.name
            sub = plot_df[plot_df[color_col].astype(str) == str(nm)].copy()
            sub = sub.sort_values("날짜_dt")
            tr.customdata = np.stack([sub["_share"]], axis=1)
            tr.hovertemplate = (
                f"{nm}"
                "<br>날짜: %{x}"
                "<br>검색량: %{y:,.0f}"
                "<br>비중: %{customdata[0]:.1f}%"
                "<extra></extra>"
            )
        fig.update_layout(barmode="relative")
        fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
        fig.update_traces(opacity=0.6)
    else:
        fig = px.line(
            plot_df,
            x="날짜_dt",
            y="검색량",
            color=color_col,
            markers=True,
        )
        for tr in fig.data:
            nm = tr.name
            sub = plot_df[plot_df[color_col].astype(str) == str(nm)].copy()
            sub = sub.sort_values("날짜_dt")
            tr.customdata = np.stack([sub["_share"]], axis=1)
            tr.hovertemplate = (
                f"{nm}"
                "<br>날짜: %{x}"
                "<br>검색량: %{y:,.0f}"
                "<br>비중: %{customdata[0]:.1f}%"
                "<extra></extra>"
            )
        fig.update_traces(opacity=0.6)
    
    # ✅ 범례/trace 순서: 총합 큰 순서대로
    trace_order = (
        plot_df.groupby(color_col, dropna=False)["검색량"]
        .sum()
        .sort_values(ascending=False)
        .index.astype(str)
        .tolist()
    )
    fig.data = tuple(sorted(fig.data, key=lambda tr: trace_order.index(str(tr.name)) if str(tr.name) in trace_order else 9999))

    fig.update_layout(
        height=330,
        bargap=0.1,
        bargroupgap=0.2,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title=None,
        yaxis_title=None,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02,
            title_text=None,
        )
    )
    fig.update_xaxes(tickformat="%m월 %d일")
    st.plotly_chart(fig, use_container_width=True)


def render_kwd_df(df: pd.DataFrame, is_all: bool = False) -> None:
    if is_all:
        long = (
            df.groupby(["브랜드", "날짜"], as_index=False)["검색량"]
            .sum()
            .rename(columns={"브랜드": "구분", "검색량": "값"})
        )
    else:
        long = (
            df.groupby(["키워드", "날짜"], as_index=False)["검색량"]
            .sum()
            .rename(columns={"키워드": "구분", "검색량": "값"})
        )

    pv = (
        long
        .pivot_table(
            index="구분",
            columns="날짜",
            values="값",
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    val_cols = [c for c in pv.columns if c != "구분"]
    styled = pv.style.format("{:,.0f}", subset=val_cols)
    st.dataframe(styled, use_container_width=True, row_height=30, hide_index=True)



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
    st.sidebar.header("Filter")
    st.sidebar.caption("영역별로 기간을 조정하세요.")

    # ────────────────────────────────────────────────────────────────
    # C) Data Load
    # ────────────────────────────────────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def load_data():
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        try:
            creds = Credentials.from_service_account_file(
                "C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope
            )
        except:
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str):
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url(
            "https://docs.google.com/spreadsheets/d/1Li4YzwsxI7rB3Q2Z0gkuGIyANTaxFrVzgsKE-RAAdME/edit?gid=2078920702#gid=2078920702"
        )

        PPL_LIST = pd.DataFrame(sh.worksheet("PPL_LIST").get_all_records())
        PPL_DATA = pd.DataFrame(sh.worksheet("PPL_DATA").get_all_records())

        wsa = sh.worksheet("PPL_ACTION")
        data = wsa.get("A1:P")
        PPL_ACTION = pd.DataFrame(data[1:], columns=data[0])
        
        # 하드코딩
        QUERY_SUM = pd.DataFrame(sh.worksheet("query_sum").get_all_records())
        QUERY_SLP = pd.DataFrame(sh.worksheet("query_슬립퍼").get_all_records())
        QUERY_NOR = pd.DataFrame(sh.worksheet("query_누어").get_all_records())
        QUERY_TDS = pd.DataFrame(sh.worksheet("query_토들즈").get_all_records())

        QUERY_SLP["날짜"] = pd.to_datetime(QUERY_SLP["날짜"])
        QUERY_NOR["날짜"] = pd.to_datetime(QUERY_NOR["날짜"])
        QUERY_TDS["날짜"] = pd.to_datetime(QUERY_TDS["날짜"])


        return PPL_LIST, PPL_DATA, PPL_ACTION, QUERY_SUM, QUERY_SLP, QUERY_NOR, QUERY_TDS

    # ──────────────────────────────────
    # C-1) tb_max -> get max date
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def get_max():
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        try:
            creds = Credentials.from_service_account_file(
                "C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope
            )
        except:
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str):
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url(
            "https://docs.google.com/spreadsheets/d/1Li4YzwsxI7rB3Q2Z0gkuGIyANTaxFrVzgsKE-RAAdME/edit?gid=2078920702#gid=2078920702"
        )
        wsa = sh.worksheet("PPL_LIST")
        
        df = pd.DataFrame(
            wsa.get_all_records(expected_headers=["채널명", "order"])
        )[["채널명", "order"]]

        df["order"] = pd.to_numeric(df["order"], errors="coerce")
        df = df.dropna(subset=["채널명", "order"])

        latest_ch = df.loc[df["order"].idxmax(), "채널명"] if not df.empty else None
        return latest_ch


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

    PPL_LIST, PPL_DATA, PPL_ACTION, QUERY_SUM, QUERY_SLP, QUERY_NOR, QUERY_TDS = load_data()

    progress_bar.progress(95, text="데이터 분석 및 시각화를 구성 중입니다...")
    time.sleep(0.4)

    progress_bar.progress(100, text="데이터 로드 완료!")
    time.sleep(0.6)

    progress_placeholder.empty()
    spacer_placeholder.empty()

    # ──────────────────────────────────
    # D) Header
    # ──────────────────────────────────
    st.subheader("언드·PPL 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px;line-height:1.5;">
            언드 시트와 네이버 DataLab 데이터를 기반으로 <b>채널별 성과와 브랜드별 검색량 및 파급 효과</b>를 확인하는 대시보드입니다.<br>
            </div>
            <div style="color:#6c757d;font-size:14px;line-height:2.0;">
            ※ 전일 데이터 업데이트 시점은 09시~10시 입니다.
            </div>
            """,
            unsafe_allow_html=True,
        )
    
    with col2:
        latest_ch = get_max()
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
                color:#7FD0C4;
                background-color:#F3FBFA;
                border:1.5px solid #7FD0C4;
                border-radius:14px;
                white-space:nowrap;
                cursor:default;">
                <span class="material-symbols-outlined" style="font-size:15px;">event_available</span>
                {latest_ch} 추가 완료
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
    # 필수 함수
    # ──────────────────────────────────
    CHANNELS_BY_BRAND = build_channels_by_brand(PPL_LIST)

    # ────────────────────────────────────────────────────────────────
    # 1) 채널별 성과 확인
    # ────────────────────────────────────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>채널별 성과 확인</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ협업한 유튜버나 인플루언서를 선택하여, 영상의 반응(조회수, 좋아요 등)과 영상을 타고 들어온 유저의 자사몰 행동 데이터를 확인합니다.", unsafe_allow_html=True)
    # 

    # PPL_DATA + PPL_ACTION + PPL_LIST를 합쳐서 채널 성과용 기본 DF 만드는 함수
    _df_merged = pd.merge(PPL_DATA, PPL_ACTION, on=['날짜', 'utm_camp', 'utm_content'], how='outer')
    df_merged = pd.merge(_df_merged, PPL_LIST, on=['utm_camp', 'utm_content'], how='left')
    df_merged_t = df_merged[[
        "날짜", "채널명", "Cost", "조회수", "좋아요수", "댓글수", "브랜드언급량", "링크클릭수",
        "session_count", "avg_session_duration_sec", "view_item_list_sessions",
        "view_item_sessions", "scroll_50_sessions", "product_option_price_sessions",
        "find_showroom_sessions", "add_to_cart_sessions", "sign_up_sessions",
        "showroom_10s_sessions", "showroom_leads_sessions"
    ]]
    numeric_cols = df_merged_t.columns.difference(["날짜", "채널명"])
    df_merged_t[numeric_cols] = df_merged_t[numeric_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)

    all_channel_info = PPL_LIST.sort_values(by="order", ascending=False)
    channel_opts = all_channel_info["채널명"].unique().tolist()

    if not channel_opts:
        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
    else:
        with st.expander("공통 Filter", expanded=True):
            f1, f2, _p, f3 = st.columns([1, 1, 0.08, 1.45], vertical_alignment="bottom")

            with f1:
                ch = st.selectbox("채널 선택", options=channel_opts, index=1, key="eng_ch_sel")

            with f2:
                target_metric = st.selectbox(
                    "그래프 지표 선택",
                    options=["조회수", "좋아요수", "댓글수", "브랜드언급량", "링크클릭수"],
                    index=0,
                    key="eng_metric_graph_sel"
                )

            with f3:
                metric_mode = st.radio(
                    "표 지표 선택",
                    options=["기본", "CVR 추가", "CPA 추가", "CVR + CPA 모두"],
                    horizontal=True,
                    key="eng_metric_radio"
                )

        info_col, chart_col = st.columns([1, 2.5])

        with info_col:
            r = all_channel_info[all_channel_info["채널명"] == ch].iloc[0]
            render_eng_card(r, CFG["BRAND_COLOR"])

        with chart_col:
            render_eng_graph(df_merged_t, ch, target_metric)

        st.markdown(" ")
        if metric_mode == "CVR + CPA 모두":
            opt = 4
        elif metric_mode == "CVR 추가":
            opt = 2
        elif metric_mode == "CPA 추가":
            opt = 3
        else:
            opt = 1

        render_eng_df(
            df_merged_t[df_merged_t["채널명"] == ch].copy(),
            select_option=opt
        )

    # ────────────────────────────────────────────────────────────────
    # 2) 채널별 검색량 기여도
    # ────────────────────────────────────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>채널별 검색량 기여도</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ검색량을 '기본 검색량'과 채널별 '기여 검색량'으로 나눠, 특정 채널이 이끌어낸 파급력을 측정합니다.", unsafe_allow_html=True)

    # with st.popover("🧐 작성 예정"):
    #         st.markdown("""

    #     """)

    # PPL_ACTION + PPL_LIST 기준으로 채널별 검색량 기여도 pivot DF 생성
    ppl_action2 = PPL_ACTION[['날짜', 'utm_content', 'SearchVolume_contribution']]
    ppl_action3 = pd.merge(ppl_action2, PPL_LIST, on=['utm_content'], how='left')
    ppl_action3 = ppl_action3[['날짜', '채널명', 'SearchVolume_contribution']]
    ppl_action3 = ppl_action3.pivot_table(
        index="날짜",
        columns="채널명",
        values="SearchVolume_contribution",
        aggfunc="sum"
    ).reset_index()

    today = date.today()
    yesterday = today - timedelta(days=1)
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)

    with st.expander("공통 Filter", expanded=True):
        f1, f2 = st.columns([1, 2.53], vertical_alignment="bottom")
        with f1: 
            selected_dates = st.date_input(
                "기간 선택",
                value=(first_day_prev_month, yesterday),
                key="query_contrib_date_filter"
            )

    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date = end_date = selected_dates[0] if selected_dates else yesterday


    # 렌더링
    # 1. 전체 + 브랜드별 탭 구성
    brand_tabs_labels = ["전체"] + [f"{brand}" for brand in CFG["BRAND_LIST"]]
    tabs = st.tabs(brand_tabs_labels)
    # 2. 브랜드별 df 구성
    query_sum_map = {
        brand_name: QUERY_SUM[QUERY_SUM["브랜드"] == brand_name].copy()
        for brand_name in CFG["BRAND_LIST"]
    }
    # 3. 전체 df 구성 (groupby 필수)
    query_sum_map["전체"] = (
        QUERY_SUM
        .groupby("날짜", as_index=False)["검색량"]
        .sum()
    )
    channels_by_brand_all = {
        **CHANNELS_BY_BRAND,
        "전체": [
            ch
            for brand_name in CFG["BRAND_LIST"]
            for ch in CHANNELS_BY_BRAND.get(brand_name, [])
        ]
    }

    def tab_ctb(
        brand_name: str,
        query_sum_df: pd.DataFrame,
        ppl_df: pd.DataFrame,
        channels_by_brand: dict,
        start_date,
        end_date
    ) -> None:
        df = build_ctb_rawdf(
            brand_name=brand_name,
            query_sum_df=query_sum_df,
            ppl_df=ppl_df,
            channels_by_brand=channels_by_brand
        )

        df["날짜_dt"] = pd.to_datetime(df["날짜"], format="%Y-%m-%d", errors="coerce")
        mask = (df["날짜_dt"] >= pd.to_datetime(start_date)) & (df["날짜_dt"] <= pd.to_datetime(end_date))
        df = df.loc[mask].copy()
        df["날짜"] = df["날짜_dt"].dt.strftime("%Y-%m-%d")

        channels = channels_by_brand.get(brand_name, [])
        plot_cols = channels + ["기본 검색량"]

        df_long = df.melt(
            id_vars="날짜",
            value_vars=plot_cols,
            var_name="콘텐츠",
            value_name="기여량"
        )
        render_ctb_graph(df_long, x="날짜", y="기여량", color="콘텐츠")
        render_ctb_df(df.drop(columns=["날짜_dt"]), channels_by_brand, brand=brand_name)

    with tabs[0]:
        tab_ctb(
            "전체",
            query_sum_map["전체"],
            ppl_action3,
            channels_by_brand_all,
            start_date,
            end_date
        )

    for tab, brand_name in zip(tabs[1:], CFG["BRAND_LIST"]):
        with tab:
            tab_ctb(
                brand_name,
                query_sum_map[brand_name],
                ppl_action3,
                channels_by_brand_all,
                start_date,
                end_date
            )


    # ────────────────────────────────────────────────────────────────
    # 3) 브랜드별 검색량 추이
    # ────────────────────────────────────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>브랜드별 검색량 추이</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ브랜드 검색량을 이루는 개별 키워드 검색량의 변화를 확인합니다.", unsafe_allow_html=True)

    today = date.today()
    yesterday = today - timedelta(days=1)
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)

    with st.expander("공통 Filter", expanded=True):
        f1, _p, f2 = st.columns([1, 0.08, 2.45], vertical_alignment="bottom") 
        with f1:
            selected_dates = st.date_input(
                "기간 선택",
                value=(first_day_prev_month, yesterday),
                key="keyword_volume_date_filter"
            )
        with f2:
            chart_type = st.radio(
                "그래프",
                ["꺾은선", "누적막대"],
                horizontal=True,
                index=0,
                key="kw_graph_type"
            )

    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date = end_date = selected_dates if selected_dates else yesterday

    local_vars = locals()

    query_df_map = {
        brand_name: local_vars[sheet_name].copy()
        for brand_name, sheet_name in zip(CFG["BRAND_LIST"], CFG["BRAND_SHEET"])
        if sheet_name in local_vars
    }

    if query_df_map:
        query_df_map["전체"] = pd.concat(
            [
                query_df_map[brand_name].assign(브랜드=brand_name)
                for brand_name in CFG["BRAND_LIST"]
                if brand_name in query_df_map
            ],
            ignore_index=True
        )

    def tab_kwd(target_df: pd.DataFrame, tk: str, is_all: bool = False) -> None:
        df = target_df.copy()
        df["날짜_dt"] = pd.to_datetime(df["날짜"], format="%Y-%m-%d", errors="coerce")
        mask = (df["날짜_dt"] >= pd.to_datetime(start_date)) & (df["날짜_dt"] <= pd.to_datetime(end_date))
        df = df.loc[mask].copy()
        df["날짜"] = df["날짜_dt"].dt.strftime("%Y-%m-%d")
        df["검색량"] = pd.to_numeric(df["검색량"], errors="coerce").fillna(0)

        if is_all:
            plot_df = build_kwd_plotdf(df, is_all=True)
            if plot_df.empty:
                st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                return
            render_kwd_graph(plot_df, chart_type=chart_type, is_all=True)
            render_kwd_df(df, is_all=True)
            return

        with st.expander("탭별 Filter", expanded=False):
            k_all = sorted(df["키워드"].astype(str).dropna().unique().tolist())
            use_custom_kw = st.checkbox(
                f"키워드 개별 선택 (전체 {len(k_all)}개 중)",
                value=False,
                key=f"k_toggle_{tk}"
            )
            if use_custom_kw:
                k_sel = st.multiselect(
                    "키워드 선택",
                    options=k_all,
                    default=k_all,
                    key=f"k_{tk}"
                )
            else:
                k_sel = k_all

        df = df[df["키워드"].astype(str).isin([str(x) for x in k_sel])]

        if df.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
            return

        plot_df = build_kwd_plotdf(df, is_all=False)
        if plot_df.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
            return

        render_kwd_graph(plot_df, chart_type=chart_type, is_all=False)
        render_kwd_df(df, is_all=False)


    if not query_df_map:
        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
    else:
        brand_tabs_labels = ["전체"] + [brand for brand in CFG["BRAND_LIST"] if brand in query_df_map]
        tabs = st.tabs(brand_tabs_labels)

        with tabs[0]:
            tab_kwd(
                query_df_map["전체"],
                tk="kw_all",
                is_all=True
            )

        for tab, brand_name in zip(tabs[1:], [brand for brand in CFG["BRAND_LIST"] if brand in query_df_map]):
            with tab:
                tab_kwd(
                    query_df_map[brand_name],
                    tk=f"kw_{brand_name}",
                    is_all=False
                )


if __name__ == '__main__':
    main()