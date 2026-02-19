# SEOHEE
# 2026-02-11 ver. (refac: keep same features)

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta
import plotly.express as px  # pie/bar만 사용
import plotly.graph_objects as go  # ✅ 상단에 있어도 되고, 밑에 블록 내부 go import도 유지할 것(요청)

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

AW_COLS  = {
            "awareness_type",
            "awareness_type_a",
            "awareness_type_b"
            }
CAT_COLS = [
            "shrm_type",
            "shrm_region",
            "shrm_branch",
            "demo_gender",
            "demo_age",
            "awareness_type",
            "purchase_purpose",
            "visit_type",
            ]

# ──────────────────────────────────
# DATE NORMALIZATION (단일 기준)
# ──────────────────────────────────
def _norm_dt(s: pd.Series | pd.Index | pd.DatetimeIndex) -> pd.Series:
    """
    날짜/시간이 섞여 들어와도 무조건 '날짜(00:00:00)'로 통일.
    스택/라인/피벗 등 모든 축의 기준을 단 하나로 유지.
    """
    x = pd.to_datetime(s, errors="coerce")
    return x.dt.normalize()


def _add_period_day(df1: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    ui.add_period_columns를 쓰되,
    _period_dt가 항상 normalize(00:00:00) 되도록 강제.
    """
    out = ui.add_period_columns(df1, date_col, "일별")
    if "_period_dt" in out.columns:
        out["_period_dt"] = _norm_dt(out["_period_dt"])
    return out


# ──────────────────────────────────
# HELPER
# ──────────────────────────────────
def _clean_cat(s: pd.Series) -> pd.Series:
    # ✅ "nan" 문자열/공백/None/NA 모두 "기타"로 통일 (기존 동작 유지 + 케이스 확장)
    ss = s.astype("string")
    ss = ss.str.strip()
    ss = ss.fillna("")
    ss = ss.replace(["nan", "NaN", "None", "<NA>"], "")
    ss = ss.replace("", "기타")
    return ss


def _order_with_etc_last(keys: list, sums: dict | None = None) -> list:
    sums = sums or {}
    ks = [str(k) for k in keys if str(k) != "nan" and str(k) != ""]
    etc = [k for k in ks if k == "기타"]
    others = [k for k in ks if k != "기타"]
    others = sorted(others, key=lambda k: float(sums.get(k, 0.0)), reverse=True)
    return others + etc


def _get_px_sequence() -> list[str]:
    # ✅ "막대가 쓰는 기본 팔레트"를 그대로 쓴다 (환경 기본값 복제)
    # seq = px.defaults.color_discrete_sequence
    seq = px.colors.qualitative.Set1
    if not isinstance(seq, (list, tuple)) or len(seq) == 0:
        seq = px.colors.qualitative.Plotly
    return list(seq)


def _make_color_map(order: list[str], seq: list[str]) -> dict[str, str]:
    if not order:
        return {}
    pal = (list(seq) * ((len(order) // len(seq)) + 1))[: len(order)]
    return dict(zip(order, pal))


def _agg_dim_for_order(d: pd.DataFrame, dim: str) -> pd.DataFrame:
    if dim not in d.columns:
        return pd.DataFrame(columns=[dim, "value"])

    key = _clean_cat(d[dim]).astype(str)

    if (dim in AW_COLS) and ("weight" in d.columns):
        out = (
            d.assign(_k=key)
            .groupby("_k", dropna=False)["weight"]
            .sum()
            .reset_index()
            .rename(columns={"_k": dim, "weight": "value"})
        )
    else:
        out = (
            d.assign(_k=key)
            .groupby("_k", dropna=False)
            .size()
            .reset_index(name="value")
            .rename(columns={"_k": dim})
        )

    out["value"] = pd.to_numeric(out["value"], errors="coerce").fillna(0)
    out[dim] = _clean_cat(out[dim]).astype(str)

    # clean으로 라벨 합쳐질 수 있으니 재집계
    out = out.groupby(dim, dropna=False, as_index=False)["value"].sum()
    out = out.sort_values("value", ascending=False).reset_index(drop=True)
    return out


def _parse_shrm_text(df1: pd.DataFrame) -> pd.DataFrame:
    if "shrm_name" in df1.columns:
        ss = (
            df1["shrm_name"]
            .astype("string")
            .fillna("")
            .astype(str)
            .str.strip()
        )

        parts = ss.str.split("_", n=2, expand=True)

        df1["shrm_type"] = parts[0].fillna("").str.strip().replace("", "기타")
        df1["shrm_branch"] = parts[1].fillna("").str.strip().replace("", "기타")
        df1["shrm_region"] = parts[2].fillna("").str.strip().replace("", "기타")

    else:
        df1["shrm_type"] = "기타"
        df1["shrm_branch"] = "기타"
        df1["shrm_region"] = "기타"

    return df1


def render_shrm_tabs(
    df: pd.DataFrame,
    df_aw: pd.DataFrame,
    title: str,
    conf: dict,
    key_tag: str = "tab",
    agg_mode: str = "auto",        # "auto" | "size" | "sum"
    agg_value_col: str | None = None,  # ex) "cnt"  (sum일 때만 의미)
):
    pie_dim = conf["pie"]
    x = conf["stack_x"]
    c = conf["stack_color"]

    if df is None or df.empty:
        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        return

    use_aw = (pie_dim in AW_COLS) or (c in AW_COLS)
    src = df_aw if (use_aw and df_aw is not None and not df_aw.empty) else df

    # agg_mode
    mode = (agg_mode or "auto").lower().strip()
    if mode not in ("auto", "size", "sum"):
        mode = "auto"

    if mode == "auto":
        if agg_value_col and (agg_value_col in src.columns):
            mode2 = "sum"
        else:
            mode2 = "size"
    else:
        mode2 = mode

    # sum을 강제했는데 컬럼이 없으면 0으로 만들어서 동작만 유지 (굳이)
    if mode2 == "sum":
        if (not agg_value_col) or (agg_value_col not in src.columns):
            src[agg_value_col or "_cnt"] = 0
            agg_value_col = agg_value_col or "_cnt"
        src[agg_value_col] = pd.to_numeric(src[agg_value_col], errors="coerce").fillna(0)

    # 팔레트/맵 로직 단일화: 전역 함수만 사용
    seq = _get_px_sequence()

    # 1) order/cmap 기준: stack_color(c) 기준 합계 큰 순 + 기타 맨뒤
    bar_order = None
    bar_cmap = None
    if c in src.columns:
        if mode2 == "sum":
            d_ord = (
                src.assign(_k=_clean_cat(src[c]).astype(str))
                .groupby("_k", dropna=False)[agg_value_col]
                .sum()
                .reset_index()
                .rename(columns={"_k": c, agg_value_col: "value"})
            )
            d_ord["value"] = pd.to_numeric(d_ord["value"], errors="coerce").fillna(0)
            d_ord[c] = _clean_cat(d_ord[c]).astype(str)
            d_ord = d_ord.groupby(c, dropna=False, as_index=False)["value"].sum()
            d_ord = d_ord.sort_values("value", ascending=False).reset_index(drop=True)
        else:
            d_ord = _agg_dim_for_order(src, c)

        if not d_ord.empty:
            sums = d_ord.set_index(c)["value"].to_dict()
            bar_order = _order_with_etc_last(list(sums.keys()), sums)
            bar_cmap = _make_color_map(bar_order, seq)

    pv = None
    c1, c2 = st.columns([3, 7], vertical_alignment="top")

    # 2) PIE
    with c1:
        if pie_dim not in src.columns:
            st.warning("표시할 데이터가 없습니다.")
        else:
            if mode2 == "sum":
                d_pie = (
                    src.assign(_k=_clean_cat(src[pie_dim]).astype(str))
                    .groupby("_k", dropna=False)[agg_value_col]
                    .sum()
                    .reset_index()
                    .rename(columns={"_k": pie_dim, agg_value_col: "value"})
                )
                d_pie["value"] = pd.to_numeric(d_pie["value"], errors="coerce").fillna(0)
                d_pie[pie_dim] = _clean_cat(d_pie[pie_dim]).astype(str)
                d_pie = d_pie.groupby(pie_dim, dropna=False, as_index=False)["value"].sum()
                d_pie = d_pie.sort_values("value", ascending=False).reset_index(drop=True)
            else:
                d_pie = _agg_dim_for_order(src, pie_dim)

            if d_pie.empty:
                st.warning("표시할 데이터가 없습니다.")
            else:
                sums_p = d_pie.set_index(pie_dim)["value"].to_dict()
                pie_order = _order_with_etc_last(list(sums_p.keys()), sums_p)

                if (pie_dim == c) and (bar_order is not None) and (bar_cmap is not None):
                    pie_order = [k for k in bar_order if k in pie_order] + [
                        k for k in pie_order if k not in bar_order
                    ]
                    pie_cmap = {k: bar_cmap[k] for k in pie_order if k in bar_cmap}
                    missing = [k for k in pie_order if k not in pie_cmap]
                    if missing:
                        pie_cmap.update(_make_color_map(missing, seq))
                else:
                    pie_cmap = _make_color_map(pie_order, seq)

                d_pie[pie_dim] = pd.Categorical(
                    d_pie[pie_dim].astype(str), categories=pie_order, ordered=True
                )
                d_pie = d_pie.sort_values(pie_dim).reset_index(drop=True)

                fig1 = px.pie(
                    d_pie,
                    names=pie_dim,
                    values="value",
                    color=pie_dim,
                    color_discrete_map=pie_cmap,
                    category_orders={pie_dim: pie_order},
                    title=None,
                )
                fig1.update_traces(opacity=0.6)
                fig1.update_layout(
                    height=360,
                    margin=dict(l=0, r=0, t=30, b=30),
                    showlegend=False,
                )
                st.plotly_chart(
                    fig1,
                    use_container_width=True,
                    key=f"pie::{key_tag}::{title}::{pie_dim}",
                )

    # 3) STACK
    with c2:
        if x in src.columns and c in src.columns:
            if x == "event_date":
                base = _add_period_day(src, "event_date")
                base = base.assign(**{c: _clean_cat(base[c]).astype(str)})
                
                # 주별/일별 자동 판단
                is_week = False
                if "_period" in base.columns:
                    is_week = base["_period"].astype(str).str.contains("~").any()

                x_col = "_period" if is_week else "_period_dt"
                sort_col = "_period_dt"

                if mode2 == "sum":
                    agg = (
                        base.groupby([x_col, "_period", c], dropna=False)[agg_value_col]
                        .sum()
                        .reset_index(name="value")
                        .sort_values(sort_col)
                        .reset_index(drop=True)
                    )
                else:
                    if (c in AW_COLS) and ("weight" in base.columns):
                        agg = (
                            base.groupby([x_col, "_period", c], dropna=False)["weight"]
                            .sum()
                            .reset_index(name="value")
                            .sort_values(sort_col)
                            .reset_index(drop=True)
                        )
                    else:
                        agg = (
                            base.groupby([x_col, "_period", c], dropna=False)
                            .size()
                            .reset_index(name="value")
                            .sort_values(sort_col)
                            .reset_index(drop=True)
                        )

                agg["value"] = pd.to_numeric(agg["value"], errors="coerce").fillna(0)

                if bar_order is not None:
                    agg[c] = pd.Categorical(agg[c].astype(str), categories=bar_order, ordered=True)
                    agg = agg.sort_values([sort_col, c]).reset_index(drop=True)

                ui.render_stack_graph(
                    agg,
                    x=x_col,
                    y="value",
                    color=c,
                    height=360,
                    opacity=0.6,
                    show_value_in_hover=True,
                    key=f"stack::{key_tag}::{title}::{c}",
                    color_discrete_map=(bar_cmap or None),
                    category_orders={c: (bar_order or None)},
                )

                pv = ui.build_pivot_table(agg, index_col=c, col_col=x_col, val_col="value")

            else:
                tmp = src.assign(**{c: _clean_cat(src[c]).astype(str)})

                if mode2 == "sum":
                    agg = (
                        tmp.groupby([x, c], dropna=False)[agg_value_col]
                        .sum()
                        .reset_index(name="value")
                    )
                else:
                    if (c in AW_COLS) and ("weight" in tmp.columns):
                        agg = (
                            tmp.groupby([x, c], dropna=False)["weight"]
                            .sum()
                            .reset_index(name="value")
                        )
                    else:
                        agg = (
                            tmp.groupby([x, c], dropna=False)
                            .size()
                            .reset_index(name="value")
                        )

                agg["value"] = pd.to_numeric(agg["value"], errors="coerce").fillna(0)
                agg[x] = agg[x].astype(str)
                agg[c] = _clean_cat(agg[c]).astype(str)

                if bar_order is not None:
                    agg[c] = pd.Categorical(agg[c].astype(str), categories=bar_order, ordered=True)
                    agg = agg.sort_values([x, c]).reset_index(drop=True)

                ui.render_stack_graph(
                    agg,
                    x=x,
                    y="value",
                    color=c,
                    height=360,
                    opacity=0.6,
                    show_value_in_hover=True,
                    key=f"stack::{key_tag}::{title}::{x}::{c}",
                    color_discrete_map=(bar_cmap or None),
                    category_orders={c: (bar_order or None)},
                )

                pv = ui.build_pivot_table(agg, index_col=c, col_col=x, val_col="value")
        else:
            st.warning("표시할 데이터가 없습니다.")

    if pv is not None:
        st.dataframe(pv, use_container_width=True, hide_index=True, row_height=30)
    else:
        st.warning("표시할 데이터가 없습니다.")



# ──────────────────────────────────
# INSIGHT (CROSS)
# ──────────────────────────────────
def write_mutable_insight(
    agg: pd.DataFrame,
    row_col: str,
    col_col: str,
    row_label: str,
    col_label: str,
    row_order: list[str],
    col_order: list[str],
    min_row_total: int = 5,
    strong_pct: float = 50.0,
    gap_pct: float = 20.0,
    topk: int = 3,
):
    if agg is None or agg.empty:
        return ["선택한 조건에 해당하는 데이터가 없습니다."]

    d = agg[[row_col, col_col, "value"]].copy()
    d[row_col] = d[row_col].astype(str)
    d[col_col] = d[col_col].astype(str)
    d["value"] = pd.to_numeric(d["value"], errors="coerce").fillna(0)

    col_sum = d.groupby(col_col, dropna=False)["value"].sum().sort_values(ascending=False)
    if col_sum.empty:
        return ["선택한 조건에 해당하는 데이터가 없습니다."]

    total = float(col_sum.sum()) if float(col_sum.sum()) != 0 else 1.0
    top_cols = [c for c in col_order if c in col_sum.index][:topk] or col_sum.index.astype(str).tolist()[:topk]

    lines = []
    lines.append(
        f"전체적으로 **{col_label}**에서는 "
        + ", ".join([f"**{c}**({col_sum[c]/total*100:.0f}%)" for c in top_cols])
        + " 순으로 많이 나타납니다."
    )

    row_tot = d.groupby(row_col, dropna=False)["value"].sum()
    d["_row_sum"] = d.groupby(row_col, dropna=False)["value"].transform("sum").replace(0, np.nan)
    d["pct_row"] = (d["value"] / d["_row_sum"] * 100).fillna(0)
    d = d.drop(columns=["_row_sum"])

    for r in row_order:
        if r not in row_tot.index:
            continue
        if float(row_tot[r]) < float(min_row_total):
            continue

        rr = d[d[row_col] == r].sort_values("pct_row", ascending=False)
        if rr.empty:
            continue

        c1 = rr.iloc[0][col_col]
        v1 = float(rr.iloc[0]["pct_row"])
        v2 = float(rr.iloc[1]["pct_row"]) if len(rr) > 1 else 0.0

        if (v1 >= strong_pct) or ((v1 - v2) >= gap_pct):
            lines.append(f"- **{r}**에서는 **{c1}**이(가) {v1:.0f}%로 가장 많이 나타납니다.")

    top1 = str(col_sum.index[0])
    top1_pct = float(col_sum.iloc[0] / total * 100)
    if top1_pct >= 40:
        lines.append(f"- 전체적으로 **{top1}** 중심으로 구성되어 있습니다. ({top1_pct:.0f}%)")

    return lines


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
        max_value=default_end,
    )
    cs = start_date.strftime("%Y%m%d")
    ce = (end_date + timedelta(days=1)).strftime("%Y%m%d")

    # ──────────────────────────────────
    # C) Data Load
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def load_data(cs: str, ce: str):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive",]
        try:
            creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope,)
        except:
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str):
                import json
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1g2HWpm3Le3t3P3Hb9nm2owoiaxywaXv--L0SHEDx3rQ/edit")
        
        df1 = pd.DataFrame(sh.worksheet("shrm_data").get_all_records())
        df2 = pd.DataFrame(sh.worksheet("shrm_nplace").get_all_records())
        
        # 쇼룸 정보가 없으면 라인 삭제
        for d in [df1, df2]:
            d["shrm_name"] = (
                d["shrm_name"]
                .astype("string")
                .str.strip()
                .replace(["", "nan", "NaN", "None", "none", "null", "<NA>"], pd.NA)
            )

            d.dropna(subset=["shrm_name"], inplace=True)
            d.drop(d[d["shrm_name"].str.len() < 2].index, inplace=True)

        # (정규화) GS용 날짜 깨짐 방지
        df1["event_date"] = df1["event_date"].astype("string").str.strip()
        df1["event_date"] = pd.to_datetime(df1["event_date"], format="%Y. %m. %d", errors="coerce")
        df1["event_date"] = _norm_dt(df1["event_date"])
        df2["event_date"] = df2["event_date"].astype("string").str.strip()
        df2["event_date"] = pd.to_datetime(df2["event_date"], format="%Y. %m. %d", errors="coerce")
        df2["event_date"] = _norm_dt(df2["event_date"])

        # (파생컬럼) {쇼룸 유형}_{쇼룸 지점}_{쇼룸 권역}

        df1 = _parse_shrm_text(df1)
        df2 = _parse_shrm_text(df2)

        # (범주화) 카테고리컬 변환
        for d in [df1, df2]:
            for c in CAT_COLS:
                if c in d.columns:
                    d[c] = d[c].astype("category")

        # 기간 필터
        cs_dt = pd.to_datetime(cs, format="%Y%m%d", errors="coerce")
        ce_dt = pd.to_datetime(ce, format="%Y%m%d", errors="coerce")

        for d in [df1, df2]:
            if "event_date" in d.columns:
                d["event_date"] = pd.to_datetime(d["event_date"], errors="coerce")
                d.drop(
                    d[(d["event_date"] < cs_dt) | (d["event_date"] >= ce_dt)].index,
                    inplace=True
                )

        return df1, df2

    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df1, df2 = load_data(cs, ce)

    # ──────────────────────────────────
    # D) Header
    # ──────────────────────────────────
    st.subheader("쇼룸 대시보드 (제작중)")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px; line-height:1.5;">
            대시보드 설명  
            </div>
            <div style="color:#6c757d; font-size:14px; line-height:2.0;">
            ※ 설명  
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            """
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
            unsafe_allow_html=True,
        )

    st.divider()


    # ──────────────────────────────────
    # 1) 조회·예약·방문 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>쇼룸 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    # 방문(df1) + 조회/예약(df2) --> long 통합
    base_cols = [c for c in ["event_date", "shrm_name", "shrm_type", "shrm_region", "shrm_branch"] if c in df1.columns]
    v = df1.loc[:, base_cols].assign(event_type="방문", cnt=1)

    m_cols = [c for c in ["look_cnt", "bookConfirmed_cnt"] if c in df2.columns]
    if not m_cols:
        df2["look_cnt"] = 0
        df2["bookConfirmed_cnt"] = 0
        m_cols = ["look_cnt", "bookConfirmed_cnt"]

    m_base_cols = [c for c in ["event_date", "shrm_name", "shrm_type", "shrm_region", "shrm_branch"] if c in df2.columns]
    m = df2.loc[:, m_base_cols + m_cols]
    m[m_cols] = m[m_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    m = m.melt(
        id_vars=m_base_cols,
        value_vars=m_cols,
        var_name="event_type",
        value_name="cnt",
    )
    m["event_type"] = m["event_type"].replace({"look_cnt": "조회", "bookConfirmed_cnt": "예약"})
    m["cnt"] = pd.to_numeric(m["cnt"], errors="coerce").fillna(0)

    df_evt = pd.concat([v, m], ignore_index=True)
    df_evt["event_date"] = pd.to_datetime(df_evt["event_date"], errors="coerce").dt.normalize()
    df_evt["event_type"] = df_evt["event_type"].astype(str).str.strip().replace("", "기타")
    df_evt["cnt"] = pd.to_numeric(df_evt["cnt"], errors="coerce").fillna(0)

    # ✅ 공통 필터
    with st.expander("Filter", expanded=True):
        c1, c2, c3, c4 = st.columns(4)

        def _opts(col, all_label="전체"):
            s = df_evt[col].astype("string").fillna("").str.strip().replace("", "기타") if col in df_evt.columns else pd.Series([], dtype="string")
            o = sorted(s.dropna().unique().astype(str).tolist())
            return ([all_label] + o) if all_label else o

        evt_opts = _opts("event_type", all_label=None)
        
        # evt_idx = evt_opts.index("방문") if "방문" in evt_opts else 0
        # with c1: sel_evt    = st.radio("이벤트", options=evt_opts, index=evt_idx, horizontal=True, key="df_evt_filter__evt")
        
        with c1:
            evt_opts_raw = _opts("event_type", all_label=None)
            _ord = [k for k in ["조회","예약","방문"] if k in evt_opts_raw]
            sel_evt = st.radio("이벤트", options=_ord, index=_ord.index("방문") if "방문" in _ord else 0, horizontal=True, key="df_evt_filter__evt")
        with c2: sel_type   = st.selectbox("쇼룸형태",  _opts("shrm_type"),   0, key="df_evt_filter__type")
        with c3: sel_region = st.selectbox("쇼룸권역",  _opts("shrm_region"), 0, key="df_evt_filter__region")
        with c4: sel_branch = st.selectbox("쇼룸지점",  _opts("shrm_branch"), 0, key="df_evt_filter__branch")

        df_evt_f = df_evt[df_evt["event_type"] == sel_evt]
        for col, val in [("shrm_type", sel_type), ("shrm_region", sel_region), ("shrm_branch", sel_branch)]:
            if val != "전체" and col in df_evt_f.columns:
                df_evt_f = df_evt_f[df_evt_f[col].astype(str) == str(val)]


    DIM_MAP = {
        "쇼룸형태": {
            "pie": "shrm_type",
            "stack_x": "event_date",
            "stack_color": "shrm_type",
            "raw_cols": ["event_date", "shrm_type"],
        },
        "쇼룸권역": {
            "pie": "shrm_region",
            "stack_x": "event_date",
            "stack_color": "shrm_region",
            "raw_cols": ["event_date", "shrm_region"],
        },
        "쇼룸지점": {
            "pie": "shrm_branch",
            "stack_x": "event_date",
            "stack_color": "shrm_branch",
            "raw_cols": ["event_date", "shrm_branch"],
        },
    }

    tabs = st.tabs(list(DIM_MAP.keys()))
    for tab, name in zip(tabs, DIM_MAP.keys()):
        with tab:
            render_shrm_tabs(
                df=df_evt_f,               # 필터 적용
                df_aw=None,
                title=name,
                conf=DIM_MAP[name],
                key_tag="status",          
                agg_mode="sum",            # cnt 합계
                agg_value_col="cnt",
            )


    # ──────────────────────────────────
    # 2) Tabs
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>방문 현황 </h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명")

    # ✅ 공통 필터
    with st.expander("Filter", expanded=True):
        f1, f2, f3 = st.columns(3)

        def _opts2(d: pd.DataFrame, col: str, all_label: str = "전체"):
            if d is None or d.empty or col not in d.columns:
                return [all_label]
            s = d[col].astype("string").fillna("").str.strip().replace("", "기타")
            o = sorted(s.dropna().unique().astype(str).tolist())
            return [all_label] + o

        with f1:
            sel_type2 = st.selectbox("쇼룸형태", _opts2(df1, "shrm_type"), 0, key="visit_filter__type")
        with f2:
            sel_region2 = st.selectbox("쇼룸권역", _opts2(df1, "shrm_region"), 0, key="visit_filter__region")
        with f3:
            sel_branch2 = st.selectbox("쇼룸지점", _opts2(df1, "shrm_branch"), 0, key="visit_filter__branch")

    # awareness_type: 콤마 멀티값 분해 + weight + (괄호)/(괄호제외) 분리
    df_aw = None
    if df1 is not None and not df1.empty and "awareness_type" in df1.columns:
        _rid = np.arange(len(df1))
        s = df1["awareness_type"].astype("string").fillna("").astype(str)

        lst = s.apply(lambda x: [t.strip() for t in str(x).split(",") if t.strip()] or ["기타"])
        n = lst.apply(len).astype(float).replace(0, 1.0)

        df_aw = df1.assign(_rid=_rid, awareness_type_list=lst, _n=n)
        df_aw = df_aw.explode("awareness_type_list", ignore_index=True)

        df_aw["awareness_type"] = df_aw["awareness_type_list"].astype(str).str.strip()
        df_aw["weight"] = (1.0 / df_aw["_n"]).astype(float)

        df_aw["awareness_type_a"] = (
            df_aw["awareness_type"]
            .astype(str)
            .str.extract(r"\((.*?)\)", expand=False)
            .fillna("기타")
            .replace("", "기타")
        )
        df_aw["awareness_type_b"] = (
            df_aw["awareness_type"]
            .astype(str)
            .str.replace(r"\(.*?\)", "", regex=True)
            .str.strip()
            .replace("", "기타")
        )

        df_aw = df_aw.drop(columns=["awareness_type_list", "_n"])

    # ✅ 필터 적용
    df1_f = df1
    if df1_f is not None and not df1_f.empty:
        for col, val in [("shrm_type", sel_type2), ("shrm_region", sel_region2), ("shrm_branch", sel_branch2)]:
            if val != "전체" and col in df1_f.columns:
                df1_f = df1_f[df1_f[col].astype(str) == str(val)]

    df_aw_f = df_aw
    if df_aw_f is not None and not df_aw_f.empty:
        for col, val in [("shrm_type", sel_type2), ("shrm_region", sel_region2), ("shrm_branch", sel_branch2)]:
            if val != "전체" and col in df_aw_f.columns:
                df_aw_f = df_aw_f[df_aw_f[col].astype(str) == str(val)]

    DIM_MAP = {
        "방문유형": {
            "pie": "visit_type",
            "stack_x": "event_date",
            "stack_color": "visit_type",
            "raw_cols": ["event_date", "visit_type"],
        },
        "데모그래픽": {
            "pie": "demo_gender",
            "stack_x": "demo_age",
            "stack_color": "demo_gender",
            "raw_cols": ["event_date", "demo_gender", "demo_age"],
        },
        "인지단계": {
            "pie": "awareness_type_a",
            "stack_x": "event_date",
            "stack_color": "awareness_type_a",
            "raw_cols": ["event_date", "awareness_type_a"],
        },
        "인지채널": {
            "pie": "awareness_type_b",
            "stack_x": "event_date",
            "stack_color": "awareness_type_b",
            "raw_cols": ["event_date", "awareness_type_b"],
        },
        "구매목적": {
            "pie": "purchase_purpose",
            "stack_x": "event_date",
            "stack_color": "purchase_purpose",
            "raw_cols": ["event_date", "purchase_purpose"],
        },
    }

    tabs = st.tabs(list(DIM_MAP.keys()))
    for tab, name in zip(tabs, DIM_MAP.keys()):
        with tab:
            render_shrm_tabs(
                df=df1_f,          # ✅ 필터 적용
                df_aw=df_aw_f,      # ✅ 필터 적용
                title=name,
                conf=DIM_MAP[name],
                key_tag="detail",
                agg_mode="size",
                agg_value_col=None,
            )


    # ──────────────────────────────────
    # 3) CROSS INSIGHT
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>CROSS INSIGHT </h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 ")

    DIM_OPTS = {
        "쇼룸형태": "shrm_type",
        "쇼룸권역": "shrm_region",
        "쇼룸지점": "shrm_branch",
        "방문유형": "visit_type",
        "성별"   : "demo_gender",
        "연령대" : "demo_age",
        "인지단계": "awareness_type_a",
        "인지채널": "awareness_type_b",
        "구매목적": "purchase_purpose",
    }

    with st.expander("Filter", expanded=True):
        cc1, cc2 = st.columns(2)
        with cc1:
            row_label = st.selectbox(
                "분석 기준 (*선택한 항목으로 데이터를 나눕니다.)",
                options=list(DIM_OPTS.keys()),
                index=5,
                key="cross_row",
            )
        with cc2:
            col_label = st.selectbox(
                "구성 기준 (*선택한 항목의 구성 비중을 표시합니다.)",
                options=[k for k in DIM_OPTS.keys() if k != row_label],
                index=7,
                key="cross_col",
            )

    row_col = DIM_OPTS[row_label]
    col_col = DIM_OPTS[col_label]

    has_row = (row_col in df1.columns) or (df_aw is not None and row_col in df_aw.columns)
    has_col = (col_col in df1.columns) or (df_aw is not None and col_col in df_aw.columns)

    if not (has_row and has_col):
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
    else:
        use_aw = (row_col in AW_COLS) or (col_col in AW_COLS)

        if use_aw:
            if df_aw is None or df_aw.empty:
                st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
                agg = None
            else:
                agg = (
                    df_aw.groupby([row_col, col_col], dropna=False)["weight"]
                    .sum()
                    .reset_index(name="value")
                )
        else:
            agg = df1.groupby([row_col, col_col], dropna=False).size().reset_index(name="value")

        if agg is None or agg.empty:
            st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        else:
            agg[row_col] = _clean_cat(agg[row_col])
            agg[col_col] = _clean_cat(agg[col_col])
            agg["value"] = pd.to_numeric(agg["value"], errors="coerce").fillna(0)

            # ✅ clean 후 라벨 합쳐질 수 있으니 키 기준 재집계(중복 제거)
            agg = agg.groupby([row_col, col_col], dropna=False, as_index=False)["value"].sum()

            row_sum = agg.groupby(row_col, dropna=False)["value"].sum().sort_values(ascending=False)
            base_order = row_sum.index.astype(str).tolist()
            etc_in = [k for k in ["기타"] if k in base_order]

            if row_col == "demo_age":
                age_order = ["20대", "30대", "40대", "50대", "60대 이상"]
                row_order = (
                    [x for x in age_order if x in base_order]
                    + [x for x in base_order if (x not in age_order) and (x not in etc_in)]
                    + etc_in
                )
            else:
                row_order = [x for x in base_order if x not in etc_in] + etc_in

            col_sum = agg.groupby(col_col, dropna=False)["value"].sum().sort_values(ascending=False)
            col_order = col_sum.index.astype(str).tolist()
            etc_in_col = [k for k in ["기타"] if k in col_order]
            col_order = [x for x in col_order if x not in etc_in_col] + etc_in_col

            agg["_row_sum"] = agg.groupby(row_col, dropna=False)["value"].transform("sum").replace(0, np.nan)
            agg["pct_row"] = (agg["value"] / agg["_row_sum"] * 100).fillna(0)
            agg = agg.drop(columns=["_row_sum"])

            pv_cnt = ui.build_pivot_table(agg, index_col=row_col, col_col=col_col, val_col="value")
            pv_pct = ui.build_pivot_table(agg, index_col=row_col, col_col=col_col, val_col="pct_row")

            pv_cnt = pv_cnt.set_index(row_col).reindex(row_order).reset_index()
            pv_pct = pv_pct.set_index(row_col).reindex(row_order).reset_index()

            cnt_cols = [c for c in pv_cnt.columns if c != row_col]
            cnt_cols = [c for c in col_order if c in cnt_cols]
            pv_cnt = pv_cnt[[row_col] + cnt_cols]

            pct_cols = [c for c in pv_pct.columns if c != row_col]
            pct_cols = [c for c in col_order if c in pct_cols]
            pv_pct = pv_pct[[row_col] + pct_cols]

            
            
            # ── 누적 가로막대(행=100%)
            import plotly.graph_objects as go  # ✅ 요청대로 여기 블록의 go import는 유지

            bar = agg[[row_col, col_col, "pct_row"]].copy()
            bar = bar.rename(columns={"pct_row": "pct"})
            bar["pct"] = pd.to_numeric(bar["pct"], errors="coerce").fillna(0)

            fig = go.Figure()

            # ✅ 범례/그래프 순서 정합: stack에서 보이는 체감 순서에 맞추려면 역순으로 그리기
            # - 범례는 trace 추가 순서
            # - stack에서 보이는 블록 순서(위/아래)는 체감상 반대로 느껴질 수 있어 역순이 맞는 경우가 많음
            col_order_draw = list(col_order)[::-1]

            for cc in col_order_draw:
                s = (
                    bar[bar[col_col].astype(str) == str(cc)]
                    .groupby(row_col, dropna=False)["pct"]
                    .sum()
                    .reindex(row_order)
                    .fillna(0)
                )

                fig.add_bar(
                    y=row_order,
                    x=s.values,
                    name=str(cc),
                    orientation="h",
                    text=(s.round(0).astype(int).astype(str) + "%").values,
                    textposition="inside",
                    hovertemplate="%{y}<br>%{fullData.name}: %{x:.1f}%<extra></extra>",
                    opacity=0.7,  # opacity
                )

            fig.update_layout(
                barmode="stack",
                height=150 + (len(row_order) * 30),
                margin=dict(l=10, r=10, t=70, b=20),
                xaxis_title=None,
                yaxis_title=None,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.15,
                    xanchor="right",
                    x=1,
                    title_text="",
                    traceorder="normal",  # ✅ trace 추가 순서 그대로
                ),
            )

            fig.update_xaxes(range=[0, 100], ticksuffix="%")
            fig.update_yaxes(categoryorder="array", categoryarray=row_order, autorange="reversed")

            st.plotly_chart(fig, use_container_width=True, key="cross_stack_100")


            pv_show = pv_cnt.copy()
            for cc in [c for c in pv_show.columns if c != row_col]:
                if cc in pv_pct.columns:
                    pv_show[cc] = (
                        pv_cnt[cc].fillna(0).astype(int).astype(str)
                        + " ("
                        + pv_pct[cc].fillna(0).round(0).astype(int).astype(str)
                        + "%)"
                    )

            st.dataframe(pv_show, use_container_width=True, hide_index=True, row_height=30)

            # insight_lines = write_mutable_insight(
            #     agg=agg,
            #     row_col=row_col,
            #     col_col=col_col,
            #     row_label=row_label,
            #     col_label=col_label,
            #     row_order=row_order,
            #     col_order=col_order,
            # )
            # st.write("시범기능입니다..")
            # st.success("\n".join(insight_lines), icon="✅")


if __name__ == "__main__":
    main()
