# SEOHEE
# 2026-02-11 ver. (refac: keep same features)

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta
import plotly.express as px  # pie/bar만 사용

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
    "DEFAULT_LOOKBACK_DAYS": 7,
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

def render_shrm_tabs(df: pd.DataFrame, df_aw: pd.DataFrame, title: str, conf: dict):
    pie_dim = conf["pie"]
    x = conf["stack_x"]
    c = conf["stack_color"]

    if df is None or df.empty:
        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        return

    AW_COLS = {"awareness_type", "awareness_type_a", "awareness_type_b"}
    use_aw = (pie_dim in AW_COLS) or (c in AW_COLS)
    src = df_aw if (use_aw and df_aw is not None and not df_aw.empty) else df

    # ✅ Stack에서 쓰는 차원 기준으로 팔레트 고정(파이/스택 톤 통일)
    dim_for_map = c if c in src.columns else pie_dim
    color_map = None
    if dim_for_map in src.columns:
        cats = _clean_cat(src[dim_for_map]).unique().tolist()
        palette = (px.defaults.color_discrete_sequence * ((len(cats) // 10) + 1))[:len(cats)]
        color_map = dict(zip(cats, palette))

    pv = None
    c1, c2 = st.columns([3, 7], vertical_alignment="top")

    # ── Pie ─────────────────────────
    with c1:
        if pie_dim in src.columns:
            if (pie_dim in AW_COLS) and ("weight" in src.columns):
                d_pie = (
                    src.groupby(pie_dim, dropna=False)["weight"]
                       .sum()
                       .reset_index(name="value")
                )
            else:
                d_pie = (
                    src.groupby(pie_dim, dropna=False)
                       .size()
                       .reset_index(name="value")
                )

            d_pie[pie_dim] = _clean_cat(d_pie[pie_dim])
            d_pie = d_pie.sort_values("value", ascending=False)

            fig1 = px.pie(
                d_pie,
                names=pie_dim,
                values="value",
                title=None,
                color=pie_dim,
                color_discrete_map=(color_map if (color_map is not None and pie_dim == dim_for_map) else None),
            )
            fig1.update_layout(height=360, margin=dict(l=0, r=0, t=30, b=30), showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("Pie 차원 컬럼이 없습니다.")

    # ── Stack ─────────────────────────
    with c2:
        if x in src.columns and c in src.columns:
            if x == "event_date":
                base = ui.add_period_columns(src, "event_date", "일별")

                if (c in AW_COLS) and ("weight" in base.columns):
                    agg = (
                        base.groupby(["_period_dt", "_period", c], dropna=False)["weight"]
                            .sum()
                            .reset_index(name="value")
                            .rename(columns={"_period": "기간"})
                            .sort_values("_period_dt")
                            .reset_index(drop=True)
                    )
                else:
                    agg = (
                        base.groupby(["_period_dt", "_period", c], dropna=False)
                            .size()
                            .reset_index(name="value")
                            .rename(columns={"_period": "기간"})
                            .sort_values("_period_dt")
                            .reset_index(drop=True)
                    )

                agg[c] = _clean_cat(agg[c])

                fig2 = px.bar(
                    agg, x="_period_dt", y="value", color=c,
                    barmode="stack", opacity=0.6,
                    color_discrete_map=color_map if color_map is not None else None,
                )
                fig2.update_layout(
                    height=360, margin=dict(l=10, r=140, t=20, b=10),
                    xaxis_title=None, yaxis_title=None,
                    legend=dict(orientation="v", x=1.02, xanchor="left", y=1, yanchor="top"),
                )
                fig2.update_traces(hovertemplate="%{x|%Y-%m-%d}<br>%{fullData.name}: %{y:,}<extra></extra>")
                st.plotly_chart(fig2, use_container_width=True, key=f"stack::{title}::{c}")

                pv = ui.build_pivot_table(agg, index_col=c, col_col="기간", val_col="value")

            else:
                if (c in AW_COLS) and ("weight" in src.columns):
                    agg = (
                        src.groupby([x, c], dropna=False)["weight"]
                           .sum()
                           .reset_index(name="value")
                    )
                else:
                    agg = (
                        src.groupby([x, c], dropna=False)
                           .size()
                           .reset_index(name="value")
                    )

                agg[x] = agg[x].astype(str)
                agg[c] = _clean_cat(agg[c])

                fig2 = px.bar(
                    agg, x=x, y="value", color=c,
                    barmode="stack", opacity=0.6,
                    color_discrete_map=color_map if color_map is not None else None,
                )
                fig2.update_layout(
                    height=360, margin=dict(l=10, r=10, t=10, b=10),
                    xaxis_title=None, yaxis_title=None,
                    legend=dict(orientation="h", y=1.02, x=1, xanchor="right"),
                )
                fig2.update_traces(hovertemplate="%{x}<br>%{fullData.name}: %{y:,}<extra></extra>")
                st.plotly_chart(fig2, use_container_width=True, key=f"stack::{title}::{x}::{c}")

                pv = ui.build_pivot_table(agg, index_col=c, col_col=x, val_col="value")
        else:
            st.info("Stack 차원 컬럼이 없습니다.")

    if pv is not None:
        st.dataframe(pv, use_container_width=True, hide_index=True, row_height=30)
    else:
        st.info("표를 만들 수 있는 데이터가 없습니다.")

def render_shrm_trend(
    base_df: pd.DataFrame,
    filt: pd.Series | None,
    dim: str,               # "shrm_type" or "shrm_name" or "_shop_type"
    chart: str,             # "line" | "stack"
    chart_key: str,
    empty_msg: str | None = None,
):
    b = base_df if filt is None else base_df[filt]
    if b.empty:
        st.info(empty_msg or "표시할 데이터가 없습니다.")
        return

    # long
    g = (
        b.groupby(["_period_dt", dim], dropna=False)
         .size()
         .reset_index(name="value")
    )
    g["_period_dt"] = pd.to_datetime(g["_period_dt"], errors="coerce")
    g = g.dropna(subset=["_period_dt"])
    g[dim] = _clean_cat(g[dim])
    g = g.sort_values(["_period_dt", dim]).reset_index(drop=True)

    # ✅ 차원 정렬(기타 맨뒤 + 합계 큰 순)
    sums = g.groupby(dim, dropna=False)["value"].sum().to_dict()
    y = _order_with_etc_last(list(sums.keys()), sums)

    # ───────────────── 그래프 ─────────────────
    if chart == "line":
        # ✅ 그래프용 wide: datetime 유지 (build_pivot_table 쓰지 말 것)
        pv_line = (
            g.pivot_table(
                index="_period_dt",
                columns=dim,
                values="value",
                aggfunc="sum",
                fill_value=0
            )
            .reset_index()
        )
        pv_line = pv_line.sort_values("_period_dt").reset_index(drop=True)

        y_cols = [c for c in y if c in pv_line.columns]
        ui.render_line_graph(pv_line, x="_period_dt", y=y_cols, height=360, key=chart_key)

    else:
        # stack은 long 그대로
        g[dim] = pd.Categorical(g[dim].astype(str), categories=y, ordered=True)
        g = g.sort_values(["_period_dt", dim]).reset_index(drop=True)
        ui.render_stack_graph(
            g, x="_period_dt", y="value", color=dim,
            height=360, show_value_in_hover=True, key=chart_key
        )

    # ───────────────── 표(날짜가 컬럼) ─────────────────
    pv_tbl = ui.build_pivot_table(g, index_col=dim, col_col="_period_dt", val_col="value")
    st.dataframe(pv_tbl, use_container_width=True, hide_index=True, row_height=30)


def write_mutable_insight(
    agg: pd.DataFrame,
    row_col: str,
    col_col: str,
    row_label: str,
    col_label: str,
    row_order: list[str],
    col_order: list[str],
    min_row_total: int = 5,     # 너무 작은 행은 인사이트에서 제외
    strong_pct: float = 50.0,   # 한 항목이 50% 이상이면 "두드러짐"
    gap_pct: float = 20.0,      # 1위-2위 격차
    topk: int = 3,
):
    if agg is None or agg.empty:
        return ["선택한 조건에 해당하는 데이터가 없습니다."]

    d = agg[[row_col, col_col, "value"]].copy()
    d[row_col] = d[row_col].astype(str)
    d[col_col] = d[col_col].astype(str)
    d["value"] = pd.to_numeric(d["value"], errors="coerce").fillna(0)

    # ✅ 전체 분포(가중치: value 합)
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

    # ✅ 행별 상위 구성(행 내부 100% 기준, 단 row_total이 너무 작으면 제외)
    row_tot = d.groupby(row_col, dropna=False)["value"].sum()
    # pct_row 재계산(여기서 다시 계산하면 항상 일관)
    d["_row_sum"] = d.groupby(row_col, dropna=False)["value"].transform("sum").replace(0, np.nan)
    d["pct_row"] = (d["value"] / d["_row_sum"] * 100).fillna(0)
    d = d.drop(columns=["_row_sum"])

    # row_order 순서대로
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

    # ✅ 한 줄 요약(구조)
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
    px.defaults.color_discrete_sequence = px.colors.qualitative.Pastel2 # 추가

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
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        try:
            creds = Credentials.from_service_account_file(
                "C:/_code/auth/sleeper-461005-c74c5cd91818.json",
                scopes=scope
            )
        except:
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str):
                import json
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1g2HWpm3Le3t3P3Hb9nm2owoiaxywaXv--L0SHEDx3rQ/edit")
        df = pd.DataFrame(sh.worksheet("shrm_data").get_all_records())

        # (정규화) event_date
        df["event_date"] = df["event_date"].astype("string").str.strip()
        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y. %m. %d", errors="coerce")

        # (파생컬럼) shrm_type
        if "shrm_name" in df.columns:
            df["shrm_type"] = (
                df["shrm_name"]
                .astype("string")
                .fillna("")
                .astype(str)
                .str.split("_", n=1, expand=True)[0]
                .str.strip()
                .replace("", "기타")
            )

        # (범주화)
        cat_cols = ["shrm_name", "shrm_type", "demo_gender", "demo_age", "awareness_type", "purchase_purpose", "visit_type"]
        for c in cat_cols:
            if c in df.columns:
                df[c] = df[c].astype("category")

        # 기간 필터
        df = df[(df["event_date"] >= pd.to_datetime(cs)) & (df["event_date"] < pd.to_datetime(ce))]
        
        return df

    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df = load_data(cs, ce)

    # ✅ awareness_type: 콤마 멀티값 분해 + weight + (괄호)/(괄호제외) 분리
    df_aw = None
    if df is not None and not df.empty and "awareness_type" in df.columns:
        _rid = np.arange(len(df))
        s = df["awareness_type"].astype("string").fillna("").astype(str)

        lst = s.apply(lambda x: [t.strip() for t in str(x).split(",") if t.strip()] or ["기타"])
        n = lst.apply(len).astype(float).replace(0, 1.0)

        df_aw = df.assign(_rid=_rid, awareness_type_list=lst, _n=n)
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

    # ──────────────────────────────────
    # D) Header
    # ──────────────────────────────────
    st.subheader("쇼룸 대시보드 (제작중-배포해가면서 확인중입니다.)")

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
            unsafe_allow_html=True
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
            unsafe_allow_html=True
        )

    st.divider()

    # ──────────────────────────────────
    # 1) 조회 / 예약 / 방문 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>조회 / 예약 / 방문 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일단 방문만 ")

    base = df
    if "shrm_type" in base.columns:
        base["shrm_type"] = _clean_cat(base["shrm_type"])
    else:
        base["shrm_type"] = "기타"

    if "shrm_name" in base.columns:
        base["shrm_name"] = _clean_cat(base["shrm_name"])
    else:
        base["shrm_name"] = "기타"

    base = ui.add_period_columns(base, "event_date", "일별")

    if base.empty:
        st.info("표시할 데이터가 없습니다.")
    else:
        is_load = base["shrm_type"].astype(str).str.contains("로드", na=False)
        is_dept = base["shrm_type"].astype(str).str.contains("백화점", na=False)
        base["_shop_type"] = np.select([is_load, is_dept], ["로드샵", "백화점"], default="기타")

        t1, t_load, t_dept = st.tabs(["쇼룸형태", "로드샵", "백화점"])

        with t1:
            render_shrm_trend(
                base_df=base,
                filt=None,
                dim="_shop_type",
                chart="line",
                chart_key="trend_shrm_type",
            )

        with t_load:
            render_shrm_trend(
                base_df=base,
                filt=(base["_shop_type"] == "로드샵"),
                dim="shrm_name",
                chart="stack",
                chart_key="trend_load",
                empty_msg="로드샵 데이터가 없습니다.",
            )

        with t_dept:
            render_shrm_trend(
                base_df=base,
                filt=(base["_shop_type"] == "백화점"),
                dim="shrm_name",
                chart="stack",
                chart_key="trend_dept",
                empty_msg="백화점 데이터가 없습니다.",
            )

    # ──────────────────────────────────
    # 2) ??
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>제목 </h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ필터 추가해서 상세히 볼수있도록 ")
    
    DIM_MAP = {
        "방문유형": {  # visit_type
            "pie": "visit_type",
            "stack_x": "event_date",
            "stack_color": "visit_type",
            "raw_cols": ["event_date", "visit_type"]
        },
        "데모그래픽": {  # demo_gender, demo_age
            "pie": "demo_gender",
            "stack_x": "demo_age",
            "stack_color": "demo_gender",
            "raw_cols": ["event_date", "demo_gender", "demo_age"]
        },
        "인지단계": {  # awareness_type_a 
            "pie": "awareness_type_a",
            "stack_x": "event_date",
            "stack_color": "awareness_type_a",
            "raw_cols": ["event_date", "awareness_type_a"]
        },
        "인지채널": {  # awareness_type_b 
            "pie": "awareness_type_b",
            "stack_x": "event_date",
            "stack_color": "awareness_type_b",
            "raw_cols": ["event_date", "awareness_type_b"]
        },
        "구매목적": {  # purchase_purpose
            "pie": "purchase_purpose",
            "stack_x": "event_date",
            "stack_color": "purchase_purpose",
            "raw_cols": ["event_date", "purchase_purpose"]
        },
    }

    tabs = st.tabs(list(DIM_MAP.keys()))
    for tab, name in zip(tabs, DIM_MAP.keys()):
        with tab:
            render_shrm_tabs(df, df_aw, name, DIM_MAP[name])

    # ──────────────────────────────────
    # 3) CROSS INSIGHT
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>CROSS INSIGHT </h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명 ")

    DIM_OPTS = {
        "쇼룸형태" : "shrm_type",
        "쇼룸구분" : "shrm_name",
        "방문유형" : "visit_type",
        "성별"    : "demo_gender",
        "연령대"   : "demo_age",
        "인지단계" : "awareness_type_a",
        "인지채널" : "awareness_type_b",
        "구매목적" : "purchase_purpose",
    }

    with st.expander("Filter", expanded=True):
        
        c1, c2 = st.columns(2)
        with c1:
            row_label = st.selectbox(
                "분석 기준 (*선택한 항목으로 데이터를 나눕니다.)",
                options=list(DIM_OPTS.keys()),
                index=4,
                key="cross_row"
            )
        with c2:
            col_label = st.selectbox(
                "구성 기준 (*선택한 항목의 구성 비중을 표시합니다.)",
                options=[k for k in DIM_OPTS.keys() if k != row_label],
                index=6,
                key="cross_col"
            )

    row_col = DIM_OPTS[row_label]
    col_col = DIM_OPTS[col_label]

    AW_COLS = {"awareness_type", "awareness_type_a", "awareness_type_b"}

    # ✅ 컬럼 존재 체크(둘 중 하나라도 없으면 종료)
    has_row = (row_col in df.columns) or (df_aw is not None and row_col in df_aw.columns)
    has_col = (col_col in df.columns) or (df_aw is not None and col_col in df_aw.columns)
    
    if not (has_row and has_col):
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
    else:
        use_aw = (row_col in AW_COLS) or (col_col in AW_COLS)

        if use_aw:
            if df_aw is None or df_aw.empty:
                st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
            else:
                agg = (
                    df_aw.groupby([row_col, col_col], dropna=False)["weight"]
                        .sum()
                        .reset_index(name="value")
                )
        else:
            agg = (
                df.groupby([row_col, col_col], dropna=False)
                    .size()
                    .reset_index(name="value")
            )

        if "agg" in locals() and agg is not None and not agg.empty:
            agg[row_col] = _clean_cat(agg[row_col])
            agg[col_col] = _clean_cat(agg[col_col])

            # ✅ 행 기준 정렬 규칙
            row_sum = (
                agg.groupby(row_col, dropna=False)["value"]
                .sum()
                .sort_values(ascending=False)
            )
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

            # ✅ 열 기준 정렬 규칙 (범례/표 공통)
            # - 기본: 열 합(value) 큰 순
            # - 기타는 항상 마지막
            col_sum = (
                agg.groupby(col_col, dropna=False)["value"]
                .sum()
                .sort_values(ascending=False)
            )
            col_order = col_sum.index.astype(str).tolist()
            etc_in_col = [k for k in ["기타"] if k in col_order]
            col_order = [x for x in col_order if x not in etc_in_col] + etc_in_col

            # ✅ 행 기준 퍼센트
            agg["_row_sum"] = agg.groupby(row_col, dropna=False)["value"].transform("sum").replace(0, np.nan)
            agg["pct_row"] = (agg["value"] / agg["_row_sum"] * 100).fillna(0)
            agg = agg.drop(columns=["_row_sum"])

            # 피벗 2종
            pv_cnt = ui.build_pivot_table(agg, index_col=row_col, col_col=col_col, val_col="value")
            pv_pct = ui.build_pivot_table(agg, index_col=row_col, col_col=col_col, val_col="pct_row")

            # ✅ 피벗 행 순서 강제
            pv_cnt = pv_cnt.set_index(row_col).reindex(row_order).reset_index()
            pv_pct = pv_pct.set_index(row_col).reindex(row_order).reset_index()

            # ✅ 피벗 열(=col_col) 순서 강제: col_order 기준으로 재배열
            cnt_cols = [c for c in pv_cnt.columns if c != row_col]
            cnt_cols = [c for c in col_order if c in cnt_cols]
            pv_cnt = pv_cnt[[row_col] + cnt_cols]

            pct_cols = [c for c in pv_pct.columns if c != row_col]
            pct_cols = [c for c in col_order if c in pct_cols]
            pv_pct = pv_pct[[row_col] + pct_cols]

            # 누적 막대(행 기준 100%)
            bar = agg[[row_col, col_col, "pct_row"]].rename(columns={"pct_row": "pct"})
            bar[row_col] = pd.Categorical(bar[row_col].astype(str), categories=row_order, ordered=True)
            bar[col_col] = pd.Categorical(bar[col_col].astype(str), categories=col_order, ordered=True)
            bar = bar.sort_values([row_col, col_col]).reset_index(drop=True)

            fig = px.bar(
                bar,
                y=row_col,
                x="pct",
                color=col_col,
                orientation="h",
                barmode="stack",
                text=bar["pct"].round(0).astype(int).astype(str) + "%",
            )

            # ✅ 표(row_order)와 그래프 순서 동일하게 고정
            fig.update_yaxes(categoryorder="array", categoryarray=row_order, autorange="reversed")

            n_rows = bar[row_col].nunique()
            fig_height = 150 + (n_rows * 30)

            fig.update_layout(
                height=fig_height,
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
                ),
            )
            fig.update_traces(
                hovertemplate="%{y}<br>%{fullData.name}: %{x:.1f}%<extra></extra>",
                textposition="inside"
            )
            st.plotly_chart(fig, use_container_width=True)

            # ── 화면용 합친 표 (row_order + col_order 고정)
            pv_show = pv_cnt.copy()
            for cc in [c for c in pv_show.columns if c != row_col]:
                if cc in pv_pct.columns:
                    pv_show[cc] = (
                        pv_cnt[cc].fillna(0).astype(int).astype(str)
                        + " ("
                        + pv_pct[cc].fillna(0).round(0).astype(int).astype(str)
                        + "%)"
                    )

            st.dataframe(
                pv_show,
                use_container_width=True,
                hide_index=True,
                row_height=30
            )
            
                    
            insight_lines = write_mutable_insight(agg=agg, row_col=row_col, col_col=col_col, row_label=row_label, col_label=col_label, row_order=row_order, col_order=col_order)
            st.write("시범기능입니다..")
            st.success("\n".join(insight_lines), icon="✅")

            
        else:
            st.warning("선택한 조건에 해당하는 데이터가 없습니다.")


if __name__ == "__main__":
    main()
