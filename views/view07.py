# 2026-01-13 ver.
# UPDATE:
# - 모든 표를 그래프 아래로 배치
# - 전체 view_item 추이 표: events_per_user / events_per_session 소수 2자리 (행 단위 포맷 적용)
# - 합계/평균 행 제거 유지
# - hover: 범례명 + 비중(%) 유지
# - 각 영역 필터 expander(항상 열림) 유지

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import importlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery

import modules.style
importlib.reload(sys.modules["modules.style"])
from modules.style import style_format


# ──────────────────────────────────
# 공통 유틸
# ──────────────────────────────────

# 날짜 컬럼을 기준으로 일별/주별 “기간 라벨”(_period) 을 만듦
def with_period(df: pd.DataFrame, date_col: str, mode: str) -> pd.DataFrame:
    w = df.copy()
    w[date_col] = pd.to_datetime(w[date_col], errors="coerce")
    w = w.dropna(subset=[date_col])

    if mode == "일별":
        dt = w[date_col].dt.floor("D")
        w["_period_dt"] = dt
        w["_period"] = dt.dt.strftime("%Y-%m-%d")
    else:
        ws = w[date_col].dt.floor("D") - pd.to_timedelta(w[date_col].dt.weekday, unit="D")
        we = ws + pd.to_timedelta(6, unit="D")
        w["_period_dt"] = ws
        w["_period"] = ws.dt.strftime("%Y-%m-%d") + " ~ " + we.dt.strftime("%Y-%m-%d")

    return w

# 표에서 날짜가 가로컬럼일 때 컬럼을 시간순으로 정렬
def sort_period_cols(cols: list[str]) -> list[str]:
    return sorted(cols, key=lambda x: x.split(" ~ ")[0] if " ~ " in x else x)

# 특정 차원(카테고리/제품명 등)에서 등장 빈도 Top K 값 리스트 생성
def topk_values(s: pd.Series, k: int) -> list[str]:
    vc = s.replace("", np.nan).dropna().value_counts()
    return vc.head(k).index.tolist()

# 라인/바 차트에 주말 영역 음영(vrect) 넣기.
def add_weekend_vrect_centered(fig, x_vals: pd.Series):
    xs = pd.to_datetime(pd.Series(x_vals), errors="coerce").dropna().dt.floor("D").unique()
    for d in xs:
        d = pd.Timestamp(d).date()
        start = datetime.combine(d, datetime.min.time()) + timedelta(hours=12)
        end = start + timedelta(hours=24)

        if d.weekday() == 4:   # 토요일 영역(원본 방식)
            fig.add_vrect(x0=start, x1=end, fillcolor="blue", opacity=0.05, layer="below", line_width=0)
        elif d.weekday() == 5: # 일요일 영역(원본 방식)
            fig.add_vrect(x0=start, x1=end, fillcolor="red", opacity=0.05, layer="below", line_width=0)

# 공통 라인차트 렌더링 (마커 포함 + 주말 음영 + 레이아웃 통일)
def render_line_chart(df: pd.DataFrame, x: str, y: list[str] | str, height: int = 360, title: str | None = None) -> None:
    y_cols = [y] if isinstance(y, str) else y
    fig = px.line(df, x=x, y=y_cols, markers=True, labels={"variable": ""}, title=title)
    add_weekend_vrect_centered(fig, df[x])
    fig.update_layout(
        height=height,
        xaxis_title=None,
        yaxis_title=None,
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom")
    )
    fig.update_xaxes(tickformat="%m월 %d일")
    st.plotly_chart(fig, use_container_width=True)


# 누적막대(스택) 그래프 + hover에 “항목명 + 비중(%)” 표시.
def render_stack_bar_share_hover(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str,
    height: int = 360,
    opacity: float = 0.6,
    title: str | None = None,
    show_value_in_hover: bool = False,  # 필요하면 True로 바꾸면 수치도 같이 표시
) -> None:
    d = df.copy()
    d[y] = pd.to_numeric(d[y], errors="coerce").fillna(0)

    tot = d.groupby(x, dropna=False)[y].transform("sum").replace(0, np.nan)
    d["_share_pct"] = (d[y] / tot * 100).round(1).fillna(0)

    fig = px.bar(
        d,
        x=x,
        y=y,
        color=color,
        barmode="relative",
        labels={x: "", y: "", color: ""},
        title=title,
        custom_data=[color, "_share_pct", y]
    )
    fig.update_traces(marker_opacity=opacity)

    if show_value_in_hover:
        fig.update_traces(
            hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]}%<br>값: %{customdata[2]:,.0f}<extra></extra>"
        )
    else:
        fig.update_traces(
            hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]}%<extra></extra>"
        )

    add_weekend_vrect_centered(fig, d[x])
    fig.update_layout(
        height=height,
        legend_title_text="",
        xaxis_title=None,
        yaxis_title=None,
        bargap=0.5,
        bargroupgap=0.2,
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
        margin=dict(l=10, r=10, t=40, b=10)
    )
    fig.update_layout(barmode="relative")
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_xaxes(tickformat="%m월 %d일")
    st.plotly_chart(fig, use_container_width=True)


# 전체 view_item을 유저/세션/이벤트 + epu/eps로 요약한 “일별/주별” 테이블 생성.
def pivot_view_item_summary(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    base = with_period(df, "event_date", mode)

    users = (
        base.groupby("_period", dropna=False)["user_pseudo_id"]
        .nunique()
        .reset_index(name="view_item_users")
        .rename(columns={"_period": "날짜"})
    )
    sessions = (
        base.groupby("_period", dropna=False)["pseudo_session_id"]
        .nunique()
        .reset_index(name="view_item_sessions")
        .rename(columns={"_period": "날짜"})
    )
    events = (
        base.groupby("_period", dropna=False)
        .size()
        .reset_index(name="view_item_events")
        .rename(columns={"_period": "날짜"})
    )

    out = users.merge(sessions, on="날짜", how="outer").merge(events, on="날짜", how="outer")
    out = out.sort_values("날짜").reset_index(drop=True)

    out["events_per_session"] = (out["view_item_events"] / out["view_item_sessions"]).replace([np.inf, -np.inf], np.nan)
    out["events_per_user"] = (out["view_item_events"] / out["view_item_users"]).replace([np.inf, -np.inf], np.nan)
    return out

# pivot_view_item_summary 결과를 “지표가 세로행, 날짜가 가로컬럼” 형태로 바꾸고
# 정수는 콤마, 비율은 소수 2자리로 행 단위 포맷을 강제로 고정.
def pivot_metrics_as_rows_formatted(df_sum: pd.DataFrame) -> pd.DataFrame:
    rows = [
        ("유저수", "view_item_users", "int"),
        ("세션수", "view_item_sessions", "int"),
        ("이벤트수", "view_item_events", "int"),
        ("events_per_user", "events_per_user", "float2"),
        ("events_per_session", "events_per_session", "float2"),
    ]

    dates = df_sum["날짜"].astype(str).tolist()
    out = pd.DataFrame({"지표": [r[0] for r in rows]})

    for dt in dates:
        out[dt] = ""

    # 날짜별 값 매핑
    m = df_sum.set_index("날짜").to_dict(orient="index")

    def _fmt(v, kind: str) -> str:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        if kind == "int":
            try:
                return f"{int(round(float(v))):,}"
            except Exception:
                return ""
        else:  # float2
            try:
                return f"{float(v):.2f}"
            except Exception:
                return ""

    for i, (label, col, kind) in enumerate(rows):
        for dt in dates:
            val = m.get(dt, {}).get(col, np.nan)
            out.at[i, dt] = _fmt(val, kind)

    # 날짜 컬럼 정렬
    out = out[["지표", *sort_period_cols(dates)]]
    return out


# ──────────────────────────────────
# 정렬/표 렌더 공통화
# ──────────────────────────────────

# 매트리스, 프레임, 부자재 순(하이라키) + 그 안에서 알파(문자) 정렬
_HIER_PRI = ["매트리스", "프레임", "부자재"]

def _hier_rank(text: str) -> int:
    t = (text or "").strip()
    for i, kw in enumerate(_HIER_PRI):
        if kw in t:
            return i
    return 99

def sort_b_opts_hier(tb: pd.DataFrame) -> list[str]:
    b = (
        tb["product_cat_b"]
        .dropna().astype(str).str.strip()
        .replace("nan", "")
    )
    b = [x for x in b.unique().tolist() if x != ""]
    return sorted(b, key=lambda x: (_hier_rank(x), x))

def sort_c_opts_hier(tb: pd.DataFrame) -> list[str]:
    t = tb.copy()
    t["product_cat_b"] = t["product_cat_b"].astype(str).str.strip()
    t["product_cat_c"] = t["product_cat_c"].astype(str).str.strip()
    t = t[(t["product_cat_c"] != "") & (t["product_cat_c"].str.lower() != "nan")].copy()

    if t.empty:
        return []

    # 소분류가 어떤 중분류에 속하는지 기반으로: (중분류 우선순위, 중분류명, 소분류명)
    tmp = (
        t.groupby(["product_cat_c"], dropna=False)["product_cat_b"]
        .apply(lambda s: sorted(list(dict.fromkeys([x for x in s.tolist() if x and x.lower() != "nan"]))))
        .reset_index(name="_parents")
    )

    def _key(row):
        c = row["product_cat_c"]
        parents = row["_parents"] or []
        # 여러 부모가 있으면 "가장 우선순위 높은" 부모를 기준으로 정렬키 결정
        if parents:
            p0 = sorted(parents, key=lambda x: (_hier_rank(x), x))[0]
            return (_hier_rank(p0), p0, c)
        return (99, "", c)

    tmp["_k"] = tmp.apply(_key, axis=1)
    tmp = tmp.sort_values("_k").reset_index(drop=True)
    return tmp["product_cat_c"].tolist()

# long -> pivot wide 표 만들고, 항상 "기간 전체 합계" 기준 내림차순 정렬 (1영역 제외)
def build_pivot_table(
    long: pd.DataFrame,
    index_col: str,
    col_col: str,
    val_col: str
) -> pd.DataFrame:
    pv = (
        long.pivot_table(index=index_col, columns=col_col, values=val_col, fill_value=0, aggfunc="sum")
        .reset_index()
    )
    date_cols = [c for c in pv.columns if c != index_col]
    pv = pv[[index_col, *sort_period_cols(date_cols)]]

    if date_cols:
        num_cols = [c for c in pv.columns if c != index_col]
        pv["_sum"] = pv[num_cols].sum(axis=1)
        pv = pv.sort_values("_sum", ascending=False).drop(columns=["_sum"])

    return pv

def render_table(df_wide: pd.DataFrame, index_col: str, decimals: int = 0):
    styled = style_format(df_wide, decimals_map={c: decimals for c in df_wide.columns if c != index_col})
    st.dataframe(styled, row_height=30, hide_index=True)


# 차원별로 찢는 추이 영역 : 브랜드별로 따로 wide pivot(날짜 × 구분컬럼) 만들기.
def build_cat_pivots(
    base: pd.DataFrame,
    mode: str,
    view_level: str,             # "브랜드" | "중분류" | "소분류" | "제품"
    sel_a: list[str],
    sel_ab_labels: list[str] | None = None,   # ["슬립퍼 · 프레임", ...]
    sel_c: list[str] | None = None,
    sel_products: list[str] | None = None,
    top_k: int = 10
) -> dict[str, pd.DataFrame]:

    tmp = with_period(base, "event_date", mode)
    tmp = tmp[tmp["product_cat_a"].isin(sel_a)].copy()

    if sel_ab_labels is not None:
        tmp["_ab"] = tmp["product_cat_a"].astype(str) + " · " + tmp["product_cat_b"].astype(str)
        tmp = tmp[tmp["_ab"].isin(sel_ab_labels)].copy()

    if sel_c is not None:
        tmp = tmp[tmp["product_cat_c"].isin(sel_c)].copy()

    if view_level == "브랜드":
        dim = "product_cat_a"
    elif view_level == "중분류":
        dim = "product_cat_b"
    elif view_level == "소분류":
        dim = "product_cat_c"
    else:
        dim = "product_name"

    out: dict[str, pd.DataFrame] = {}

    for brand in sel_a:
        tb = tmp[tmp["product_cat_a"] == brand].copy()
        if tb.empty:
            continue

        if view_level == "제품" and sel_products:
            tb = tb[tb["product_name"].isin(sel_products)].copy()

        if view_level in ["중분류", "소분류", "제품"]:
            if not (view_level == "제품" and sel_products):
                top_vals = topk_values(tb[dim], top_k)
                tb[dim] = tb[dim].where(tb[dim].isin(top_vals), "기타")

        agg = (
            tb.groupby(["_period", dim], dropna=False)["pseudo_session_id"]
            .nunique()
            .reset_index(name="sessions")
            .rename(columns={"_period": "날짜"})
        )

        wide = (
            agg.pivot_table(index="날짜", columns=dim, values="sessions", fill_value=0, aggfunc="sum")
            .reset_index()
            .sort_values("날짜")
            .reset_index(drop=True)
        )
        out[brand] = wide

    return out


# build_cat_pivots 결과를 받아서, 브랜드별로
# 스택막대 그래프 먼저, 그 아래에 날짜 가로표 출력
def render_brand_split_stack_and_table(pivots: dict[str, pd.DataFrame], title_prefix: str = ""):
    if not pivots:
        st.info("데이터가 없습니다.")
        return

    for brand, wide in pivots.items():
        st.markdown(f"###### {title_prefix}{brand}")

        y_cols = [c for c in wide.columns if c != "날짜"]
        long = wide.melt(id_vars=["날짜"], value_vars=y_cols, var_name="구분", value_name="sessions")

        # ✅ 그래프 먼저
        render_stack_bar_share_hover(long, x="날짜", y="sessions", color="구분", height=340, opacity=0.6)

        # ✅ 표는 아래 (날짜 가로) + 고정 내림차순
        pv = build_pivot_table(long, index_col="구분", col_col="날짜", val_col="sessions")
        render_table(pv, index_col="구분", decimals=0)

        st.markdown(" ")


# ──────────────────────────────────
# ✅ NEW: 브랜드 고정형(슬립퍼/누어) pills UI/필터 공통화
# ──────────────────────────────────

def _brands_exist(base: pd.DataFrame, brand_order: list[str]) -> list[str]:
    exist = base["product_cat_a"].dropna().astype(str).unique().tolist()
    return [b for b in brand_order if b in exist]

def _render_brand_pills_ui(
    base: pd.DataFrame,
    brand_order: list[str],
    brands_exist: list[str],
    need_ab: bool,
    need_c: bool,
    key_prefix: str
) -> tuple[dict[str, list[str] | None], dict[str, list[str] | None]]:
    """
    브랜드별 중분류/소분류 pills UI를 찍고,
    선택값 dict를 반환.
    """
    sel_b_by_brand: dict[str, list[str] | None] = {}
    sel_c_by_brand: dict[str, list[str] | None] = {}

    for b in brand_order:
        if b not in brands_exist:
            continue

        tb = base[base["product_cat_a"] == b].copy()
        if tb.empty:
            continue

        c0, c1, c2 = st.columns([1, 2, 8], vertical_alignment="center")
        with c0:
            st.markdown(
                f"<div style='font-size:13px;font-weight:700;line-height:1;white-space:nowrap;'>{b}</div>",
                unsafe_allow_html=True
            )

        # 중분류 pills
        if need_ab:
            b_opts = sort_b_opts_hier(tb)
            with c1:
                sel_b_by_brand[b] = st.pills(
                    " ", b_opts,
                    selection_mode="multi",
                    default=b_opts,
                    key=f"{key_prefix}__ab__{b}"
                ) or []
        else:
            sel_b_by_brand[b] = None
            with c1:
                st.markdown(" ")

        # 소분류 pills
        if need_c:
            tb2 = tb.copy()
            picked_b = (sel_b_by_brand.get(b) or [])
            tb2 = tb2[tb2["product_cat_b"].isin(picked_b)].copy() if len(picked_b) > 0 else tb2.iloc[0:0].copy()

            c_opts = sort_c_opts_hier(tb2)
            with c2:
                sel_c_by_brand[b] = st.pills(
                    " ", c_opts,
                    selection_mode="multi",
                    default=c_opts,
                    key=f"{key_prefix}__c__{b}"
                ) or []
        else:
            sel_c_by_brand[b] = None
            with c2:
                st.markdown(" ")

    return sel_b_by_brand, sel_c_by_brand

def _apply_brand_filters(
    df_brand: pd.DataFrame,
    brand: str,
    view_level: str,
    need_ab: bool,
    need_c: bool,
    sel_b_by_brand: dict[str, list[str] | None],
    sel_c_by_brand: dict[str, list[str] | None],
    sel_products: list[str] | None
) -> pd.DataFrame:
    """
    브랜드 DF에 pills/제품선택 필터 적용 (브랜드 기준으로만)
    """
    out = df_brand

    if view_level in ["중분류", "소분류", "제품"] and need_ab:
        picked_b = sel_b_by_brand.get(brand)
        if picked_b is not None:
            if len(picked_b) == 0:
                return out.iloc[0:0].copy()
            out = out[out["product_cat_b"].isin(picked_b)]

    if view_level in ["소분류", "제품"] and need_c:
        picked_c = sel_c_by_brand.get(brand)
        if picked_c is not None:
            if len(picked_c) == 0:
                return out.iloc[0:0].copy()
            out = out[out["product_cat_c"].isin(picked_c)]

    # 제품 선택이 있으면: 그 제품만(브랜드에 없으면 empty 정상)
    if view_level == "제품" and sel_products:
        out = out[out["product_name"].isin(sel_products)]

    return out

def _build_product_candidates_for_multiselect(
    base: pd.DataFrame,
    brand_order: list[str],
    brands_exist: list[str],
    view_level: str,
    need_ab: bool,
    need_c: bool,
    sel_b_by_brand: dict[str, list[str] | None],
    sel_c_by_brand: dict[str, list[str] | None],
) -> list[str]:
    """
    '제품' 뎁스일 때, pills 조건(브랜드별)을 모두 반영한 후보 풀에서 제품 후보 리스트 생성
    """
    if view_level != "제품":
        return []

    tmpP = base[base["product_cat_a"].isin(brands_exist)].copy()
    if tmpP.empty:
        return []

    mask = pd.Series(False, index=tmpP.index)

    for b in brand_order:
        if b not in brands_exist:
            continue

        tb = tmpP[tmpP["product_cat_a"] == b].copy()
        if tb.empty:
            continue

        if need_ab:
            picked_b = (sel_b_by_brand.get(b) or [])
            tb = tb[tb["product_cat_b"].isin(picked_b)].copy() if len(picked_b) > 0 else tb.iloc[0:0].copy()

        if need_c:
            picked_c = (sel_c_by_brand.get(b) or [])
            tb = tb[tb["product_cat_c"].isin(picked_c)].copy() if len(picked_c) > 0 else tb.iloc[0:0].copy()

        mask.loc[tb.index] = True

    tmpP = tmpP[mask].copy()
    if tmpP.empty:
        return []

    return topk_values(tmpP["product_name"], max(50, 200))


# ──────────────────────────────────
# main
# ──────────────────────────────────
def main():
    st.markdown(
        """
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
        unsafe_allow_html=True
    )
    st.markdown("""
        <style>
            [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)

    # ──────────────────────────────────
    # A) 사이드바
    # ──────────────────────────────────
    st.sidebar.header("Filter")
    today = datetime.now().date()
    default_end = today - timedelta(days=1)
    default_start = today - timedelta(days=14)

    start_date, end_date = st.sidebar.date_input("기간 선택", value=[default_start, default_end], key="view07__date_range")
    cs = start_date.strftime("%Y%m%d")
    ce_exclusive = (end_date + timedelta(days=1)).strftime("%Y%m%d")

    # ──────────────────────────────────
    # B) 데이터 호출
    # ──────────────────────────────────
    @st.cache_data(ttl=3600)
    def load_data(cs: str, ce: str) -> tuple[pd.DataFrame, str | datetime]:
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df = bq.get_data("tb_sleeper_product")
        last_updated_time = df["event_date"].max()

        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d", errors="coerce")
        if "event_name" in df.columns:
            df = df[df["event_name"] == "view_item"].copy()

        def _safe_str_col(colname: str) -> pd.Series:
            if colname in df.columns:
                s = df[colname]
            else:
                s = pd.Series([""] * len(df), index=df.index)
            s = s.astype(str).replace("nan", "").fillna("")
            s = s.str.strip()
            return s

        # null -> "(not set)"
        df["_source"] = _safe_str_col("collected_traffic_source__manual_source").replace("", "(not set)")
        df["_medium"] = _safe_str_col("collected_traffic_source__manual_medium").replace("", "(not set)")
        df["_campaign"] = _safe_str_col("collected_traffic_source__manual_campaign_name").replace("", "(not set)")
        df["_content"] = _safe_str_col("collected_traffic_source__manual_content").replace("", "(not set)")
        df["_sourceMedium"] = df["_source"] + " / " + df["_medium"]

        return df, last_updated_time

    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df, last_updated_time = load_data(cs, ce_exclusive)

    # ──────────────────────────────────
    # C) 헤더
    # ──────────────────────────────────
    st.subheader("GA PDP 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px; line-height:1.5;">
            이 대시보드에서는 <b>브랜드·카테고리·제품</b> 단위의
            <b>제품 상세 페이지 조회량(view_item)</b>을 확인할 수 있습니다.<br>
            </div>
            <div style="color:#6c757d; font-size:14px; line-height:2.0;">
            ※ GA D-1 데이터의 세션 수치는 <b>오전에 1차</b> 집계되나 , 세션의 유입출처는 <b>오후에 2차</b> 반영됩니다.
            </div>
            """,
            unsafe_allow_html=True
        )

    with col2:
        if isinstance(last_updated_time, str):
            latest_dt = datetime.strptime(last_updated_time, "%Y%m%d")
        else:
            latest_dt = last_updated_time
        latest_date = latest_dt.date() if hasattr(latest_dt, "date") else datetime.now().date()

        now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
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
            if hm_ref >= 1535:
                msg = "2차 업데이트 완료 (PM 15:35)"
                sub_bg = "#fff7ed"
                sub_bd = "#fdba74"
                sub_fg = "#c2410c"
            elif hm_ref >= 850:
                msg = "1차 업데이트 완료 (AM 08:50)"

        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
            <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 8px;font-size:13px;line-height:1;
                color:{sub_fg};background:{sub_bg};border:1px solid {sub_bd};
                border-radius:10px;white-space:nowrap;">
                🔔 {msg}
            </span>
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
    # 1) 첫 번쨰 영역
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>PDP조회 추이<span style='color:#FF4B4B;'></span></h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ**유저수**는 고유 사람, **세션수**는 방문 단위, **이벤트수**는 방문 안에서 발생한 view_item의 총 횟수 입니다.")

    with st.expander("Filter", expanded=True):
        r0_1, r0_2 = st.columns([1.3, 2.7], vertical_alignment="bottom")
        with r0_1:
            mode_all = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_all")

        with r0_2:
            metric_map = {
                "유저수": "view_item_users",
                "세션수": "view_item_sessions",
                "이벤트수": "view_item_events",
            }
            sel_metrics = st.pills(
                "집계 단위",
                list(metric_map.keys()),
                selection_mode="multi",
                default=list(metric_map.keys()),
                key="sel_metrics_all"
            ) or list(metric_map.keys())

    df_all = pivot_view_item_summary(df, mode_all)
    y_cols = [metric_map[m] for m in sel_metrics if m in metric_map] or ["view_item_users"]

    # ✅ 그래프
    render_line_chart(df_all, x="날짜", y=y_cols, height=360)

    # ✅ 표(아래): 날짜 가로 + EPx 소수2자리
    pv = pivot_metrics_as_rows_formatted(df_all)
    st.dataframe(pv, row_height=30, hide_index=True, use_container_width=True)


    # ──────────────────────────────────
    # 2) 두 번쨰 영역
    # ──────────────────────────────────
    st.header(" ") # 공백 필수
    st.markdown("<h5 style='margin:0'>PDP조회 유입<span style='color:#FF4B4B;'></span></h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤPDP 조회가 발생한 세션을 기준으로, 매체/채널별 증감 변화를 확인합니다.")

    with st.expander("Filter", expanded=True):
        r1, r2, r3 = st.columns([3,3,3], vertical_alignment="bottom")
        with r1:
            mode_path = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_path")
        with r2:
            path_dim = st.selectbox("유입 기준", ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"], index=0, key="path_dim")
        with r3:
            topk_path = st.selectbox("표시 Top K", [7, 10, 15, 20], index=1, key="topk_path")

    tmp = with_period(df, "event_date", mode_path)

    PATH_MAP = {
        "소스 / 매체": tmp["_sourceMedium"],
        "소스": tmp["_source"],
        "매체": tmp["_medium"],
        "캠페인": tmp["_campaign"],
        "컨텐츠": tmp["_content"],
    }
    tmp["_path"] = PATH_MAP[path_dim].replace("", "(not set)")

    top_paths = tmp["_path"].value_counts().head(topk_path).index.tolist()
    tmp["_path2"] = tmp["_path"].where(tmp["_path"].isin(top_paths), "기타")

    agg_path = (
        tmp.groupby(["_period", "_path2"], dropna=False)["pseudo_session_id"]
        .nunique()
        .reset_index(name="sessions")
        .rename(columns={"_period": "날짜", "_path2": "유입경로"})
    )

    # ✅ 그래프
    render_stack_bar_share_hover(agg_path, x="날짜", y="sessions", color="유입경로", height=360, opacity=0.6)

    # ✅ 표(아래): 날짜 가로 + 고정 내림차순
    pv = build_pivot_table(agg_path, index_col="유입경로", col_col="날짜", val_col="sessions")
    render_table(pv, index_col="유입경로", decimals=0)


    # ──────────────────────────────────
    # 3) 세 번째 영역
    # ──────────────────────────────────
    st.header(" ")  # 공백 필수
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>품목별 </span>PDP조회 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤPDP조회가 어떤 상품군에서 발생하고 있는지 보여줍니다.")

    tab1, tab2 = st.tabs(["커스텀", "[고정뷰 예시] 슬립퍼 프레임별"])

    with tab1:
        with st.expander("Filter", expanded=True):
            # 상단 1줄(배치 고정)
            r1, r2, r3 = st.columns([1.4, 2.6, 2.0], vertical_alignment="bottom")
            with r1:
                mode_cat = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_cat_tab1")
            with r2:
                view_level = st.radio("품목 뎁스", ["브랜드", "중분류", "소분류", "제품"], index=2, horizontal=True, key="view_level_tab1")
            with r3:
                topk_cat = st.selectbox("표시 Top K", [5, 7, 10, 15, 20], index=2, key="topk_cat_tab1")

            base2 = df  # ✅ 여기선 굳이 copy() 필요 없음(아래에서 필요한 곳만 copy)
            sel_products = None  # ✅ 무조건 먼저 선언

            # ──────────────────────────────────
            # ✅ 2줄 고정: 슬립퍼 / 누어 (브랜드 UI 없음)
            # ──────────────────────────────────
            brand_order = ["슬립퍼", "누어"]
            brands_exist = _brands_exist(base2, brand_order)

            sel_a = brands_exist[:]   # ✅ 브랜드는 고정

            need_ab = view_level in ["중분류", "소분류", "제품"]
            need_c  = view_level in ["소분류", "제품"]

            sel_ab_labels = None
            sel_c = None

            # ──────────────────────────────────
            # ✅ UI: 브랜드/중분류/소분류 줄 (브랜드 뎁스면 아예 숨김)
            # ──────────────────────────────────
            sel_b_by_brand, sel_c_by_brand = ({}, {})

            if view_level != "브랜드":
                sel_b_by_brand, sel_c_by_brand = _render_brand_pills_ui(
                    base=base2,
                    brand_order=brand_order,
                    brands_exist=brands_exist,
                    need_ab=need_ab,
                    need_c=need_c,
                    key_prefix="cat_tab1"
                )

                if need_ab:
                    _ab_labels = []
                    for b in brands_exist:
                        picked_b = (sel_b_by_brand.get(b) or [])
                        _ab_labels += [f"{b} · {bb}" for bb in picked_b]
                    sel_ab_labels = list(dict.fromkeys(_ab_labels))

                if need_c:
                    _c = []
                    for b in brands_exist:
                        _c += (sel_c_by_brand.get(b) or [])
                    sel_c = list(dict.fromkeys(_c))

                # ✅ 제품 선택 (제품 뎁스일 때만)
                if view_level == "제품":
                    prod_candidates = _build_product_candidates_for_multiselect(
                        base=base2,
                        brand_order=brand_order,
                        brands_exist=brands_exist,
                        view_level=view_level,
                        need_ab=need_ab,
                        need_c=need_c,
                        sel_b_by_brand=sel_b_by_brand,
                        sel_c_by_brand=sel_c_by_brand,
                    )
                    sel_products = st.multiselect(
                        "제품 선택 (선택하지 않으면 전체 TopK로 표시)",
                        options=prod_candidates,
                        default=[],
                        placeholder="전체",
                        key="sel_products_tab1"
                    )

        pivots = build_cat_pivots(
            base=base2,
            mode=mode_cat,
            view_level=view_level,
            sel_a=sel_a,
            sel_ab_labels=sel_ab_labels if view_level in ["중분류", "소분류", "제품"] else None,
            sel_c=sel_c if view_level in ["소분류", "제품"] else None,
            sel_products=sel_products if view_level == "제품" else None,
            top_k=topk_cat
        )

        # ✅ 4번 영역처럼: 브랜드별 empty 안내
        for b in sel_a:
            st.markdown(f"###### {b}")
            wide = pivots.get(b)
            if wide is None or wide.empty:
                st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                st.markdown(" ")
                continue

            y_cols = [c for c in wide.columns if c != "날짜"]
            long = wide.melt(id_vars=["날짜"], value_vars=y_cols, var_name="구분", value_name="sessions")

            render_stack_bar_share_hover(long, x="날짜", y="sessions", color="구분", height=340, opacity=0.6)

            pv = build_pivot_table(long, index_col="구분", col_col="날짜", val_col="sessions")
            render_table(pv, index_col="구분", decimals=0)

            st.markdown(" ")


    # ───────── 탭2: 고정뷰 예시 ─────────
    with tab2:
        with st.expander("필터", expanded=True):
            mode_cat3 = st.radio("집계 단위", ["일별", "주별"], horizontal=True, key="mode_cat_tab3")
            topk_cat3 = st.selectbox("표시 Top K", [5, 7, 10, 15, 20], index=2, key="topk_cat_tab3")

        pivots3 = build_cat_pivots(
            base=df,
            mode=mode_cat3,
            view_level="소분류",
            sel_a=["슬립퍼"],
            sel_ab_labels=["슬립퍼 · 프레임"],
            sel_c=["원목", "패브릭", "호텔침대"],
            sel_products=None,
            top_k=topk_cat3
        )
        render_brand_split_stack_and_table(pivots3)


    # # ──────────────────────────────────
    # # 4) 네 번째 영역
    # # ──────────────────────────────────
    # st.header(" ")  # 공백 필수
    # st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>제품군</span> 유입경로</h5>", unsafe_allow_html=True)
    # st.markdown(":gray-badge[:material/Info: Info]ㅤhover는 <b>비중(%)</b>입니다.", unsafe_allow_html=True)

    # with st.expander("Filter", expanded=True):
    #     r1, r2, r3 = st.columns([1.4, 2.6, 2.0], vertical_alignment="bottom")
    #     with r1:
    #         mode_prod_path = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_prod_path")
    #     with r2:
    #         view_level_pp = st.radio("품목 뎁스", ["브랜드", "중분류", "소분류", "제품"], index=3, horizontal=True, key="view_level_prod_path")
    #     with r3:
    #         topk_path_pp = st.selectbox("유입경로 Top K", [7, 10, 15, 20], index=1, key="topk_path_pp")

    #     r4, r5 = st.columns([2.0, 8.0], vertical_alignment="bottom")
    #     with r4:
    #         path_dim_pp = st.selectbox("유입 기준", ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"], index=0, key="path_dim_prod_path")
    #     with r5:
    #         st.markdown(" ")

    #     base4 = df.copy()

    #     # ✅ 브랜드 고정(슬립퍼/누어)
    #     brand_order = ["슬립퍼", "누어"]
    #     brands_exist = [
    #         b for b in brand_order
    #         if b in base4["product_cat_a"].dropna().astype(str).unique().tolist()
    #     ]
    #     sel_a_pp = brands_exist[:]   # 브랜드는 고정

    #     need_ab = view_level_pp in ["중분류", "소분류", "제품"]
    #     need_c  = view_level_pp in ["소분류", "제품"]

    #     sel_ab_by_brand = {}
    #     sel_c_by_brand  = {}

    #     sel_b_pp = None
    #     sel_c_pp = None
    #     sel_products_pp = None

    #     # ✅ pills UI (3영역 동일)
    #     if view_level_pp != "브랜드":
    #         for b in brand_order:
    #             if b not in brands_exist:
    #                 continue

    #             tb = base4[base4["product_cat_a"] == b].copy()
    #             if tb.empty:
    #                 continue

    #             c0, c1, c2 = st.columns([1, 2, 8], vertical_alignment="center")

    #             with c0:
    #                 st.markdown(
    #                     f"<div style='font-size:13px;font-weight:700;line-height:1;white-space:nowrap;'>{b}</div>",
    #                     unsafe_allow_html=True
    #                 )

    #             if need_ab:
    #                 b_opts = sort_b_opts_hier(tb)
    #                 with c1:
    #                     sel_ab_by_brand[b] = st.pills(
    #                         " ", b_opts,
    #                         selection_mode="multi",
    #                         default=b_opts,
    #                         key=f"ab__prodpath__{b}"
    #                     ) or []
    #             else:
    #                 sel_ab_by_brand[b] = None
    #                 with c1:
    #                     st.markdown(" ")

    #             if need_c:
    #                 tb2 = tb.copy()
    #                 if sel_ab_by_brand.get(b):
    #                     tb2 = tb2[tb2["product_cat_b"].isin(sel_ab_by_brand[b])].copy()

    #                 c_opts = sort_c_opts_hier(tb2)
    #                 with c2:
    #                     sel_c_by_brand[b] = st.pills(
    #                         " ", c_opts,
    #                         selection_mode="multi",
    #                         default=c_opts,
    #                         key=f"c__prodpath__{b}"
    #                     ) or []
    #             else:
    #                 sel_c_by_brand[b] = None
    #                 with c2:
    #                     st.markdown(" ")

    #         # ✅ 합치기 (중분류/소분류)
    #         if need_ab:
    #             _b = []
    #             for b in brands_exist:
    #                 _b += (sel_ab_by_brand.get(b) or [])
    #             sel_b_pp = list(dict.fromkeys(_b))

    #         if need_c:
    #             _c = []
    #             for b in brands_exist:
    #                 _c += (sel_c_by_brand.get(b) or [])
    #             sel_c_pp = list(dict.fromkeys(_c))

    #     # ✅ 제품 멀티셀렉트는 "제품 뎁스"일 때만 노출
    #     if view_level_pp == "제품":
    #         tmpP = base4.copy()
    #         tmpP = tmpP[tmpP["product_cat_a"].isin(sel_a_pp)].copy()
    #         if sel_b_pp is not None:
    #             # sel_b_pp가 빈 리스트면 = 아무 중분류도 선택 안 한 것 → 후보 0개
    #             tmpP = tmpP[tmpP["product_cat_b"].isin(sel_b_pp)].copy() if len(sel_b_pp) > 0 else tmpP.iloc[0:0].copy()
    #         if sel_c_pp is not None:
    #             tmpP = tmpP[tmpP["product_cat_c"].isin(sel_c_pp)].copy() if len(sel_c_pp) > 0 else tmpP.iloc[0:0].copy()

    #         prod_candidates = topk_values(tmpP["product_name"], max(50, 200))
    #         sel_products_pp = st.multiselect(
    #             "제품 선택 (선택 시: 선택 제품만 / 미선택 시: pills로 정의된 제품군 전체)",
    #             options=prod_candidates,
    #             default=[],
    #             placeholder="제품군 전체",
    #             key="sel_products_pp_prodpath"
    #         )

    # # ──────────────────────────────────
    # # ✅ 필터 적용 로직 (핵심)
    # # - 제품 선택 O: 그 제품만
    # # - 제품 선택 X: pills로 정의된 제품군 전체(해당 군의 모든 제품)
    # # ──────────────────────────────────
    # df_p = df.copy()
    # df_p = df_p[df_p["product_cat_a"].isin(sel_a_pp)].copy()

    # if view_level_pp in ["중분류", "소분류", "제품"]:
    #     # sel_b_pp가 None이면(=브랜드 뎁스) 패스
    #     if sel_b_pp is not None:
    #         if len(sel_b_pp) == 0:
    #             df_p = df_p.iloc[0:0].copy()
    #         else:
    #             df_p = df_p[df_p["product_cat_b"].isin(sel_b_pp)].copy()

    # if view_level_pp in ["소분류", "제품"]:
    #     if sel_c_pp is not None:
    #         if len(sel_c_pp) == 0:
    #             df_p = df_p.iloc[0:0].copy()
    #         else:
    #             df_p = df_p[df_p["product_cat_c"].isin(sel_c_pp)].copy()

    # # ✅ 제품 선택이 있으면: 그 제품만 (pills보다 우선)
    # if view_level_pp == "제품" and sel_products_pp:
    #     df_p = df_p[df_p["product_name"].isin(sel_products_pp)].copy()

    # if df_p.empty:
    #     st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
    #     return

    # # ──────────────────────────────────
    # # ✅ 기간 라벨 + 유입경로 라벨
    # # ──────────────────────────────────
    # df_p = with_period(df_p, "event_date", mode_prod_path)

    # PATH_MAP_PP = {
    #     "소스 / 매체": df_p["_sourceMedium"],
    #     "소스": df_p["_source"],
    #     "매체": df_p["_medium"],
    #     "캠페인": df_p["_campaign"],
    #     "컨텐츠": df_p["_content"],
    # }
    # df_p["_path"] = PATH_MAP_PP[path_dim_pp].replace("", "(not set)")

    # top_paths = df_p["_path"].value_counts().head(topk_path_pp).index.tolist()
    # df_p["_path2"] = df_p["_path"].where(df_p["_path"].isin(top_paths), "기타")

    # # ──────────────────────────────────
    # # ✅ 제품군 전체를 합쳐서(= 제품차원 제거) 유입경로 집계
    # # ──────────────────────────────────
    # agg_path_all = (
    #     df_p.groupby(["_period", "_path2"], dropna=False)["pseudo_session_id"]
    #     .nunique()
    #     .reset_index(name="sessions")
    #     .rename(columns={"_period": "날짜", "_path2": "유입경로"})
    # )

    # if agg_path_all.empty:
    #     st.info("표시할 데이터가 없습니다.")
    #     return

    # # ✅ 그래프(뭉텅이 1개)
    # render_stack_bar_share_hover(
    #     agg_path_all,
    #     x="날짜",
    #     y="sessions",
    #     color="유입경로",
    #     height=360,
    #     opacity=0.6
    # )

    # # ✅ 표(아래)
    # pv = build_pivot_table(agg_path_all, index_col="유입경로", col_col="날짜", val_col="sessions")
    # render_table(pv, index_col="유입경로", decimals=0)


    # ──────────────────────────────────
    # 4) 네 번째 영역 (브랜드별 분리 렌더)
    # ──────────────────────────────────
    st.header(" ")  # 공백 필수
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>품목별 </span>PDP조회 유입</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ품목별로 어떤 세션을 통해 이벤트가 발생했는지, 매체/채널별 증감 변화를 확인합니다.", unsafe_allow_html=True)

    with st.expander("Filter", expanded=True):
        r1, r2, r3, r4 = st.columns([1.4, 2.6, 2.0, 2.0], vertical_alignment="bottom")
        with r1:
            mode_prod_path = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_prod_path")
        with r2:
            view_level_pp = st.radio("품목 뎁스", ["브랜드", "중분류", "소분류", "제품"], index=3, horizontal=True, key="view_level_prod_path")
        with r3:
            topk_path_pp = st.selectbox("유입경로 Top K", [7, 10, 15, 20], index=1, key="topk_path_pp")
        with r4:
            path_dim_pp = st.selectbox("유입 기준", ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"], index=0, key="path_dim_prod_path")

        base4 = df  # ✅ 원본 참조
        brand_order = ["슬립퍼", "누어"]
        brands_exist = _brands_exist(base4, brand_order)
        sel_a_pp = brands_exist[:]   # 브랜드는 고정

        need_ab = view_level_pp in ["중분류", "소분류", "제품"]
        need_c  = view_level_pp in ["소분류", "제품"]

        sel_b_by_brand, sel_c_by_brand = ({}, {})
        if view_level_pp != "브랜드":
            sel_b_by_brand, sel_c_by_brand = _render_brand_pills_ui(
                base=base4,
                brand_order=brand_order,
                brands_exist=brands_exist,
                need_ab=need_ab,
                need_c=need_c,
                key_prefix="prodpath"
            )

        sel_products_pp = None
        if view_level_pp == "제품":
            prod_candidates = _build_product_candidates_for_multiselect(
                base=base4,
                brand_order=brand_order,
                brands_exist=brands_exist,
                view_level=view_level_pp,
                need_ab=need_ab,
                need_c=need_c,
                sel_b_by_brand=sel_b_by_brand,
                sel_c_by_brand=sel_c_by_brand,
            )
            sel_products_pp = st.multiselect(
                "제품 선택 (선택 시: 선택 제품만 / 미선택 시: pills로 정의된 제품군 전체)",
                options=prod_candidates,
                default=[],
                placeholder="제품군 전체",
                key="sel_products_pp_prodpath"
            )

    # ──────────────────────────────────
    # ✅ 브랜드별로 분리 집계 + 렌더 (뭉텅이 1개씩)
    # ──────────────────────────────────
    if not sel_a_pp:
        st.info("브랜드 데이터가 없습니다.")
        return

    for brand in sel_a_pp:
        df_b = df[df["product_cat_a"] == brand].copy()
        if df_b.empty:
            st.info(f"{brand}: 데이터가 없습니다.")
            continue

        df_b = _apply_brand_filters(
            df_brand=df_b,
            brand=brand,
            view_level=view_level_pp,
            need_ab=need_ab,
            need_c=need_c,
            sel_b_by_brand=sel_b_by_brand,
            sel_c_by_brand=sel_c_by_brand,
            sel_products=sel_products_pp
        )

        st.markdown(f"###### {brand}")

        if df_b.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
            st.markdown(" ")
            continue

        df_b = with_period(df_b, "event_date", mode_prod_path)

        PATH_MAP_PP = {
            "소스 / 매체": df_b["_sourceMedium"],
            "소스": df_b["_source"],
            "매체": df_b["_medium"],
            "캠페인": df_b["_campaign"],
            "컨텐츠": df_b["_content"],
        }
        df_b["_path"] = PATH_MAP_PP[path_dim_pp].replace("", "(not set)")

        top_paths = df_b["_path"].value_counts().head(topk_path_pp).index.tolist()
        df_b["_path2"] = df_b["_path"].where(df_b["_path"].isin(top_paths), "기타")

        agg_path_brand = (
            df_b.groupby(["_period", "_path2"], dropna=False)["pseudo_session_id"]
            .nunique()
            .reset_index(name="sessions")
            .rename(columns={"_period": "날짜", "_path2": "유입경로"})
        )

        if agg_path_brand.empty:
            st.info("표시할 데이터가 없습니다.")
            st.markdown(" ")
            continue

        # ✅ 그래프(브랜드별 1개)
        render_stack_bar_share_hover(
            agg_path_brand,
            x="날짜",
            y="sessions",
            color="유입경로",
            height=360,
            opacity=0.6
        )

        # ✅ 표(아래)
        pv = build_pivot_table(agg_path_brand, index_col="유입경로", col_col="날짜", val_col="sessions")
        render_table(pv, index_col="유입경로", decimals=0)

        st.markdown(" ")

