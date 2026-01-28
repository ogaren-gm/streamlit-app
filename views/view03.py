# SEOHEE
# 2026-01-27 ver.

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys
import plotly.express as px # 추가

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery

import modules.ui_common as ui
importlib.reload(ui)


# ──────────────────────────────────
# CONFIG
# ──────────────────────────────────
CFG = {
    "TZ": "Asia/Seoul",
    "CACHE_TTL": 3600,
    "DEFAULT_LOOKBACK_DAYS": 14,
    "HEADER_UPDATE_AM": 850,
    "HEADER_UPDATE_PM": 1535,

    "HIER_BRAND": ["슬립퍼", "누어"],                # Order Rule - 대분류 고정 순서
    "HIER_CATE": ["매트리스", "프레임", "부자재"],    # Order Rule - 중분류 우선 순위

    "OPTS_TOPK": [5, 10, 15, 20],
    "OPTS_PERIOD": ["일별", "주별"],
    "OPTS_PATH": ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"],

    "SIZE_LABEL" : ["MS","S","SS","Q","K","LK","EK","S/SS","D/Q","Q/K","D/Q/K"], # 추가
    "SIZE_LABEL_MATCH" : {"01":"MS","02":"S","03":"SS","04":"Q","05":"K","06":"LK","07":"EK","31":"S/SS","32":"D/Q","33":"Q/K","34":"D/Q/K"}, # 추가


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
# HELPER
# ──────────────────────────────────

# 문자열 컬럼 안전 정리
def _safe_str(df0: pd.DataFrame, col: str) -> pd.Series:
    s = df0[col] if col in df0.columns else pd.Series([""] * len(df0), index=df0.index)
    return s.astype(str).replace("nan", "").fillna("").str.strip()

# value_counts 결과 캐시
_OPT_CACHE: dict[tuple[int, str], list[str]] = {}

def _select_opt(df0: pd.DataFrame, col: str, label: str, key: str):
    ck = (id(df0), col)
    if ck not in _OPT_CACHE:
        s = _safe_str(df0, col)
        _OPT_CACHE[ck] = ["전체"] + s[s != ""].value_counts(dropna=False).index.astype(str).tolist()
    return st.selectbox(label, _OPT_CACHE[ck], index=0, key=key)

# 누적막대 + 피벗 테이블 공통 렌더
def _render_stack_and_table(agg: pd.DataFrame, mode: str, y: str, color: str, key: str, height: int = 360):
    if agg is None or agg.empty:
        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        st.markdown(" ")
        return

    ui.render_stack_graph(agg, x="_period_dt", y=y, color=color, height=height, opacity=0.6, show_value_in_hover=True, key=key)

    # 표는 기존처럼 "기간" 라벨을 컬럼으로 사용 < ?? 
    ui.render_table(
        ui.build_pivot_table(agg, index_col=color, col_col="기간", val_col=y),
        index_col=color,
        decimals=0
    )
    st.markdown(" ")

# 중분류 우선순위
def _hier_rank(text: str) -> int:
    t = (text or "").strip()
    for i, kw in enumerate(CFG["HIER_CATE"]):
        if kw in t:
            return i
    return 99

# 중분류 옵션 정렬
def _sort_b_opts(tb: pd.DataFrame) -> list[str]:
    b = [x for x in _safe_str(tb, "product_cat_b").unique().tolist() if x]
    return sorted(b, key=lambda x: (_hier_rank(x), x))

# 소분류 옵션 정렬
def _sort_c_opts(tb: pd.DataFrame) -> list[str]:
    if "product_cat_c" not in tb.columns:
        return []

    b = _safe_str(tb, "product_cat_b")
    c = _safe_str(tb, "product_cat_c")
    ok = (c != "") & (c.str.lower() != "nan")
    if not ok.any():
        return []

    tmp = pd.DataFrame({"b": b[ok], "c": c[ok]}).drop_duplicates()
    tmp["_r"] = tmp["b"].map(_hier_rank)

    rep = (
        tmp.sort_values(["c", "_r", "b"])
        .groupby("c", as_index=False)
        .first()
    )
    rep["_k"] = rep.apply(lambda r: (_hier_rank(r["b"]), r["b"], r["c"]), axis=1)
    return rep.sort_values("_k")["c"].tolist()

# 브랜드 리스트 (고정 순서)
def _brand_list(df0: pd.DataFrame) -> list[str]:
    exist = _safe_str(df0, "product_cat_a").unique().tolist()
    return [b for b in CFG["HIER_BRAND"] if b in exist]

# 브랜드 계층 필터
def _apply_brand_hier_filter(
    df_in: pd.DataFrame,
    brand: str,
    view_level: str,
    need_ab: bool,
    need_c: bool,
    sel_b_by_brand: dict,
    sel_c_by_brand: dict,
    sel_products: list | None
) -> pd.DataFrame:
    tb = df_in[df_in["product_cat_a"] == brand]
    if tb.empty:
        return tb

    if view_level in ["중분류", "소분류", "제품"] and need_ab:
        picked = sel_b_by_brand.get(brand)
        if picked is not None:
            if not picked:
                return tb.iloc[0:0]
            tb = tb[tb["product_cat_b"].isin(picked)]

    if view_level in ["소분류", "제품"] and need_c:
        picked = sel_c_by_brand.get(brand)
        if picked is not None:
            if not picked:
                return tb.iloc[0:0]
            tb = tb[tb["product_cat_c"].isin(picked)]

    if view_level == "제품" and sel_products:
        tb = tb[tb["product_name"].isin(sel_products)]

    return tb

def _get_src_dim(sel):
    if sel == "소스 / 매체":
        return "_sourceMedium", "소스/매체"
    if sel == "소스":
        return "_source", "소스"
    if sel == "매체":
        return "_medium", "매체"
    if sel == "캠페인":
        return "_campaign", "캠페인"
    return "_content", "컨텐츠"

# ✅ (신규 helper 1개) 집계만 담당: 기간/차원(topk->기타) + groupby(+_period_dt)
def _agg_period_dim(tb: pd.DataFrame, mode: str, dim: pd.Series, dim_label: str, topk: int, metrics: dict[str, tuple[str, str]]) -> pd.DataFrame:
    tmp = ui.add_period_columns(tb, "event_date", mode)

    s = dim.reindex(tmp.index).astype(str).replace("nan", "").fillna("").str.strip()
    s = s.replace("", "기타")

    topv = set(ui.get_topk_values(s[s != "기타"], topk))
    tmp.loc[:, "_dim2"] = s.where(s.isin(topv), "기타")

    agg = (
        tmp.groupby(["_period", "_dim2"], dropna=False)
            .agg(
                **metrics,
                _period_dt=("_period_dt", "min"),
            )
            .reset_index()
            .rename(columns={"_period": "기간", "_dim2": dim_label})
            .sort_values("_period_dt")
            .reset_index(drop=True)
    )
    return agg


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
        df = bq.get_data("tb_sleeper_e_cart")
        geo_map = bq.get_data("raw_geo_city_kr")

        # 안전 장치
        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d", errors="coerce")
        if "event_name" in df.columns:
            df = df[df["event_name"] == "add_to_cart"]

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

        # 파생 컬럼 (2) 접속 권역
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

    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df = load_data(cs, ce)

    # ──────────────────────────────────
    # D) Header
    # ──────────────────────────────────
    st.subheader("장바구니 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px; line-height:1.5;">
            GA 기준 <b>장바구니 담기</b> 추이와 유입경로를
            <b>브랜드·품목·제품</b> 단위로 확인할 수 있는 대시보드 입니다.<br>
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
    # 1) 장바구니 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>장바구니 </span>추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ장바구니 담기의 증감 추이를 확인합니다.")

    with st.popover("🤔 유저 VS 세션 VS 이벤트"):
        st.markdown("""
    - **유저수 (user_pseudo_id)**  
    고유 사람 수 (중복 제거).

    - **세션수 (pseudo_session_id)**  
    방문 단위 수 (같은 사람도 방문이 나뉘면 세션이 늘어남).

    - **이벤트수 (add_to_cart)**  
    한 방문(세션) 안에서 발생한 행동의 총 횟수.

    - **예시**  
    사람 A가 1월 1일 오전에 시그니처를 조회 후 이탈, 오후에 시그니처와 허쉬를 재조회했다면  
    1월 1일의 **유저수=1**, **세션수=2**, **이벤트수=3** 입니다.
    """)


    # 필터
    with st.expander("Filter", expanded=True):
        r0_1, r0_2 = st.columns([1, 2], vertical_alignment="bottom")
        with r0_1:
            mode_all = st.radio("기간 단위", CFG["OPTS_PERIOD"], horizontal=True, key="mode_all")
        with r0_2:
            metric_map = {"유저수": "add_to_cart_users", "세션수": "add_to_cart_sessions", "이벤트수": "add_to_cart_events"}
            sel_metrics = st.pills(
                "집계 단위",
                list(metric_map.keys()),
                selection_mode="multi",
                default=list(metric_map.keys()),
                key="sel_metrics_all"
            ) or list(metric_map.keys())

    # 기간 라벨/정렬용 컬럼(_period, _period_dt) 생성
    base = ui.add_period_columns(df, "event_date", mode_all)

    # 기간별 유저/세션/이벤트를 1회 groupby로 집계
    df_all = (
        base.groupby("_period", dropna=False)
            .agg(
                add_to_cart_users=("user_pseudo_id", "nunique"),
                add_to_cart_sessions=("pseudo_session_id", "nunique"),
                add_to_cart_events=("user_pseudo_id", "size"),
                _period_dt=("_period_dt", "min"),
            )
            .reset_index()
            .rename(columns={"_period": "기간"})
            .sort_values("_period_dt")
            .reset_index(drop=True)
    )

    # 파생지표(SPU/EPS)
    df_all["sessions_per_user"] = (df_all["add_to_cart_sessions"] / df_all["add_to_cart_users"]).replace([np.inf, -np.inf], np.nan)
    df_all["events_per_session"] = (df_all["add_to_cart_events"] / df_all["add_to_cart_sessions"]).replace([np.inf, -np.inf], np.nan)

    # 그래프 범례 표기용 한글 컬럼 생성
    df_plot = df_all.rename(columns={
        "add_to_cart_users": "유저수",
        "add_to_cart_sessions": "세션수",
        "add_to_cart_events": "이벤트수",
    })

    # 표시 지표 순서 고정 + 최소 1개 보장
    ORDER = ["유저수", "세션수", "이벤트수"]
    y_cols = [m for m in ORDER if m in (sel_metrics or [])]
    y_cols = y_cols or ["유저수"]

    # ✅ (그래프) 장바구니 추이 라인차트
    x_col = "_period_dt"
    ui.render_line_graph(df_plot, x=x_col, y=y_cols, height=360, title=None)

    # 표 출력용 지표 정의(표시는 고정 순서)
    rows = [
        ("유저수", "add_to_cart_users", "int"),
        ("세션수", "add_to_cart_sessions", "int"),
        ("이벤트수", "add_to_cart_events", "int"),
        ("SPU (세션수/유저수)", "sessions_per_user", "float2"),
        ("EPS (이벤트수/세션수)", "events_per_session", "float2"),
    ]

    # 기간 컬럼을 가로축으로 하는 피벗형 테이블 구성
    dates = df_all["기간"].astype(str).tolist()
    pv = pd.DataFrame({"지표": [r[0] for r in rows]})
    for dt in dates:
        pv[dt] = ""

    m = df_all.set_index("기간").to_dict(orient="index")

    # 포맷 규칙(int / float2)
    def _fmt(v, kind: str) -> str:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        if kind == "int":
            try:
                return f"{int(round(float(v))):,}"
            except Exception:
                return ""
        try:
            return f"{float(v):.2f}"
        except Exception:
            return ""

    # 셀 단위 값 채우기
    for i, (_, col, kind) in enumerate(rows):
        for dt in dates:
            pv.at[i, dt] = _fmt(m.get(dt, {}).get(col, np.nan), kind)

    # 기간 라벨 정렬 적용(일별/주별 모두 대응)
    pv = pv[["지표", *ui.sort_period_labels(dates)]]

    # ✅ (표) 장바구니 지표 요약 테이블
    st.dataframe(pv, row_height=30, hide_index=True, use_container_width=True)

    # ──────────────────────────────────
    # 2) 장바구니 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>장바구니 </span>현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ장바구니 담기가 발생한 지역 또는 유입한 매체별 비중을 확인합니다.")

    # 필터
    with st.expander("Filter", expanded=False):
        c1, c2, c3 = st.columns([1, 1, 1], vertical_alignment="bottom")
        with c1:
            mode = st.radio("기간 단위", CFG["OPTS_PERIOD"], index=0, horizontal=True, key="atc_comm_m")
        with c2:
            unit = st.radio("집계 단위", ["유저수", "세션수"], index=1, horizontal=True, key="atc_comm_u")
        with c3:
            topk = st.selectbox("표시 Top K", CFG["OPTS_TOPK"], index=1, key="atc_comm_k")

    tab_src, tab_geo_kr, tab_geo, tab_mix = st.tabs(["유입매체", "접속권역", "접속지역", "매체X권역"])

    with tab_src:
        with st.expander("Filter", expanded=True):
            c1, c2, _p = st.columns([1, 1, 2], vertical_alignment="bottom")
            with c1:
                sel_dim = st.selectbox("유입 단위", CFG["OPTS_PATH"], index=0, key="atc_s_d")
            with c2:
                dim_col, dim_label = _get_src_dim(sel_dim)
                sel = _select_opt(df, dim_col, "유입 선택", "atc_s_v")
            with _p:
                pass

        df_f = df
        if sel != "전체" and dim_col in df_f.columns:
            df_f = df_f[df_f[dim_col] == sel]

        if df_f is None or df_f.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        else:
            grp = _agg_period_dim(
                tb=df_f,
                mode=mode,
                dim=_safe_str(df_f, dim_col),
                dim_label=dim_label,
                topk=topk,
                metrics={
                    "세션수": ("pseudo_session_id", "nunique"),
                    "유저수": ("user_pseudo_id", "nunique"),
                }
            )

            chart_key = f"atc_stack::tab=src::{dim_label}::{dim_col}::{mode}::{unit}::{topk}"
            if sel != "전체":
                chart_key += f"::{dim_col}={sel}"

            _render_stack_and_table(
                agg=grp,
                mode=mode,
                y=unit,
                color=dim_label,
                key=chart_key
            )

    with tab_geo_kr:
        with st.expander("Filter", expanded=True):
            c1, _p = st.columns([1, 3], vertical_alignment="bottom")
            with c1:
                sel = _select_opt(df, "geo__city_kr", "권역 선택", "atc_gk_s")
            with _p:
                pass

        df_f = df
        if sel != "전체" and "geo__city_kr" in df_f.columns:
            df_f = df_f[df_f["geo__city_kr"] == sel]

        if df_f is None or df_f.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        else:
            dim_col = "geo__city_kr"
            dim_label = "접속권역"

            grp = _agg_period_dim(
                tb=df_f,
                mode=mode,
                dim=_safe_str(df_f, dim_col),
                dim_label=dim_label,
                topk=topk,
                metrics={
                    "세션수": ("pseudo_session_id", "nunique"),
                    "유저수": ("user_pseudo_id", "nunique"),
                }
            )

            chart_key = f"atc_stack::tab=gk::{dim_label}::{dim_col}::{mode}::{unit}::{topk}"
            if sel != "전체":
                chart_key += f"::{dim_col}={sel}"

            _render_stack_and_table(
                agg=grp,
                mode=mode,
                y=unit,
                color=dim_label,
                key=chart_key
            )

    with tab_geo:
        with st.expander("Filter", expanded=True):
            c1, c2, _p = st.columns([1, 1, 2], vertical_alignment="bottom")
            with c1:
                sel_kr = _select_opt(df, "geo__city_kr", "권역 선택", "atc_g_kr")
            with c2:
                sel = _select_opt(df, "geo__city", "지역 선택", "atc_g_c")
            with _p:
                pass

        df_f = df
        if sel_kr != "전체" and "geo__city_kr" in df_f.columns:
            df_f = df_f[df_f["geo__city_kr"] == sel_kr]
        if sel != "전체" and "geo__city" in df_f.columns:
            df_f = df_f[df_f["geo__city"] == sel]

        if df_f is None or df_f.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        else:
            dim_col = "geo__city"
            dim_label = "접속지역"

            grp = _agg_period_dim(
                tb=df_f,
                mode=mode,
                dim=_safe_str(df_f, dim_col),
                dim_label=dim_label,
                topk=topk,
                metrics={
                    "세션수": ("pseudo_session_id", "nunique"),
                    "유저수": ("user_pseudo_id", "nunique"),
                }
            )

            chart_key = f"atc_stack::tab=g::{dim_label}::{dim_col}::{mode}::{unit}::{topk}"
            if sel_kr != "전체":
                chart_key += f"::geo__city_kr={sel_kr}"
            if sel != "전체":
                chart_key += f"::geo__city={sel}"

            _render_stack_and_table(
                agg=grp,
                mode=mode,
                y=unit,
                color=dim_label,
                key=chart_key
            )

    with tab_mix:
        with st.expander("Filter", expanded=True):
            c1, c2, c3, c4 = st.columns([1, 1, 1, 1], vertical_alignment="bottom")
            with c1:
                sel_dim = st.selectbox("유입 단위", CFG["OPTS_PATH"], index=0, key="atc_m_d")
            with c2:
                dim_col, dim_label = _get_src_dim(sel_dim)
                sel_src = _select_opt(df, dim_col, "유입 선택", "atc_m_s")
            with c3:
                sel_kr = _select_opt(df, "geo__city_kr", "권역 선택", "atc_m_kr")
            with c4:
                sel_city = _select_opt(df, "geo__city", "지역 선택", "atc_m_c")

        df_f = df
        if sel_src != "전체" and dim_col in df_f.columns:
            df_f = df_f[df_f[dim_col] == sel_src]
        if sel_kr != "전체" and "geo__city_kr" in df_f.columns:
            df_f = df_f[df_f["geo__city_kr"] == sel_kr]
        if sel_city != "전체" and "geo__city" in df_f.columns:
            df_f = df_f[df_f["geo__city"] == sel_city]

        if df_f is None or df_f.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        else:
            grp = _agg_period_dim(
                tb=df_f,
                mode=mode,
                dim=_safe_str(df_f, dim_col),
                dim_label=dim_label,
                topk=topk,
                metrics={
                    "세션수": ("pseudo_session_id", "nunique"),
                    "유저수": ("user_pseudo_id", "nunique"),
                }
            )

            chart_key = f"atc_stack::tab=mix::{dim_label}::{dim_col}::{mode}::{unit}::{topk}"
            if sel_src != "전체":
                chart_key += f"::{dim_col}={sel_src}"
            if sel_kr != "전체":
                chart_key += f"::geo__city_kr={sel_kr}"
            if sel_city != "전체":
                chart_key += f"::geo__city={sel_city}"

            _render_stack_and_table(
                agg=grp,
                mode=mode,
                y=unit,
                color=dim_label,
                key=chart_key
            )

    # ──────────────────────────────────
    # 3) 품목별 추이
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>품목별 </span>추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ품목별로 장바구니 담기의 증감 추이를 확인합니다.")

    with st.popover("🤔 품목 뎁스 설명"):
        st.markdown("""
                    - **대분류(브랜드)** : 슬립퍼 or 누어 
                    - **중분류** : 매트리스 or 프레임 or 부자재
                    - **소분류** :  
                        - 매트리스는 모두 매트리스/토퍼  
                        - 슬립퍼 **프레임** : 원목 or 패브릭 or 호텔침대  
                        - 누어 **프레임** : 룬드 or 수입파운 or 원목  
                        - 슬립퍼 **부자재** : 경추베개 외 기타  
                        - 누어 **부자재** : 룬드 라이브러리 외 기타   
                    - **참고**   
                    소분류 중 부자재의 '기타' 외 세부 구성은 변경될 수 있으며, 필요 시 별도 문의 바랍니다.  
                    """)


    tab1, tab2 = st.tabs(["커스텀", "PASS"])

    # tab1
    with tab1:
        with st.expander("Filter", expanded=True):
            c1, c2, c3 = st.columns([1.8, 2.6, 2.0], vertical_alignment="bottom")
            with c1:
                mode_cat = st.radio("기간 단위", CFG["OPTS_PERIOD"], horizontal=True, key="mode_cat_tab1")
            with c2:
                view_level = st.radio("품목 뎁스", ["브랜드", "중분류", "소분류", "제품"], index=2, horizontal=True, key="view_level_tab1")
            with c3:
                topk_cat = st.selectbox("표시 Top K", CFG["OPTS_TOPK"], index=1, key="topk_cat_tab1")

            base2 = df
            brand_order = CFG["HIER_BRAND"]
            brands_exist = [b for b in brand_order if b in base2.get("product_cat_a", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()]
            sel_a = brands_exist[:]

            need_ab = view_level in ["중분류", "소분류", "제품"]
            need_c = view_level in ["소분류", "제품"]

            sel_b_by_brand, sel_c_by_brand = {}, {}
            if view_level != "브랜드":
                for b in brand_order:
                    if b not in brands_exist:
                        continue
                    tb0 = base2[base2["product_cat_a"] == b]
                    if tb0 is None or tb0.empty:
                        continue

                    cc0, cc1, cc2 = st.columns([1, 2, 8], vertical_alignment="center")
                    with cc0:
                        st.markdown(
                            f"<div style='font-size:13px;font-weight:700;line-height:1;white-space:nowrap;'>{b}</div>",
                            unsafe_allow_html=True
                        )

                    if need_ab:
                        b_opts = _sort_b_opts(tb0)
                        with cc1:
                            sel_b_by_brand[b] = st.pills(
                                " ", b_opts,
                                selection_mode="multi",
                                default=b_opts,
                                key=f"cat_tab1__ab__{b}"
                            ) or []
                    else:
                        sel_b_by_brand[b] = None
                        with cc1:
                            st.markdown(" ")

                    if need_c:
                        picked_b = (sel_b_by_brand.get(b) or [])
                        tb1 = tb0[tb0["product_cat_b"].isin(picked_b)] if len(picked_b) > 0 else tb0.iloc[0:0]
                        c_opts = _sort_c_opts(tb1)
                        with cc2:
                            sel_c_by_brand[b] = st.pills(
                                " ", c_opts,
                                selection_mode="multi",
                                default=c_opts,
                                key=f"cat_tab1__c__{b}"
                            ) or []
                    else:
                        sel_c_by_brand[b] = None
                        with cc2:
                            st.markdown(" ")

            sel_products = None
            if view_level == "제품":
                tmpP = base2[base2["product_cat_a"].isin(brands_exist)]
                mask = pd.Series(False, index=tmpP.index)

                for b in brand_order:
                    if b not in brands_exist:
                        continue
                    tb0 = tmpP[tmpP["product_cat_a"] == b]
                    if tb0 is None or tb0.empty:
                        continue
                    if need_ab:
                        picked_b = (sel_b_by_brand.get(b) or [])
                        tb0 = tb0[tb0["product_cat_b"].isin(picked_b)] if len(picked_b) > 0 else tb0.iloc[0:0]
                    if need_c:
                        picked_c = (sel_c_by_brand.get(b) or [])
                        tb0 = tb0[tb0["product_cat_c"].isin(picked_c)] if len(picked_c) > 0 else tb0.iloc[0:0]
                    mask.loc[tb0.index] = True

                tmpP = tmpP[mask]
                prod_candidates = ui.get_topk_values(tmpP["product_name"], 200) if (tmpP is not None and not tmpP.empty and "product_name" in tmpP.columns) else []

                sel_products = st.multiselect(
                    "제품 선택 (미선택시 선택된 Top K 모두 표시)",
                    options=prod_candidates,
                    default=[],
                    placeholder="전체",
                    key="sel_products_tab1"
                )

        baseP = df[df["product_cat_a"].isin(sel_a)]

        dim_map = {
            "브랜드": "product_cat_a",
            "중분류": "product_cat_b",
            "소분류": "product_cat_c",
            "제품": "product_name",
        }
        dim = dim_map[view_level]

        for brand in sel_a:
            st.markdown(f"###### {brand}")

            tb = _apply_brand_hier_filter(
                baseP, brand, view_level, need_ab, need_c,
                sel_b_by_brand, sel_c_by_brand, sel_products
            )
            if tb is None or tb.empty:
                st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                st.markdown(" ")
                continue

            # # (표시 TopK) 제품 선택을 직접 했으면 치환하지 않음
            # if view_level in ["중분류", "소분류", "제품"] and not (view_level == "제품" and sel_products):
            #     if dim in tb.columns:
            #         top_vals = ui.get_topk_values(tb[dim], topk_cat)
            #         tb.loc[:, dim] = tb[dim].where(tb[dim].isin(top_vals), "기타")

            agg = _agg_period_dim(
                tb=tb,
                mode=mode_cat,
                dim=_safe_str(tb, dim),
                dim_label="구분",
                topk=topk_cat,
                metrics={"sessions": ("pseudo_session_id", "nunique")}
            )

            _render_stack_and_table(
                agg=agg,
                mode=mode_cat,
                y="sessions",
                color="구분",
                key=f"cat_stack__{brand}__{view_level}__{mode_cat}__{topk_cat}",
                height=340
            )

    with tab2:
        pass

    # ──────────────────────────────────
    # 4) 품목별 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>품목별 </span>현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ품목별로 장바구니 담기가 발생한 지역 또는 유입한 매체별 비중을 확인합니다.", unsafe_allow_html=True)

    def _k4(tag: str) -> str:
        return f"pp4__{tag}"

    # 공통 필터
    with st.expander("Filter", expanded=True):
        c1, c2, c3 = st.columns([1.8, 2.6, 2.0], vertical_alignment="bottom")
        with c1:
            mode_pp4 = st.radio("기간 단위", CFG["OPTS_PERIOD"], horizontal=True, key=_k4("mode"))
        with c2:
            view_pp4 = st.radio("품목 뎁스", ["브랜드", "중분류", "소분류", "제품"], index=3, horizontal=True, key=_k4("view"))
        with c3:
            topk_pp4 = st.selectbox("표시 Top K", CFG["OPTS_TOPK"], index=1, key=_k4("topk"))

        brands = _brand_list(df)
        need_ab = view_pp4 in ["중분류", "소분류", "제품"]
        need_c = view_pp4 in ["소분류", "제품"]

        sel_b, sel_c = {}, {}
        if view_pp4 != "브랜드":
            for b in brands:
                tb0 = df[df["product_cat_a"] == b]
                if tb0 is None or tb0.empty:
                    continue

                h0, h1, h2 = st.columns([1, 2, 8], vertical_alignment="center")
                with h0:
                    st.markdown(
                        f"<div style='font-size:13px;font-weight:700;line-height:1;white-space:nowrap;'>{b}</div>",
                        unsafe_allow_html=True
                    )

                if need_ab:
                    bo = _sort_b_opts(tb0)
                    with h1:
                        sel_b[b] = st.pills(" ", bo, selection_mode="multi", default=bo, key=_k4(f"ab__{b}")) or []
                else:
                    sel_b[b] = None
                    with h1:
                        st.markdown(" ")

                if need_c:
                    pb = (sel_b.get(b) or [])
                    tb1 = tb0[tb0["product_cat_b"].isin(pb)] if len(pb) > 0 else tb0.iloc[0:0]
                    co = _sort_c_opts(tb1)
                    with h2:
                        sel_c[b] = st.pills(" ", co, selection_mode="multi", default=co, key=_k4(f"c__{b}")) or []
                else:
                    sel_c[b] = None
                    with h2:
                        st.markdown(" ")

        sel_p = None
        if view_pp4 == "제품":
            tmpP = df[df["product_cat_a"].isin(brands)]
            mask = pd.Series(False, index=tmpP.index)

            for b in brands:
                tb0 = tmpP[tmpP["product_cat_a"] == b]
                if tb0 is None or tb0.empty:
                    continue
                if need_ab:
                    pb = (sel_b.get(b) or [])
                    tb0 = tb0[tb0["product_cat_b"].isin(pb)] if len(pb) > 0 else tb0.iloc[0:0]
                if need_c:
                    pc = (sel_c.get(b) or [])
                    tb0 = tb0[tb0["product_cat_c"].isin(pc)] if len(pc) > 0 else tb0.iloc[0:0]
                mask.loc[tb0.index] = True

            tmpP = tmpP[mask]
            cand = ui.get_topk_values(tmpP["product_name"], 200) if (tmpP is not None and not tmpP.empty and "product_name" in tmpP.columns) else []
            sel_p = st.multiselect(
                "제품 선택 (미선택시 선택된 품목군 모두 표시)",
                options=cand, default=[], placeholder="전체", key=_k4("prod")
            )

    if not brands:
        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
    else:
        tab_src, tab_gk, tab_g, tab_mix = st.tabs(["유입매체", "접속권역", "접속지역", "매체X권역"])

        with tab_src:
            with st.expander("Filter", expanded=True):
                c1, c2, _p = st.columns([1, 1, 2], vertical_alignment="bottom")
                with c1:
                    sel_dim = st.selectbox("유입 단위", CFG["OPTS_PATH"], index=0, key=_k4("src__d"))
                with c2:
                    dim_col, dim_label = _get_src_dim(sel_dim)
                    sel_dim_val = _select_opt(df, dim_col, "유입 선택", key=_k4("src__v"))
                with _p:
                    pass

            for b in brands:
                st.markdown(f"###### {b}")

                tb = _apply_brand_hier_filter(df, b, view_pp4, need_ab, need_c, sel_b, sel_c, sel_p)
                if tb is None or tb.empty:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue

                if sel_dim_val != "전체" and dim_col in tb.columns:
                    tb = tb[tb[dim_col] == sel_dim_val]

                if tb is None or tb.empty:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue

                # 유입경로는 사용자 선택(소스/매체/캠페인/컨텐츠) 기반으로 만든 "_path"
                PATH_MAP = {
                    "소스 / 매체": _safe_str(tb, "_sourceMedium").replace("", "(not set)"),
                    "소스": _safe_str(tb, "_source").replace("", "(not set)"),
                    "매체": _safe_str(tb, "_medium").replace("", "(not set)"),
                    "캠페인": _safe_str(tb, "_campaign").replace("", "(not set)"),
                    "컨텐츠": _safe_str(tb, "_content").replace("", "(not set)"),
                }
                s_path = PATH_MAP[sel_dim]

                agg = _agg_period_dim(
                    tb=tb,
                    mode=mode_pp4,
                    dim=s_path,
                    dim_label="유입경로",
                    topk=topk_pp4,
                    metrics={"sessions": ("pseudo_session_id", "nunique")}
                )

                _render_stack_and_table(
                    agg=agg,
                    mode=mode_pp4,
                    y="sessions",
                    color="유입경로",
                    key=_k4(f"chart__src__{b}__{sel_dim}__{sel_dim_val}__{mode_pp4}__{topk_pp4}")
                )

        with tab_gk:
            with st.expander("Filter", expanded=True):
                c1, _p = st.columns([1, 3], vertical_alignment="bottom")
                with c1:
                    sel_kr = _select_opt(df, "geo__city_kr", "권역 선택", key=_k4("gk__s"))
                with _p:
                    pass

            for b in brands:
                st.markdown(f"###### {b}")

                tb = _apply_brand_hier_filter(df, b, view_pp4, need_ab, need_c, sel_b, sel_c, sel_p)
                if tb is None or tb.empty:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue

                if sel_kr != "전체" and "geo__city_kr" in tb.columns:
                    tb = tb[tb["geo__city_kr"] == sel_kr]

                if tb is None or tb.empty:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue

                agg = _agg_period_dim(
                    tb=tb,
                    mode=mode_pp4,
                    dim=_safe_str(tb, "geo__city_kr"),
                    dim_label="접속권역",
                    topk=topk_pp4,
                    metrics={"sessions": ("pseudo_session_id", "nunique")}
                )

                _render_stack_and_table(
                    agg=agg,
                    mode=mode_pp4,
                    y="sessions",
                    color="접속권역",
                    key=_k4(f"chart__gk__{b}__{mode_pp4}__{topk_pp4}__{sel_kr}")
                )

        with tab_g:
            with st.expander("Filter", expanded=True):
                c1, c2, _p = st.columns([1, 1, 2], vertical_alignment="bottom")
                with c1:
                    sel_kr = _select_opt(df, "geo__city_kr", "권역 선택", key=_k4("g__kr"))
                with c2:
                    sel_g = _select_opt(df, "geo__city", "지역 선택", key=_k4("g__c"))
                with _p:
                    pass

            for b in brands:
                st.markdown(f"###### {b}")

                tb = _apply_brand_hier_filter(df, b, view_pp4, need_ab, need_c, sel_b, sel_c, sel_p)
                if tb is None or tb.empty:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue

                if sel_kr != "전체" and "geo__city_kr" in tb.columns:
                    tb = tb[tb["geo__city_kr"] == sel_kr]
                if sel_g != "전체" and "geo__city" in tb.columns:
                    tb = tb[tb["geo__city"] == sel_g]

                if tb is None or tb.empty:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue

                agg = _agg_period_dim(
                    tb=tb,
                    mode=mode_pp4,
                    dim=_safe_str(tb, "geo__city"),
                    dim_label="접속지역",
                    topk=topk_pp4,
                    metrics={"sessions": ("pseudo_session_id", "nunique")}
                )

                _render_stack_and_table(
                    agg=agg,
                    mode=mode_pp4,
                    y="sessions",
                    color="접속지역",
                    key=_k4(f"chart__g__{b}__{mode_pp4}__{topk_pp4}__{sel_kr}__{sel_g}")
                )

        with tab_mix:
            with st.expander("Filter", expanded=True):
                c1, c2, c3, c4 = st.columns([1, 1, 1, 1], vertical_alignment="bottom")
                with c1:
                    sel_dim = st.selectbox("유입 단위", CFG["OPTS_PATH"], index=0, key=_k4("mix__d"))
                with c2:
                    dim_col, dim_label = _get_src_dim(sel_dim)
                    sel_src = _select_opt(df, dim_col, "유입 선택", key=_k4("mix__s"))
                with c3:
                    sel_kr = _select_opt(df, "geo__city_kr", "권역 선택", key=_k4("mix__kr"))
                with c4:
                    sel_g = _select_opt(df, "geo__city", "지역 선택", key=_k4("mix__c"))

            for b in brands:
                st.markdown(f"###### {b}")

                tb = _apply_brand_hier_filter(df, b, view_pp4, need_ab, need_c, sel_b, sel_c, sel_p)
                if tb is None or tb.empty:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue

                if sel_src != "전체" and dim_col in tb.columns:
                    tb = tb[tb[dim_col] == sel_src]
                if sel_kr != "전체" and "geo__city_kr" in tb.columns:
                    tb = tb[tb["geo__city_kr"] == sel_kr]
                if sel_g != "전체" and "geo__city" in tb.columns:
                    tb = tb[tb["geo__city"] == sel_g]

                if tb is None or tb.empty:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue

                agg = _agg_period_dim(
                    tb=tb,
                    mode=mode_pp4,
                    dim=_safe_str(tb, dim_col),
                    dim_label=dim_label,
                    topk=topk_pp4,
                    metrics={"sessions": ("pseudo_session_id", "nunique")}
                )

                _render_stack_and_table(
                    agg=agg,
                    mode=mode_pp4,
                    y="sessions",
                    color=dim_label,
                    key=_k4(f"chart__mix__{b}__{sel_dim}__{sel_src}__{sel_kr}__{sel_g}__{mode_pp4}__{topk_pp4}")
                )

    # ──────────────────────────────────
    # 5) 장바구니 구성 분포
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>장바구니 옵션 분석 </h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ옵션 포함 기준의 담김 금액(가격대)·사이즈·옵션 조합의 비중을 확인합니다.", unsafe_allow_html=True)

    # 해당 영역 전용 일회용 함수
    def make_price_bucket(s: pd.Series, step: int = 500_000) -> tuple[list[int], list[str]]:
        v_max  = float(s.max() if not s.empty else 0)
        v_edge = max(step, (int(v_max // step) + 1) * step)

        bins = list(range(0, int(v_edge) + step, step))
        if len(bins) < 2:
            bins = [0, step]

        labels = [
            f"{bins[i] / 1_000_000:.1f}M ~ {bins[i + 1] / 1_000_000:.1f}M"
            for i in range(len(bins) - 1)
        ]
        return bins, labels

    def build_match_mask(s: pd.Series, q: str):
        q = (q or "").strip()
        if not q:
            return pd.Series(False, index=s.index), "미입력"

        if "&" in q:
            parts = [p.strip() for p in q.split("&") if p.strip()]
            m = pd.Series(True, index=s.index)
            for p in parts:
                m &= s.str.contains(p, regex=False, na=False)
            return m, "AND"

        if "|" in q:
            try:
                return s.str.contains(q, regex=True, na=False), "OR"
            except Exception:
                parts = [p.strip() for p in q.split("|") if p.strip()]
                m = pd.Series(False, index=s.index)
                for p in parts:
                    m |= s.str.contains(p, regex=False, na=False)
                return m, "OR"

        return s.str.contains(q, regex=False, na=False), "부분일치"

    # (리팩토링) 5-1/5-2 공통 레이아웃(차트+표) 렌더
    def _render_bar_and_table(
        df_chart: pd.DataFrame,
        df_tbl: pd.DataFrame,
        x: str,
        y: str,
        hover_pct_col: str,
        table_cols: list[str],
        table_height: int = 320,
        x_order: list[str] | None = None,
        key: str | None = None
    ):
        cL, _p, cR = st.columns([6, 0.2, 4], vertical_alignment="top")
        with cL:
            fig = px.bar(df_chart, x=x, y=y, hover_data={hover_pct_col: ":.1%"})
            fig.update_traces(opacity=0.60)
            if x_order:
                fig.update_xaxes(
                    type="category",
                    categoryorder="array",
                    categoryarray=x_order,
                    tickmode="array",
                    tickvals=x_order
                )
            fig.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=40))
            st.plotly_chart(fig, use_container_width=True, key=key)
        with _p:
            pass
        with cR:
            df_tbl = df_tbl.copy()
            df_tbl["비중"] = (pd.to_numeric(df_tbl["비중"], errors="coerce").fillna(0) * 100).round(1).astype(str) + "%"
            st.dataframe(df_tbl[table_cols], hide_index=True, row_height=30, use_container_width=True, height=table_height)

    # 컬럼 전처리
    df["item_value_total"]    = pd.to_numeric(df.get("item_value_total"), errors="coerce").fillna(0)                                           # 숫자형 변환(결측=0)
    df["items__item_variant"] = df.get("items__item_variant", "").astype(str).replace("nan","").fillna("").str.strip().replace("", "정보없음")  # 문자열 정리(빈값→정보없음)
    df["variant_size_code"]   = df.get("variant_size_code", None)                                                                              # 코드 컬럼 유지(없으면 NaN)
    v_bins, v_lbl = make_price_bucket(df["item_value_total"])
    df["price_bucket"] = pd.cut(df["item_value_total"], bins=v_bins, labels=v_lbl, right=False, include_lowest=True).astype(str).replace("nan", v_lbl[0])

    # 전체 필터
    with st.expander("Filter", expanded=True):
        prod_opts = ["전체"] + sorted(df["product_name"].dropna().astype(str).unique().tolist())
        sel_prod = st.selectbox("제품 선택", prod_opts, index=0, key="dist__product")

    df_f = df if sel_prod == "전체" else df[df["product_name"] == sel_prod]

    if df_f.empty:
        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")

    else:
        # ──────────────────────────────────
        # 5-1) 가격대 분포도
        # ──────────────────────────────────
        st.markdown(" ")
        st.markdown("<h6 style='margin:0'>가격대 분포도</h6>", unsafe_allow_html=True)

        df_bucket = (
            df_f.groupby("price_bucket", dropna=False).size()
                .reindex(v_lbl, fill_value=0)
                .reset_index(name="이벤트수")
                .rename(columns={"price_bucket": "가격대"})
        )
        total_cnt = int(df_bucket["이벤트수"].sum())
        df_bucket["비중"] = (df_bucket["이벤트수"] / max(1, total_cnt)).fillna(0)

        rep_prod = (
            df_f.groupby(["price_bucket", "product_name"], dropna=False).size()
                .reset_index(name="이벤트수")
                .sort_values(["price_bucket", "이벤트수", "product_name"], ascending=[True, False, True])
                .drop_duplicates(subset=["price_bucket"], keep="first")
                .rename(columns={
                    "price_bucket": "가격대",
                    "product_name": "대표 제품"
                })[["가격대", "대표 제품"]]
        )

        df_bucket_tbl = df_bucket.merge(rep_prod, on="가격대", how="left")
        df_bucket_tbl["대표 제품"] = df_bucket_tbl["대표 제품"].fillna("정보없음")

        _render_bar_and_table(
            df_chart=df_bucket,
            df_tbl=df_bucket_tbl,
            x="가격대",
            y="이벤트수",
            hover_pct_col="비중",
            table_cols=["가격대", "이벤트수", "비중", "대표 제품"],
            table_height=320,
            x_order=v_lbl,
            key="dist__price"
        )

        # ──────────────────────────────────
        # 5-2) 사이즈 분포도
        # ──────────────────────────────────
        st.markdown(" ")
        st.markdown("<h6 style='margin:0'>사이즈 분포도</h6>", unsafe_allow_html=True)

        df_sz = (
            df_f.assign(
                _vs=df_f["variant_size_code"]
                    .astype(str).str.strip().str.zfill(2)
                    .map(CFG["SIZE_LABEL_MATCH"])
            )
            .loc[lambda x: x["_vs"].isin(CFG["SIZE_LABEL"])]
        )

        df_size = (
            df_sz.groupby("_vs", dropna=False).size()
                .reindex(CFG["SIZE_LABEL"], fill_value=0)
                .reset_index(name="이벤트수")
                .rename(columns={"_vs": "사이즈"})
        )
        size_total = int(df_size["이벤트수"].sum())
        df_size["비중"] = (df_size["이벤트수"] / max(1, size_total)).fillna(0)

        rep_size = (
            df_sz.groupby(["_vs", "product_name"], dropna=False).size()
                .reset_index(name="이벤트수")
                .sort_values(["_vs", "이벤트수", "product_name"], ascending=[True, False, True])
                .drop_duplicates(subset=["_vs"], keep="first")
                .rename(columns={"_vs": "사이즈", "product_name": "대표 제품"})[["사이즈", "대표 제품"]]
        )

        df_size_tbl = df_size.merge(rep_size, on="사이즈", how="left")
        df_size_tbl["대표 제품"] = df_size_tbl["대표 제품"].fillna("정보없음")

        _render_bar_and_table(
            df_chart=df_size,
            df_tbl=df_size_tbl,
            x="사이즈",
            y="이벤트수",
            hover_pct_col="비중",
            table_cols=["사이즈", "이벤트수", "비중", "대표 제품"],
            table_height=320,
            x_order=CFG["SIZE_LABEL"],
            key="dist__size"
        )

        # ──────────────────────────────────
        # 5-3) 옵션조합 분포도 (동적 비교)
        # ──────────────────────────────────
        st.markdown(" ")
        st.markdown("<h6 style='margin:0'>옵션조합 분포도</h6>", unsafe_allow_html=True)

        if "var_blocks" not in st.session_state: st.session_state["var_blocks"] = 1
        if "var_limit_hit" not in st.session_state: st.session_state["var_limit_hit"] = False

        hL, hR = st.columns([6, 0.4], vertical_alignment="center")

        with hL:
            with st.popover("🤔 옵션조합 검색 방법"):
                st.markdown("""
            - **단일 입력**  
            입력한 단어를 *포함하는* 옵션을 찾습니다.  
            예) `익스클루시브`

            - **OR (`|`)**  
            입력한 여러 단어 중 *하나라도 포함하는* 옵션을 찾습니다.  
            예) `프라임|프레스티지` : 프라임 또는 프레스를 구매하는 비중이 궁금해  

            - **AND (`&`)**  
            입력한 *모든 단어가 포함되는* 옵션을 찾습니다.   
            예) `익스클루시브&토퍼` : 익스클을 토퍼와 함께 구매하는 비중이 궁금해  
            """)

        with hR:
            cA, cR = st.columns([1, 1], gap="small")
            with cA:
                if st.button("＋", key="var_add"):
                    if st.session_state["var_blocks"] < 4: st.session_state["var_blocks"] += 1
                    else: st.session_state["var_limit_hit"] = True
            with cR:
                if st.button("↺", key="var_reset"):
                    st.session_state["var_blocks"] = 1
                    st.session_state["var_limit_hit"] = False

        HOLE, RED, GRAY = 0.58, "#FF4B4B", "#E5E7EB"
        s_all = df_f["items__item_variant"]
        n_all = int(len(df_f))

        for i in range(1, st.session_state["var_blocks"] + 1):
            q = st.text_input(
                f"검색 {i}",
                value="",
                placeholder="[🤔 옵션조합 검색 방법] 을 참고하여, 텍스트나 조건식을 입력하세요.",
                key=f"var_q_{i}"
            ).strip()

            m, _mode = build_match_mask(s_all, q)
            n_match, n_other = int(m.sum()), int(n_all - int(m.sum()))

            df_pie = (
                pd.DataFrame({"구분": ["검색어 매칭", "비매칭"], "이벤트수": [n_match, n_other]})
                if q else
                pd.DataFrame({"구분": ["검색어 미입력"], "이벤트수": [1]})
            )

            fig_pie = px.pie(df_pie, names="구분", values="이벤트수", hole=HOLE)
            fig_pie.update_traces(
                sort=False,
                direction="clockwise",
                rotation=0,
                marker=dict(colors=([RED, GRAY] if q else [GRAY])),
                hovertemplate="%{label}<br>%{value:,} (%{percent:.1%})<extra></extra>",
                textinfo=("none" if not q else "percent")
            )
            fig_pie.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10), showlegend=False)

            cL, cR = st.columns([3, 7], vertical_alignment="top")
            with cL:
                st.plotly_chart(fig_pie, use_container_width=True, key=f"var_pie_{i}")
            with cR:
                if q:
                    df_tbl = (
                        df_f.loc[m]
                            .groupby("items__item_variant", dropna=False).size()
                            .reset_index(name="이벤트수")
                            .rename(columns={"items__item_variant": "옵션조합"})
                            .sort_values(["이벤트수", "옵션조합"], ascending=[False, True])
                            .reset_index(drop=True)
                    )
                    tot = int(df_tbl["이벤트수"].sum())
                    df_tbl["비중 (검색결과내)"] = (df_tbl["이벤트수"] / max(1, tot) * 100).round(1).astype(str) + "%"
                    st.dataframe(df_tbl[["옵션조합", "이벤트수", "비중 (검색결과내)"]], hide_index=True, row_height=30, use_container_width=True, height=320)
                else:
                    df_tbl = (
                        df_f.groupby("items__item_variant", dropna=False).size()
                            .reset_index(name="이벤트수")
                            .rename(columns={"items__item_variant": "옵션조합"})
                            .sort_values(["이벤트수", "옵션조합"], ascending=[False, True])
                            .reset_index(drop=True)
                    )
                    tot = int(df_tbl["이벤트수"].sum())
                    df_tbl["비중"] = (df_tbl["이벤트수"] / max(1, tot) * 100).round(1).astype(str) + "%"
                    st.dataframe(df_tbl[["옵션조합", "이벤트수", "비중"]], hide_index=True, row_height=30, use_container_width=True, height=320)

            st.markdown(" ")
            if i == 4 and st.session_state.get("var_limit_hit"):
                st.warning("옵션조합 비교는 최대 4개까지 가능합니다."); st.session_state["var_limit_hit"] = False

if __name__ == "__main__":
    main()

