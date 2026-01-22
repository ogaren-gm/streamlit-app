# 2026-01-13 ver. (REFAC -> ui_common 적용)

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sys
import plotly.express as px

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery

import modules.style
importlib.reload(sys.modules["modules.style"])
from modules.style import style_format  # (이 파일에서 직접 쓰진 않지만 유지)

# ✅ ui_common: 모듈 import -> reload
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
    "BRAND_ORDER": ["슬립퍼", "누어"],           # 브랜드 고정 순서
    "HIER_PRI": ["매트리스", "프레임", "부자재"],  # 중분류 우선순위
    "TOPK_PATH_OPTS": [7, 10, 15, 20],
    "TOPK_CAT_OPTS": [5, 7, 10, 15, 20],
    "PATH_DIM_OPTS": ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"],
    "PATH_DIM_DEFAULT_IDX": 0,
    "SIZE_LABEL" : ["MS","S","SS","Q","K","LK","EK","S/SS","D/Q","Q/K","D/Q/K"],
    "SIZE_LABEL_MATCH" : {"01":"MS","02":"S","03":"SS","04":"Q","05":"K","06":"LK","07":"EK","31":"S/SS","32":"D/Q","33":"Q/K","34":"D/Q/K"},
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
    def load_data(cs: str, ce: str) -> tuple[pd.DataFrame, str | datetime]:
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df = bq.get_data("tb_sleeper_e_cart")
        last_updated_time = df["event_date"].max()

        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d", errors="coerce")
        if "event_name" in df.columns:
            df = df[df["event_name"] == "add_to_cart"].copy()

        def _safe_str_col(colname: str) -> pd.Series:
            if colname in df.columns:
                s = df[colname]
            else:
                s = pd.Series([""] * len(df), index=df.index)
            s = s.astype(str).replace("nan", "").fillna("").str.strip()
            return s

        # 유입 경로 추가
        df["_source"] = _safe_str_col("collected_traffic_source__manual_source").replace("", "(not set)")
        df["_medium"] = _safe_str_col("collected_traffic_source__manual_medium").replace("", "(not set)")
        df["_campaign"] = _safe_str_col("collected_traffic_source__manual_campaign_name").replace("", "(not set)")
        df["_content"] = _safe_str_col("collected_traffic_source__manual_content").replace("", "(not set)")
        df["_sourceMedium"] = df["_source"] + " / " + df["_medium"]

        return df, last_updated_time

    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df, last_updated_time = load_data(cs, ce_exclusive)

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
        if isinstance(last_updated_time, str):
            latest_dt = datetime.strptime(last_updated_time, "%Y%m%d")
        else:
            latest_dt = last_updated_time
        latest_date = latest_dt.date() if hasattr(latest_dt, "date") else datetime.now().date()

        now_kst = datetime.now(ZoneInfo(CFG["TZ"]))
        today_kst = now_kst.date()
        delta_days = (today_kst - latest_date).days
        hm_ref = now_kst.hour * 100 + now_kst.minute

        msg = "집계 예정 (AM 08:50 / PM 15:35)"
        sub_bg, sub_bd, sub_fg = "#f8fafc", "#e2e8f0", "#475569"

        if delta_days >= 2:
            msg = "업데이트가 지연되고 있습니다"
            sub_bg, sub_bd, sub_fg = "#fef2f2", "#fee2e2", "#b91c1c"
        elif delta_days == 1:
            if hm_ref >= CFG["HEADER_UPDATE_PM"]:
                msg = "2차 업데이트 완료 (PM 15:35)"
                sub_bg, sub_bd, sub_fg = "#fff7ed", "#fdba74", "#c2410c"
            elif hm_ref >= CFG["HEADER_UPDATE_AM"]:
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
    # 1) 장바구니 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>장바구니 </span>추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ장바구니 담기의 증감 추이를 확인합니다.")

    with st.popover("🤔 유저 VS 세션 VS 이벤트 차이점"):
        st.markdown("""
                    - **유저수** (user_pseudo_id) : 고유 사람수  
                    - **세션수** (pseudo_session_id) : 방문 단위수  
                    - **이벤트수** (add_to_cart) : 방문 안에서 발생한 이벤트 총 횟수  
                    - 사람 A가 1월 1일 오전에 시그니처를 조회 후 이탈, 오후에 시그니처와 허쉬를 재조회했다면,  
                      1월 1일의 **유저수**는 1, **세션수**는 2, **이벤트수**는 3 입니다.
                    - 유저수 ≤ 세션수 ≤ 이벤트수 입니다.
                    """)

    with st.expander("Filter", expanded=False):
        r0_1, r0_2 = st.columns([1.3, 2.7], vertical_alignment="bottom")
        with r0_1:
            mode_all = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_all")
        with r0_2:
            metric_map = {"유저수": "add_to_cart_users", "세션수": "add_to_cart_sessions", "이벤트수": "add_to_cart_events"}
            sel_metrics = st.pills(
                "집계 단위",
                list(metric_map.keys()),
                selection_mode="multi",
                default=list(metric_map.keys()),
                key="sel_metrics_all"
            ) or list(metric_map.keys())

    base = ui.add_period_columns(df, "event_date", mode_all)

    users = base.groupby("_period", dropna=False)["user_pseudo_id"].nunique().reset_index(name="add_to_cart_users")
    sessions = base.groupby("_period", dropna=False)["pseudo_session_id"].nunique().reset_index(name="add_to_cart_sessions")
    events = base.groupby("_period", dropna=False).size().reset_index(name="add_to_cart_events")
    period_dt = base.groupby("_period", dropna=False)["_period_dt"].min().reset_index(name="_period_dt")

    df_all = (
        users.merge(sessions, on="_period", how="outer")
             .merge(events, on="_period", how="outer")
             .merge(period_dt, on="_period", how="left")
             .rename(columns={"_period": "날짜"})
             .sort_values("_period_dt")
             .reset_index(drop=True)
    )

    # 파생지표
    df_all["sessions_per_user"] = (df_all["add_to_cart_sessions"] / df_all["add_to_cart_users"]).replace([np.inf, -np.inf], np.nan)   # 유저당 세션수 (Sessions per User)
    df_all["events_per_session"] = (df_all["add_to_cart_events"] / df_all["add_to_cart_sessions"]).replace([np.inf, -np.inf], np.nan) # 세션당 이벤트수 (Events per Session)


    # ✅ 그래프 범례명 한글로 고정
    plot_rename = {
        "add_to_cart_users": "유저수",
        "add_to_cart_sessions": "세션수",
        "add_to_cart_events": "이벤트수",
    }
    df_plot = df_all.rename(columns=plot_rename).copy()

    ORDER = ["유저수", "세션수", "이벤트수"]
    y_cols = [m for m in ORDER if m in (sel_metrics or [])]
    y_cols = y_cols or ["유저수"]


    x_col = "_period_dt" if mode_all == "일별" else "날짜"
    ui.render_line_graph(df_plot, x=x_col, y=y_cols, height=360, title=None)


    # ✅ (표) 이 부분은 “지표 고정 순서 + 표시 포맷”이라 공통화 대상 아님 → 그대로 유지
    rows = [
        ("유저수", "add_to_cart_users", "int"),
        ("세션수", "add_to_cart_sessions", "int"),
        ("이벤트수", "add_to_cart_events", "int"),
        ("SPU (세션수/유저수)", "sessions_per_user", "float2"),
        ("EPS (이벤트수/세션수)", "events_per_session", "float2"),
    ]

    dates = df_all["날짜"].astype(str).tolist()
    pv = pd.DataFrame({"지표": [r[0] for r in rows]})
    for dt in dates:
        pv[dt] = ""

    m = df_all.set_index("날짜").to_dict(orient="index")

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

    for i, (_, col, kind) in enumerate(rows):
        for dt in dates:
            pv.at[i, dt] = _fmt(m.get(dt, {}).get(col, np.nan), kind)

    pv = pv[["지표", *ui.sort_period_labels(dates)]]
    st.dataframe(pv, row_height=30, hide_index=True, use_container_width=True)

    # ──────────────────────────────────
    # 2) 장바구니 유입
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>장바구니 </span>유입</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ장바구니 담기가 발생한 세션의 유입경로를 확인합니다.")

    with st.expander("Filter", expanded=True):
        r1, r2, r3 = st.columns([3, 3, 3], vertical_alignment="bottom")
        with r1:
            mode_path = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_path")
        with r2:
            path_dim = st.selectbox("유입 기준", CFG["PATH_DIM_OPTS"], index=CFG["PATH_DIM_DEFAULT_IDX"], key="path_dim")
        with r3:
            topk_path = st.selectbox("표시 Top K", CFG["TOPK_PATH_OPTS"], index=1, key="topk_path")

    tmp = ui.add_period_columns(df, "event_date", mode_path)

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
           .rename(columns={"_period": "기간", "_path2": "유입경로"})
    )

    # ✅ (추가) 기간 dt 붙이기 (shading 안정화용)
    dt_map = tmp[["_period", "_period_dt"]].drop_duplicates().rename(columns={"_period": "기간"})
    agg_path = agg_path.merge(dt_map, on="기간", how="left")

    # ✅ (수정) 일별이면 dt축으로 render
    x_col_path = "_period_dt" if mode_path == "일별" else "기간"
    ui.render_stack_graph(agg_path, x=x_col_path, y="sessions", color="유입경로", height=360, opacity=0.6, title=None, show_value_in_hover=True, key=None)

    pv2 = ui.build_pivot_table(agg_path, index_col="유입경로", col_col="기간", val_col="sessions")
    ui.render_table(pv2, index_col="유입경로", decimals=0)

    # ──────────────────────────────────
    # 3) 품목별 장바구니 추이
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
                    - 소분류 중 부자재의 '기타' 외 세부 구성은 변경될 수 있으며, 필요 시 별도 문의 바랍니다.  
                    """)

    tab1, tab2 = st.tabs(["커스텀", "[고정뷰 예시] 슬립퍼 프레임별"])

    with tab1:
        with st.expander("Filter", expanded=True):
            c1, c2, c3 = st.columns([1.4, 2.6, 2.0], vertical_alignment="bottom")
            with c1:
                mode_cat = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_cat_tab1")
            with c2:
                view_level = st.radio("품목 뎁스", ["브랜드", "중분류", "소분류", "제품"], index=2, horizontal=True, key="view_level_tab1")
            with c3:
                topk_cat = st.selectbox("표시 Top K", CFG["TOPK_CAT_OPTS"], index=2, key="topk_cat_tab1")

            base2 = df
            brand_order = CFG["BRAND_ORDER"]
            brands_exist = [b for b in brand_order if b in base2["product_cat_a"].dropna().astype(str).unique().tolist()]
            sel_a = brands_exist[:]

            need_ab = view_level in ["중분류", "소분류", "제품"]
            need_c  = view_level in ["소분류", "제품"]

            def _hier_rank(text: str) -> int:
                t = (text or "").strip()
                for i, kw in enumerate(CFG["HIER_PRI"]):
                    if kw in t:
                        return i
                return 99

            def _sort_b_opts(tb: pd.DataFrame) -> list[str]:
                b = tb["product_cat_b"].dropna().astype(str).str.strip().replace("nan", "")
                b = [x for x in b.unique().tolist() if x != ""]
                return sorted(b, key=lambda x: (_hier_rank(x), x))

            def _sort_c_opts(tb: pd.DataFrame) -> list[str]:
                t = tb.copy()
                t["product_cat_b"] = t["product_cat_b"].astype(str).str.strip()
                t["product_cat_c"] = t["product_cat_c"].astype(str).str.strip()
                t = t[(t["product_cat_c"] != "") & (t["product_cat_c"].str.lower() != "nan")].copy()
                if t.empty:
                    return []

                tmp = (
                    t.groupby(["product_cat_c"], dropna=False)["product_cat_b"]
                     .apply(lambda s: sorted(list(dict.fromkeys([x for x in s.tolist() if x and x.lower() != "nan"])) ))
                     .reset_index(name="_parents")
                )

                def _key(row):
                    c = row["product_cat_c"]
                    parents = row["_parents"] or []
                    if parents:
                        p0 = sorted(parents, key=lambda x: (_hier_rank(x), x))[0]
                        return (_hier_rank(p0), p0, c)
                    return (99, "", c)

                tmp["_k"] = tmp.apply(_key, axis=1)
                tmp = tmp.sort_values("_k").reset_index(drop=True)
                return tmp["product_cat_c"].tolist()

            sel_b_by_brand, sel_c_by_brand = {}, {}
            if view_level != "브랜드":
                for b in brand_order:
                    if b not in brands_exist:
                        continue
                    tb = base2[base2["product_cat_a"] == b]
                    if tb.empty:
                        continue

                    cc0, cc1, cc2 = st.columns([1, 2, 8], vertical_alignment="center")
                    with cc0:
                        st.markdown(
                            f"<div style='font-size:13px;font-weight:700;line-height:1;white-space:nowrap;'>{b}</div>",
                            unsafe_allow_html=True
                        )

                    if need_ab:
                        b_opts = _sort_b_opts(tb)
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
                        tb2 = tb.copy()
                        picked_b = (sel_b_by_brand.get(b) or [])
                        tb2 = tb2[tb2["product_cat_b"].isin(picked_b)].copy() if len(picked_b) > 0 else tb2.iloc[0:0].copy()
                        c_opts = _sort_c_opts(tb2)
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
                tmpP = base2[base2["product_cat_a"].isin(brands_exist)].copy()
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
                prod_candidates = ui.get_topk_values(tmpP["product_name"], max(50, 200)) if not tmpP.empty else []

                sel_products = st.multiselect(
                    "제품 선택 (미선택시 선택된 Top K 모두 표시)",
                    options=prod_candidates,
                    default=[],
                    placeholder="전체",
                    key="sel_products_tab1"
                )

        baseP = ui.add_period_columns(df, "event_date", mode_cat)
        baseP = baseP[baseP["product_cat_a"].isin(sel_a)].copy()

        for brand in sel_a:
            st.markdown(f"<h6 style='margin:0'>{brand}</h6>", unsafe_allow_html=True)

            tb = baseP[baseP["product_cat_a"] == brand].copy()
            if tb.empty:
                st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                st.markdown(" ")
                continue

            if view_level in ["중분류", "소분류", "제품"] and need_ab:
                picked_b = sel_b_by_brand.get(brand)
                if picked_b is not None:
                    if len(picked_b) == 0:
                        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                        st.markdown(" ")
                        continue
                    tb = tb[tb["product_cat_b"].isin(picked_b)]

            if view_level in ["소분류", "제품"] and need_c:
                picked_c = sel_c_by_brand.get(brand)
                if picked_c is not None:
                    if len(picked_c) == 0:
                        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                        st.markdown(" ")
                        continue
                    tb = tb[tb["product_cat_c"].isin(picked_c)]

            if view_level == "브랜드":
                dim = "product_cat_a"
            elif view_level == "중분류":
                dim = "product_cat_b"
            elif view_level == "소분류":
                dim = "product_cat_c"
            else:
                dim = "product_name"

            if view_level == "제품" and sel_products:
                tb = tb[tb["product_name"].isin(sel_products)].copy()

            if tb.empty:
                st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                st.markdown(" ")
                continue

            if view_level in ["중분류", "소분류", "제품"]:
                if not (view_level == "제품" and sel_products):
                    top_vals = ui.get_topk_values(tb[dim], topk_cat)
                    tb[dim] = tb[dim].where(tb[dim].isin(top_vals), "기타")

            agg = (
                tb.groupby(["_period", dim], dropna=False)["pseudo_session_id"]
                  .nunique()
                  .reset_index(name="sessions")
                  .rename(columns={"_period": "기간", dim: "구분"})
            )

            if agg.empty:
                st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                st.markdown(" ")
                continue

            # ✅ (추가) 기간 dt 매핑 붙이기 (주말 shading 안정화)
            dt_map = tb[["_period", "_period_dt"]].drop_duplicates().rename(columns={"_period": "기간"})
            agg = agg.merge(dt_map, on="기간", how="left")

            # ✅ (수정) 일별이면 dt축으로 렌더
            x_col_cat = "_period_dt" if mode_cat == "일별" else "기간"
            ui.render_stack_graph(agg, x=x_col_cat, y="sessions", color="구분", height=340, opacity=0.6, title=None, show_value_in_hover=True, key=f"cat_stack__{brand}")

            pv3 = ui.build_pivot_table(agg, index_col="구분", col_col="기간", val_col="sessions")
            ui.render_table(pv3, index_col="구분", decimals=0)

    with tab2:
        with st.expander("Filter", expanded=True):
            mode_cat3 = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_cat_tab3")
            topk_cat3 = st.selectbox("표시 Top K", CFG["TOPK_CAT_OPTS"], index=2, key="topk_cat_tab3")

        baseX = ui.add_period_columns(df, "event_date", mode_cat3)
        tb = baseX[
            (baseX["product_cat_a"] == "슬립퍼") &
            (baseX["product_cat_b"].astype(str) == "프레임") &
            (baseX["product_cat_c"].isin(["원목", "패브릭", "호텔침대"]))
        ].copy()

        st.markdown("<h6 style='margin:0'>슬립퍼</h6>", unsafe_allow_html=True)
        if tb.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        else:
            dim = "product_cat_c"
            top_vals = ui.get_topk_values(tb[dim], topk_cat3)
            tb[dim] = tb[dim].where(tb[dim].isin(top_vals), "기타")

            agg = (
                tb.groupby(["_period", dim], dropna=False)["pseudo_session_id"]
                  .nunique()
                  .reset_index(name="sessions")
                  .rename(columns={"_period": "기간", dim: "구분"})
            )

            dt_map = tb[["_period", "_period_dt"]].drop_duplicates().rename(columns={"_period": "기간"})
            agg = agg.merge(dt_map, on="기간", how="left")

            x_col_cat2 = "_period_dt" if mode_cat3 == "일별" else "기간"
            ui.render_stack_graph(agg, x=x_col_cat2, y="sessions", color="구분", height=340, opacity=0.6, title=None, show_value_in_hover=True, key="cat_tab2_stack")

            pv3b = ui.build_pivot_table(agg, index_col="구분", col_col="기간", val_col="sessions")
            ui.render_table(pv3b, index_col="구분", decimals=0)

    # ──────────────────────────────────
    # 4) 품목별 장바구니 유입
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>품목별 </span>유입</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ품목별로 장바구니 담기가 발생한 세션의 유입경로를 확인합니다.", unsafe_allow_html=True)

    with st.expander("Filter", expanded=True):
        r1, r2, r3, r4 = st.columns([1.4, 2.6, 2.0, 2.0], vertical_alignment="bottom")
        with r1:
            mode_prod_path = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_prod_path")
        with r2:
            view_level_pp = st.radio("품목 뎁스", ["브랜드", "중분류", "소분류", "제품"], index=3, horizontal=True, key="view_level_prod_path")
        with r3:
            topk_path_pp = st.selectbox("표시 Top K", CFG["TOPK_PATH_OPTS"], index=1, key="topk_path_pp")
        with r4:
            path_dim_pp = st.selectbox("유입 기준", CFG["PATH_DIM_OPTS"], index=CFG["PATH_DIM_DEFAULT_IDX"], key="path_dim_prod_path")

        base4 = df
        brand_order = CFG["BRAND_ORDER"]
        brands_exist = [b for b in brand_order if b in base4["product_cat_a"].dropna().astype(str).unique().tolist()]
        sel_a_pp = brands_exist[:]

        need_ab = view_level_pp in ["중분류", "소분류", "제품"]
        need_c  = view_level_pp in ["소분류", "제품"]

        def _hier_rank__pp(text: str) -> int:
            t = (text or "").strip()
            for i, kw in enumerate(CFG["HIER_PRI"]):
                if kw in t:
                    return i
            return 99

        def _sort_b_opts__pp(tb: pd.DataFrame) -> list[str]:
            b = tb["product_cat_b"].dropna().astype(str).str.strip().replace("nan", "")
            b = [x for x in b.unique().tolist() if x != ""]
            return sorted(b, key=lambda x: (_hier_rank__pp(x), x))

        def _sort_c_opts__pp(tb: pd.DataFrame) -> list[str]:
            t = tb.copy()
            t["product_cat_b"] = t["product_cat_b"].astype(str).str.strip()
            t["product_cat_c"] = t["product_cat_c"].astype(str).str.strip()
            t = t[(t["product_cat_c"] != "") & (t["product_cat_c"].str.lower() != "nan")].copy()
            if t.empty:
                return []

            tmp = (
                t.groupby(["product_cat_c"], dropna=False)["product_cat_b"]
                 .apply(lambda s: sorted(list(dict.fromkeys([x for x in s.tolist() if x and x.lower() != "nan"])) ))
                 .reset_index(name="_parents")
            )

            def _key(row):
                c = row["product_cat_c"]
                parents = row["_parents"] or []
                if parents:
                    p0 = sorted(parents, key=lambda x: (_hier_rank__pp(x), x))[0]
                    return (_hier_rank__pp(p0), p0, c)
                return (99, "", c)

            tmp["_k"] = tmp.apply(_key, axis=1)
            tmp = tmp.sort_values("_k").reset_index(drop=True)
            return tmp["product_cat_c"].tolist()

        sel_b_by_brand, sel_c_by_brand = {}, {}
        if view_level_pp != "브랜드":
            for b in brand_order:
                if b not in brands_exist:
                    continue
                tb = base4[base4["product_cat_a"] == b]
                if tb.empty:
                    continue

                cc0, cc1, cc2 = st.columns([1, 2, 8], vertical_alignment="center")
                with cc0:
                    st.markdown(
                        f"<div style='font-size:13px;font-weight:700;line-height:1;white-space:nowrap;'>{b}</div>",
                        unsafe_allow_html=True
                    )

                if need_ab:
                    b_opts = _sort_b_opts__pp(tb)
                    with cc1:
                        sel_b_by_brand[b] = st.pills(
                            " ", b_opts, selection_mode="multi", default=b_opts, key=f"prodpath__ab__{b}"
                        ) or []
                else:
                    sel_b_by_brand[b] = None
                    with cc1:
                        st.markdown(" ")

                if need_c:
                    tb2 = tb.copy()
                    picked_b = (sel_b_by_brand.get(b) or [])
                    tb2 = tb2[tb2["product_cat_b"].isin(picked_b)].copy() if len(picked_b) > 0 else tb2.iloc[0:0].copy()

                    c_opts = _sort_c_opts__pp(tb2)
                    with cc2:
                        sel_c_by_brand[b] = st.pills(
                            " ", c_opts, selection_mode="multi", default=c_opts, key=f"prodpath__c__{b}"
                        ) or []
                else:
                    sel_c_by_brand[b] = None
                    with cc2:
                        st.markdown(" ")

        sel_products_pp = None
        if view_level_pp == "제품":
            tmpP = base4[base4["product_cat_a"].isin(brands_exist)].copy()
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

            prod_candidates = ui.get_topk_values(tmpP["product_name"], max(50, 200)) if not tmpP.empty else []
            sel_products_pp = st.multiselect(
                "제품 선택 (미선택시 선택된 품목군 모두 표시)",
                options=prod_candidates,
                default=[],
                placeholder="전체",
                key="sel_products_pp_prodpath"
            )

    if not sel_a_pp:
        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        return

    for brand in sel_a_pp:
        st.markdown(f"<h6 style='margin:0'>{brand}</h6>", unsafe_allow_html=True)
        df_b = df[df["product_cat_a"] == brand].copy()
        if df_b.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
            st.markdown(" ")
            continue

        if view_level_pp in ["중분류", "소분류", "제품"] and need_ab:
            picked_b = sel_b_by_brand.get(brand)
            if picked_b is not None:
                if len(picked_b) == 0:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue
                df_b = df_b[df_b["product_cat_b"].isin(picked_b)]

        if view_level_pp in ["소분류", "제품"] and need_c:
            picked_c = sel_c_by_brand.get(brand)
            if picked_c is not None:
                if len(picked_c) == 0:
                    st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                    st.markdown(" ")
                    continue
                df_b = df_b[df_b["product_cat_c"].isin(picked_c)]

        if view_level_pp == "제품" and sel_products_pp:
            df_b = df_b[df_b["product_name"].isin(sel_products_pp)]

        if df_b.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
            st.markdown(" ")
            continue

        df_b = ui.add_period_columns(df_b, "event_date", mode_prod_path)

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
                .rename(columns={"_period": "기간", "_path2": "유입경로"})
        )

        if agg_path_brand.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
            st.markdown(" ")
            continue
        
        # ✅ (추가) 기간 dt 붙이기 (shading 안정화용)
        dt_map = df_b[["_period", "_period_dt"]].drop_duplicates().rename(columns={"_period": "기간"})
        agg_path_brand = agg_path_brand.merge(dt_map, on="기간", how="left")

        # ✅ (수정) 일별이면 dt축으로 render
        x_col_prodpath = "_period_dt" if mode_prod_path == "일별" else "기간"
        ui.render_stack_graph(agg_path_brand, x=x_col_prodpath, y="sessions", color="유입경로", height=360, opacity=0.6, title=None, show_value_in_hover=True, key=f"prodpath__{brand}")

        pv4 = ui.build_pivot_table(agg_path_brand, index_col="유입경로", col_col="기간", val_col="sessions")
        ui.render_table(pv4, index_col="유입경로", decimals=0)
        

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

        # 데이터프레임 생성 (차트)
        df_bucket = (
            df_f.groupby("price_bucket", dropna=False).size()
                .reindex(v_lbl, fill_value=0)  # 라벨 순서 고정
                .reset_index(name="이벤트수")
                .rename(columns={"price_bucket": "가격대"})
        )
        total_cnt = int(df_bucket["이벤트수"].sum())
        df_bucket["비중"] = (df_bucket["이벤트수"] / max(1, total_cnt)).fillna(0)

        # 데이터프레임 생성 (표)
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
        
        cL, _p, cR = st.columns([6, 0.2, 4], vertical_alignment="top")
        with cL:
            fig_price = px.bar(df_bucket, x="가격대", y="이벤트수", hover_data={"비중": ":.1%"} )
            fig_price.update_traces(opacity=0.60)
            fig_price.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=40))
            st.plotly_chart(fig_price, use_container_width=True)
        with _p:
            pass
        with cR:
            df_bucket_tbl["비중"] = (df_bucket_tbl["비중"] * 100).round(1).astype(str) + "%"
            st.dataframe(df_bucket_tbl[["가격대", "이벤트수", "비중", "대표 제품"]], hide_index=True, row_height=30, use_container_width=True, height=320)


        # ──────────────────────────────────
        # 5-2) 사이즈 분포도
        # ──────────────────────────────────
        st.markdown(" ")
        st.markdown("<h6 style='margin:0'>사이즈 분포도</h6>", unsafe_allow_html=True)

        df_sz = df_f.assign(_vs=df_f["variant_size_code"].astype(str).str.strip().str.zfill(2).map(CFG["SIZE_LABEL_MATCH"])).loc[lambda x: x["_vs"].isin(CFG["SIZE_LABEL"])]

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
        
        cL, _p, cR = st.columns([6, 0.2, 4], vertical_alignment="top")
        with cL:
            fig_size = px.bar(df_size, x="사이즈", y="이벤트수", hover_data={"비중": ":.1%"} )
            fig_size.update_traces(opacity=0.60)
            fig_size.update_xaxes(type="category", categoryorder="array", categoryarray=CFG["SIZE_LABEL"], tickmode="array", tickvals=CFG["SIZE_LABEL"])
            fig_size.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=40))
            st.plotly_chart(fig_size, use_container_width=True)
        with _p:
            pass
        with cR:
            df_size_tbl["비중"] = (df_size_tbl["비중"] * 100).round(1).astype(str) + "%"
            st.dataframe(df_size_tbl[["사이즈", "이벤트수", "비중", "대표 제품"]], hide_index=True, row_height=30, use_container_width=True, height=320)


        # ──────────────────────────────────
        # 5-3) 옵션조합 분포도 (동적 비교)
        # ──────────────────────────────────
        st.markdown(" ")
        st.markdown("<h6 style='margin:0'>옵션조합 분포도</h6>", unsafe_allow_html=True)

        if "var_blocks" not in st.session_state: st.session_state["var_blocks"] = 1
        if "var_limit_hit" not in st.session_state: st.session_state["var_limit_hit"] = False


        hL, hR = st.columns([6,0.4], vertical_alignment="center")

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
            cA, cR= st.columns([1,1], gap="small")
            with cA:
                if st.button("＋", key="var_add"):
                    if st.session_state["var_blocks"] < 4: st.session_state["var_blocks"] += 1
                    else: st.session_state["var_limit_hit"] = True
            with cR:
                if st.button("↺", key="var_reset"):
                    st.session_state["var_blocks"] = 1
                    st.session_state["var_limit_hit"] = False


        HOLE, RED, GRAY = 0.58, "#FF4B4B", "#E5E7EB"
        s_all = df_f["items__item_variant"].astype(str)
        n_all = int(len(df_f))

        def _build_match_mask(s: pd.Series, q: str):
            q = (q or "").strip()
            if not q: return pd.Series(False, index=s.index), "미입력"
            if "&" in q:
                parts = [p.strip() for p in q.split("&") if p.strip()]
                m = pd.Series(True, index=s.index)
                for p in parts: m &= s.str.contains(p, regex=False, na=False)
                return m, "AND"
            if "|" in q:
                try: return s.str.contains(q, regex=True, na=False), "OR"
                except Exception:
                    parts = [p.strip() for p in q.split("|") if p.strip()]
                    m = pd.Series(False, index=s.index)
                    for p in parts: m |= s.str.contains(p, regex=False, na=False)
                    return m, "OR"
            return s.str.contains(q, regex=False, na=False), "부분일치"

        for i in range(1, st.session_state["var_blocks"] + 1):
            q = st.text_input(f"검색 {i}", value="", placeholder="[🤔 옵션조합 검색 방법] 을 참고하여, 텍스트나 조건식을 입력하세요.", key=f"var_q_{i}").strip()
            m, _mode = _build_match_mask(s_all, q)
            n_match, n_other = int(m.sum()), int(n_all - int(m.sum()))
            df_pie = pd.DataFrame({"구분": ["검색어 매칭", "비매칭"], "이벤트수": [n_match, n_other]}) if q else pd.DataFrame({"구분": ["검색어 미입력"], "이벤트수": [1]})

            fig_pie = px.pie(df_pie, names="구분", values="이벤트수", hole=HOLE)
            fig_pie.update_traces(sort=False, direction="clockwise", rotation=0, marker=dict(colors=([RED, GRAY] if q else [GRAY])), hovertemplate="%{label}<br>%{value:,} (%{percent:.1%})<extra></extra>", textinfo=("none" if not q else "percent"))
            fig_pie.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10), showlegend=False)

            cL, cR = st.columns([3, 7], vertical_alignment="top")
            with cL:
                st.plotly_chart(fig_pie, use_container_width=True, key=f"var_pie_{i}")
            with cR:
                if q:
                    df_tbl = (df_f.loc[m].groupby("items__item_variant", dropna=False).size().reset_index(name="이벤트수").rename(columns={"items__item_variant": "옵션조합"}).sort_values(["이벤트수", "옵션조합"], ascending=[False, True]).reset_index(drop=True))
                    tot = int(df_tbl["이벤트수"].sum())
                    df_tbl["비중 (검색결과내)"] = (df_tbl["이벤트수"] / max(1, tot) * 100).round(1).astype(str) + "%"
                    st.dataframe(df_tbl[["옵션조합", "이벤트수", "비중 (검색결과내)"]], hide_index=True, row_height=30, use_container_width=True, height=320)
                else:
                    df_tbl = (df_f.groupby("items__item_variant", dropna=False).size().reset_index(name="이벤트수").rename(columns={"items__item_variant": "옵션조합"}).sort_values(["이벤트수", "옵션조합"], ascending=[False, True]).reset_index(drop=True))
                    tot = int(df_tbl["이벤트수"].sum())
                    df_tbl["비중"] = (df_tbl["이벤트수"] / max(1, tot) * 100).round(1).astype(str) + "%"
                    st.dataframe(df_tbl[["옵션조합", "이벤트수", "비중"]], hide_index=True, row_height=30, use_container_width=True, height=320)
            
            st.markdown(" ")
            if i == 4 and st.session_state.get("var_limit_hit"):
                st.warning("옵션조합 비교는 최대 4개까지 가능합니다."); st.session_state["var_limit_hit"] = False


if __name__ == "__main__":
    main()

