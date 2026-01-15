# 2026-01-13 ver. (REFAC)

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
# CONFIG
# ──────────────────────────────────
CFG = {
    "TZ": "Asia/Seoul",
    "CACHE_TTL": 3600,
    "DEFAULT_LOOKBACK_DAYS": 14,          # 기본 기간: D-14 ~ D-1
    "HEADER_UPDATE_AM": 850,              # 1차 08:50
    "HEADER_UPDATE_PM": 1535,             # 2차 15:35
    "BRAND_ORDER": ["슬립퍼", "누어"],           # 브랜드 고정 순서
    "HIER_PRI": ["매트리스", "프레임", "부자재"],  # 중분류 우선순위
    "TOPK_PATH_OPTS": [7, 10, 15, 20],
    "TOPK_CAT_OPTS": [5, 7, 10, 15, 20],
    "PATH_DIM_OPTS": ["소스 / 매체", "소스", "매체", "캠페인", "컨텐츠"],
    "PATH_DIM_DEFAULT_IDX": 0,
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
# 공통 유틸 함수
# ──────────────────────────────────
def with_period(df: pd.DataFrame, date_col: str, mode: str) -> pd.DataFrame:
    # event_date를 기준으로 일별/주별 라벨(_period) 생성.
    # 일별: YYYY-MM-DD
    # 주별: YYYY-MM-DD ~ YYYY-MM-DD (월~일 범위)
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

def sort_period_cols(cols: list[str]) -> list[str]:
    # pivot table에서 날짜가 가로 컬럼이 되면 시간순 정렬 보장.
    return sorted(cols, key=lambda x: x.split(" ~ ")[0] if " ~ " in x else x)

def topk_values(s: pd.Series, k: int) -> list[str]:
    # 특정 차원(유입경로/카테고리/제품명 등)을 빈도 기준 TopK로 뽑아 리스트로 반환.
    # TopK 밖은 이후 “기타”로 묶는 로직에서 사용.
    vc = s.replace("", np.nan).dropna().value_counts()
    return vc.head(k).index.tolist()

def add_weekend_vrect_centered(fig, x_vals: pd.Series):
    # 주말 영역 음영(vrect)
    xs = pd.to_datetime(pd.Series(x_vals), errors="coerce").dropna().dt.floor("D").unique()
    for d in xs:
        d = pd.Timestamp(d).date()
        start = datetime.combine(d, datetime.min.time()) + timedelta(hours=12)
        end = start + timedelta(hours=24)
        if d.weekday() == 4:    # 토
            fig.add_vrect(x0=start, x1=end, fillcolor="blue", opacity=0.05, layer="below", line_width=0)
        elif d.weekday() == 5:  # 일
            fig.add_vrect(x0=start, x1=end, fillcolor="red", opacity=0.05, layer="below", line_width=0)


# ──────────────────────────────────
# 공통 렌더 함수
# ──────────────────────────────────
def render_line_chart(df: pd.DataFrame, x: str, y: list[str] | str, height: int = 360, title: str | None = None) -> None:
    # px.line로 라인차트 생성
    # 마커 포함
    # 주말 음영 추가
    # 범례 레이아웃 통일
    # x축 날짜 포맷 %m월 %d일
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

def render_stack_bar_share_hover(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str,
    height: int = 360,
    opacity: float = 0.6,
    title: str | None = None,
    show_value_in_hover: bool = False,
) -> None:
    # 누적막대 그래프
    # hover에 “항목명 + 비중(%)” 넣음 (_share_pct 계산)
    d = df.copy()
    d[y] = pd.to_numeric(d[y], errors="coerce").fillna(0)

    tot = d.groupby(x, dropna=False)[y].transform("sum").replace(0, np.nan)
    d["_share_pct"] = (d[y] / tot * 100).round(1).fillna(0)

    fig = px.bar(
        d,
        x=x, y=y, color=color,
        barmode="relative",
        labels={x: "", y: "", color: ""},
        title=title,
        custom_data=[color, "_share_pct", y]
    )
    fig.update_traces(marker_opacity=opacity)

    if show_value_in_hover:
        fig.update_traces(hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]}%<br>값: %{customdata[2]:,.0f}<extra></extra>")
    else:
        fig.update_traces(hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]}%<extra></extra>")

    add_weekend_vrect_centered(fig, d[x])
    fig.update_layout(
        height=height,
        legend_title_text="",
        xaxis_title=None,
        yaxis_title=None,
        bargap=0.5,
        bargroupgap=0.2,
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
        margin=dict(l=10, r=10, t=40, b=10),
        barmode="relative",
    )
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
    fig.update_xaxes(tickformat="%m월 %d일")
    st.plotly_chart(fig, use_container_width=True)

def build_pivot_table(long: pd.DataFrame, index_col: str, col_col: str, val_col: str) -> pd.DataFrame:
    # long 형태를 wide 형태로 변환
    # 날짜 컬럼 시간순 정렬
    # 기간 전체 합계 기준 내림차순 정렬(요구사항 유지)
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
    # st.dataframe 출력
    # style_format으로 숫자 포맷 강제 적용
    styled = style_format(df_wide, decimals_map={c: decimals for c in df_wide.columns if c != index_col})
    st.dataframe(styled, row_height=30, hide_index=True)

def render_stack_and_table(long: pd.DataFrame, x: str, y: str, color: str, index_col: str, height: int = 360, opacity: float = 0.6):
    # 그래프 먼저 -> 표 아래 배치 고정하는 함수. 굳이필요한가 싶긴함 ~
    render_stack_bar_share_hover(long, x=x, y=y, color=color, height=height, opacity=opacity)
    pv = build_pivot_table(long, index_col=index_col, col_col=x, val_col=y)
    render_table(pv, index_col=index_col, decimals=0)


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
        key="view07__date_range"
    )
    cs = start_date.strftime("%Y%m%d")
    ce_exclusive = (end_date + timedelta(days=1)).strftime("%Y%m%d")

    # ──────────────────────────────────
    # C) Data Load
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
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
    # 제목
    # 설명
    # 업데이트 상태
    st.subheader("GA PDP 대시보드 V2")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px; line-height:1.5;">
            <b>PDP 조회</b>에 대한 추이와 유입경로를
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
    # 1) PDP조회 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>PDP조회 </span>추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤPDP 조회량의 증감 추이를 확인합니다.")

    with st.popover("🤔 유저 VS 세션 VS 이벤트 차이점"):
        st.markdown("""
                    - **유저수** (user_pseudo_id) : 고유 사람수  
                    - **세션수** (pseudo_session_id) : 방문 단위수  
                    - **이벤트수** (view_item) : 방문 안에서 발생한 이벤트 총 횟수  
                    - 사람 A가 1월 1일 오전에 시그니처를 조회 후 이탈, 오후에 시그니처와 허쉬를 재조회했다면,  
                      1월 1일의 **유저수**는 1, **세션수**는 2, **이벤트수**는 3 입니다.
                    - 유저수 ≤ 세션수 ≤ 이벤트수 입니다.
                    """)

    with st.expander("Filter", expanded=False): # 닫아두기 
        r0_1, r0_2 = st.columns([1.3, 2.7], vertical_alignment="bottom")
        with r0_1:
            mode_all = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_all")
        with r0_2:
            metric_map = {"유저수": "view_item_users", "세션수": "view_item_sessions", "이벤트수": "view_item_events"}
            sel_metrics = st.pills(
                "집계 단위",
                list(metric_map.keys()),
                selection_mode="multi",
                default=list(metric_map.keys()),
                key="sel_metrics_all"
            ) or list(metric_map.keys())

    base = with_period(df, "event_date", mode_all)

    users = base.groupby("_period", dropna=False)["user_pseudo_id"].nunique().reset_index(name="view_item_users")
    sessions = base.groupby("_period", dropna=False)["pseudo_session_id"].nunique().reset_index(name="view_item_sessions")
    events = base.groupby("_period", dropna=False).size().reset_index(name="view_item_events")

    df_all = (
        users.merge(sessions, on="_period", how="outer")
             .merge(events, on="_period", how="outer")
             .rename(columns={"_period": "날짜"})
             .sort_values("날짜")
             .reset_index(drop=True)
    )
    df_all["events_per_session"] = (df_all["view_item_events"] / df_all["view_item_sessions"]).replace([np.inf, -np.inf], np.nan)
    df_all["events_per_user"] = (df_all["view_item_events"] / df_all["view_item_users"]).replace([np.inf, -np.inf], np.nan)

    y_cols = [metric_map[m] for m in sel_metrics if m in metric_map] or ["view_item_users"]
    render_line_chart(df_all, x="날짜", y=y_cols, height=360)

    rows = [
        ("유저수", "view_item_users", "int"),
        ("세션수", "view_item_sessions", "int"),
        ("이벤트수", "view_item_events", "int"),
        ("EPU (유저당 이벤트수)", "events_per_user", "float2"),
        ("EPS (유저당 세션수)", "events_per_session", "float2"),
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

    pv = pv[["지표", *sort_period_cols(dates)]]
    st.dataframe(pv, row_height=30, hide_index=True, use_container_width=True)

    # ──────────────────────────────────
    # 2) PDP조회 유입
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>PDP조회 </span>유입</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤPDP 조회가 발생한 세션의 유입경로를 확인합니다.")

    with st.expander("Filter", expanded=True):
        r1, r2, r3 = st.columns([3, 3, 3], vertical_alignment="bottom")
        with r1:
            mode_path = st.radio("기간 단위", ["일별", "주별"], horizontal=True, key="mode_path")
        with r2:
            path_dim = st.selectbox("유입 기준", CFG["PATH_DIM_OPTS"], index=CFG["PATH_DIM_DEFAULT_IDX"], key="path_dim")
        with r3:
            topk_path = st.selectbox("표시 Top K", CFG["TOPK_PATH_OPTS"], index=1, key="topk_path")

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

    render_stack_and_table(agg_path, x="날짜", y="sessions", color="유입경로", index_col="유입경로", height=360, opacity=0.6)

    # ──────────────────────────────────
    # 3) 품목별 PDP조회 추이
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>품목별 </span>추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ품목 뎁스별로 어떤 상품군이 PDP 조회량에 영향을 주는지 확인합니다.")

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
                prod_candidates = topk_values(tmpP["product_name"], max(50, 200)) if not tmpP.empty else []

                sel_products = st.multiselect(
                    "제품 선택 (미선택시 선택된 Top K 모두 표시)",
                    options=prod_candidates,
                    default=[],
                    placeholder="전체",
                    key="sel_products_tab1"
                )

        baseP = with_period(df, "event_date", mode_cat)
        baseP = baseP[baseP["product_cat_a"].isin(sel_a)].copy()

        for brand in sel_a:
            st.markdown(f"###### {brand}")

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
                    top_vals = topk_values(tb[dim], topk_cat)
                    tb[dim] = tb[dim].where(tb[dim].isin(top_vals), "기타")

            agg = (
                tb.groupby(["_period", dim], dropna=False)["pseudo_session_id"]
                  .nunique()
                  .reset_index(name="sessions")
                  .rename(columns={"_period": "날짜", dim: "구분"})
            )

            if agg.empty:
                st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
                st.markdown(" ")
                continue

            wide = (
                agg.pivot_table(index="날짜", columns="구분", values="sessions", fill_value=0, aggfunc="sum")
                   .reset_index()
                   .sort_values("날짜")
                   .reset_index(drop=True)
            )

            y_cols = [c for c in wide.columns if c != "날짜"]
            long = wide.melt(id_vars=["날짜"], value_vars=y_cols, var_name="구분", value_name="sessions")

            render_stack_and_table(long, x="날짜", y="sessions", color="구분", index_col="구분", height=340, opacity=0.6)
            st.markdown(" ")

    with tab2:
        with st.expander("Filter", expanded=True):
            mode_cat3 = st.radio("집계 단위", ["일별", "주별"], horizontal=True, key="mode_cat_tab3")
            topk_cat3 = st.selectbox("표시 Top K", CFG["TOPK_CAT_OPTS"], index=2, key="topk_cat_tab3")

        baseX = with_period(df, "event_date", mode_cat3)
        tb = baseX[
            (baseX["product_cat_a"] == "슬립퍼") &
            (baseX["product_cat_b"].astype(str) == "프레임") &
            (baseX["product_cat_c"].isin(["원목", "패브릭", "호텔침대"]))
        ].copy()

        st.markdown("###### 슬립퍼")
        if tb.empty:
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        else:
            dim = "product_cat_c"
            top_vals = topk_values(tb[dim], topk_cat3)
            tb[dim] = tb[dim].where(tb[dim].isin(top_vals), "기타")

            agg = (
                tb.groupby(["_period", dim], dropna=False)["pseudo_session_id"]
                  .nunique()
                  .reset_index(name="sessions")
                  .rename(columns={"_period": "날짜", dim: "구분"})
            )
            wide = (
                agg.pivot_table(index="날짜", columns="구분", values="sessions", fill_value=0, aggfunc="sum")
                   .reset_index()
                   .sort_values("날짜")
                   .reset_index(drop=True)
            )
            y_cols = [c for c in wide.columns if c != "날짜"]
            long = wide.melt(id_vars=["날짜"], value_vars=y_cols, var_name="구분", value_name="sessions")

            render_stack_and_table(long, x="날짜", y="sessions", color="구분", index_col="구분", height=340, opacity=0.6)

    # ──────────────────────────────────
    # 4) 품목별 PDP조회 유입
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>품목별 </span>유입</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ품목 뎁스별로 특정 상품군의 PDP 조회가 발생한 세션의 유입경로를 확인합니다.", unsafe_allow_html=True)

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

        # ✅ 4영역 pills 정렬을 3영역과 동일하게 (중분류 우선순위 + 소분류 부모기반 정렬)
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

        # (4영역) pills 선택값 수집
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

        # (4영역) 제품 선택
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
            prod_candidates = topk_values(tmpP["product_name"], max(50, 200)) if not tmpP.empty else []

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

    # ──────────────────────────────────
    # (4영역 본문) 브랜드별 렌더
    # ──────────────────────────────────
    for brand in sel_a_pp:
        st.markdown(f"###### {brand}")

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
            st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
            st.markdown(" ")
            continue

        render_stack_and_table(agg_path_brand, x="날짜", y="sessions", color="유입경로", index_col="유입경로", height=360, opacity=0.6)
        st.markdown(" ")


if __name__ == "__main__":
    main()
