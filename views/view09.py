# 서희_최신수정일_25-08-20
# ✅ 기능 동일 유지
# ✅ def 최소화(필수 3개)
# ✅ 히트맵: UI 제거 + 코드에서 직접 지정 (HEAT_ROWS__TAB1 / HEAT_ROWS__BD_TAB1)
# ✅ 테마 시작주 하드코딩은 CONFIG에서만 수정
# ✅ NEW: 필터(UI)를 sidebar가 아니라 subheader 아래(본문)에 배치
# ✅ NEW: event_name == total_session_start → isSessionStart = 2
# ✅ NEW: utm null/blank → 'direct' 로 채움
# ✅ NEW: isSessionStart(0/1) 체크박스 필터 (기본 둘다) — 단, 2는 항상 포함

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta, date
import plotly.graph_objects as go

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery

from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import JsCode

import sys
import modules.style
importlib.reload(sys.modules["modules.style"])
from modules.style import style_format, style_cmap


# ──────────────────────────────────
# CONFIG (사용자가 자유롭게 수정)
# ──────────────────────────────────
THEME_START_ISO = {
    "에브리원": (2025, 35, 1),  # (year, week, weekday=1=Mon)
    "홀리데이": (2025, 49, 1),
}

SCROLL_ROW_ORDER = [
    "total_session_start",
    "cmp_session_start",
    "scroll_10", "scroll_20", "scroll_30", "scroll_40",
    "scroll_50", "scroll_60", "scroll_70", "scroll_80", "scroll_90", "scroll_100",
]

CTA_ORDER = [
    "1. 네비게이션",
    "2. 쇼룸 클릭",
    "3. 혜택/이벤트 보기",
    "4. 제품 바로가기",
    "5. 하단 PLP 탐색",
]

CTA_MAP = {
    "cta_nav": "1. 네비게이션",
    "cta_showroom": "2. 쇼룸 클릭",
    "cta_product_main": "4. 제품 바로가기",
    "cta_product_sub": "5. 하단 PLP 탐색",
}
CTA_PROMO_SET = {"cta", "cta_promo_campaign", "cta_promo_event", "cta_promo_wedding"}

HEATMAP_RGBA = (255, 107, 107)  # #FF6B6B
HEATMAP_ALPHA_MULT = 0.45        # 진하기

# ✅ CONFIG에 히트맵 대상 행 추가 (표별로 관리)
HEAT_ROWS__TAB1    = ["ALL"]
HEAT_ROWS__TAB2    = ["ALL"]
HEAT_ROWS__BD_TAB1 = ["ALL"]
HEAT_ROWS__BD_TAB2 = ["ALL"]


# ──────────────────────────────────
# 최소 helper (진짜 필요한 것만)
# ──────────────────────────────────
def _iso_week_to_monday(iso_week: str) -> date | None:
    """'YYYY-Www' -> 해당 ISO week의 월요일(date)."""
    if iso_week is None or (isinstance(iso_week, float) and pd.isna(iso_week)):
        return None
    s = str(iso_week).strip()
    if "-W" not in s:
        return None
    y, w = s.split("-W", 1)
    if not (y.isdigit() and w.isdigit()):
        return None
    try:
        return date.fromisocalendar(int(y), int(w), 1)
    except Exception:
        return None


def _safe_div_pct(num, den) -> pd.Series:
    """(num/den)*100 안전 처리 + 소수 1자리."""
    num = pd.to_numeric(pd.Series(num), errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan).fillna(0)
    den = pd.to_numeric(pd.Series(den), errors="coerce").astype(float).replace([np.inf, -np.inf], np.nan)
    return (num / den * 100).replace([np.inf, -np.inf], 0).fillna(0).round(1)


def _to_int_safe(x) -> int:
    v = pd.to_numeric(x, errors="coerce")
    if pd.isna(v) or np.isinf(v):
        return 0
    try:
        return int(v)
    except Exception:
        return 0


# ✅ 공통 히트맵 함수 (표별로 pvt_cnt_show / heat_rows 를 받아서 style 리턴)
def apply_heatmap(
    pvt_disp_show: pd.DataFrame,
    pvt_cnt_show: pd.DataFrame,
    heat_rows: list[str] | None
):

    if heat_rows is None:
        heat_rows_set = set()
    else:
        heat_rows_set = (
            set(pvt_cnt_show.index.astype(str).tolist())
            if "ALL" in heat_rows
            else set([str(x) for x in heat_rows])
        )

    def _heat_css(_data: pd.DataFrame):
        css = pd.DataFrame("", index=_data.index, columns=_data.columns)
        r, g, b = HEATMAP_RGBA
        alpha_mult = float(HEATMAP_ALPHA_MULT)

        for i, row_name in enumerate(pvt_cnt_show.index):
            if str(row_name) not in heat_rows_set:
                continue

            row_vals = pd.to_numeric(pvt_cnt_show.loc[row_name].values, errors="coerce")
            row_vals = row_vals[~np.isnan(row_vals)]
            if len(row_vals) == 0:
                continue

            rmin, rmax = float(row_vals.min()), float(row_vals.max())
            if rmax <= rmin:
                continue

            norm = (pvt_cnt_show.loc[row_name].astype(float) - rmin) / (rmax - rmin)
            norm = norm.replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)

            for j, col in enumerate(pvt_cnt_show.columns):
                a = float(norm.loc[col])
                css.iat[i, j] = f"background-color: rgba({r}, {g}, {b}, {a * alpha_mult:.3f});"
        return css

    return (
        pvt_disp_show.style
        .apply(_heat_css, axis=None)
        .set_properties(**{"white-space": "nowrap"})
    )


# ──────────────────────────────────
# Main
# ──────────────────────────────────
def main():
    # ──────────────────────────────────
    # 0) 레이아웃/CSS
    # ──────────────────────────────────
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
    st.markdown(
        """
        <style>
            [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
        """,
        unsafe_allow_html=True
    )

    # ──────────────────────────────────
    # 1) BigQuery load (cache)
    # ──────────────────────────────────
    @st.cache_data(ttl=3600)
    def load_all():
        cs = "20000101"
        ce = (datetime.now().date() - timedelta(days=1)).strftime("%Y%m%d")
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        return bq.get_data("tb_sleeper_sessionCMP")

    with st.spinner("BigQuery에서 전체 데이터를 불러오는 중…"):
        df = load_all()

    if df is None or df.empty:
        st.stop()

    df = df.copy()

    # ──────────────────────────────────
    # 2) 기본 전처리 (타입/결측/컬럼 보장)
    # ──────────────────────────────────
    for c in ["user_cnt", "session_cnt", "event_cnt"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0).astype(int)

    df["event_name"] = df["event_name"].astype(str)

    for c in ["utm_source", "utm_campaign"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].astype(str).replace({"nan": "", "None": ""}).fillna("")

    # ✅ utm null/blank → direct
    for c in ["utm_source", "utm_campaign"]:
        df.loc[df[c].astype(str).str.strip() == "", c] = "direct"

    # ✅ isSessionStart 보장 + total_session_start = 2
    if "isSessionStart" not in df.columns:
        df["isSessionStart"] = np.nan
    df["isSessionStart"] = pd.to_numeric(df["isSessionStart"], errors="coerce")
    df.loc[df["event_name"].astype(str) == "total_session_start", "isSessionStart"] = 2

    # ──────────────────────────────────
    # 2-1) event_date -> event_day(yyyy-mm-dd) 보장 (NEW)
    # ──────────────────────────────────
    if "event_date" not in df.columns:
        df["event_date"] = ""

    s = df["event_date"].astype(str)
    mask = s.str.fullmatch(r"\d{8}", na=False)
    dt = pd.Series(pd.NaT, index=df.index)
    dt.loc[mask] = pd.to_datetime(s.loc[mask], format="%Y%m%d", errors="coerce")
    dt.loc[~mask] = pd.to_datetime(s.loc[~mask], errors="coerce")

    df["event_day"] = dt.dt.strftime("%Y-%m-%d")
    df["event_day"] = df["event_day"].fillna("").astype(str)

    # ──────────────────────────────────
    # 3) iso_week 생성 (없거나 전부 NA일 때만 생성)
    # ──────────────────────────────────
    if "iso_week" not in df.columns or df["iso_week"].isna().all():
        s = df["event_date"].astype(str)
        mask = s.str.fullmatch(r"\d{8}", na=False)
        dt = pd.Series(pd.NaT, index=df.index)
        dt.loc[mask] = pd.to_datetime(s.loc[mask], format="%Y%m%d", errors="coerce")
        dt.loc[~mask] = pd.to_datetime(s.loc[~mask], errors="coerce")
        iso = dt.dt.isocalendar()
        df["iso_week"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)

    # ──────────────────────────────────
    # 4) cmp_theme / cmp_theme_passed (CONFIG 기반)
    # ──────────────────────────────────
    starts = {k: date.fromisocalendar(*v) for k, v in THEME_START_ISO.items()}
    everyone_start = starts.get("에브리원", date.max)
    holiday_start  = starts.get("홀리데이", date.max)

    iso_monday = df["iso_week"].astype(str).map(_iso_week_to_monday)

    df["cmp_theme"] = np.where(
        iso_monday >= holiday_start, "홀리데이",
        np.where(iso_monday >= everyone_start, "에브리원", None)
    )

    def _passed_label(monday: date | None) -> str | None:
        if monday is None:
            return None
        if monday >= holiday_start:
            n = (monday - holiday_start).days // 7 + 1
            return f"홀리데이 {n}주차"
        if monday >= everyone_start:
            n = (monday - everyone_start).days // 7 + 1
            return f"에브리원 {n}주차"
        return None

    df["cmp_theme_passed"] = iso_monday.map(_passed_label)

    # ──────────────────────────────────
    # 5) 이벤트명 정규화 (스크롤 이벤트만 rename)
    # ──────────────────────────────────
    rename_map = {
        "everyone_page_scroll_10": "scroll_10",
        "everyone_page_scroll_20": "scroll_20",
        "everyone_page_scroll_30": "scroll_30",
        "everyone_page_scroll_40": "scroll_40",
        "everyone_page_scroll_50": "scroll_50",
        "everyone_page_scroll_60": "scroll_60",
        "everyone_page_scroll_70": "scroll_70",
        "everyone_page_scroll_80": "scroll_80",
        "everyone_page_scroll_90": "scroll_90",
        "everyone_page_scroll_100": "scroll_100",
    }
    df["event_name"] = df["event_name"].replace(rename_map)

    # ──────────────────────────────────
    # 6) week_order / theme 옵션/테마별 주차 리스트 / 멀티헤더 구성
    # ──────────────────────────────────
    week_order = (
        df[["iso_week"]]
        .drop_duplicates()
        .sort_values("iso_week")["iso_week"]
        .tolist()
    )

    theme_opts = sorted(df["cmp_theme"].dropna().astype(str).unique().tolist())
    default_left  = "에브리원" if "에브리원" in theme_opts else (theme_opts[0] if theme_opts else "")
    default_right = "홀리데이" if "홀리데이" in theme_opts else (theme_opts[-1] if theme_opts else "")

    def _build_week_mi(wlist: list[str]) -> pd.MultiIndex:
        top, bot = [], []
        for w in wlist:
            monday = _iso_week_to_monday(w)
            top.append(_passed_label(monday) or "")
            bot.append(w)
        return pd.MultiIndex.from_arrays([top, bot], names=["cmp_theme_passed", "iso_week"])

    week_mi = _build_week_mi(week_order)

    # ──────────────────────────────────
    # 7) 타이틀/설명/refresh
    # ──────────────────────────────────
    st.subheader("GA CMP 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    st.markdown(
        """
        <div style="  
            font-size:14px;       
            line-height:1.5;      
        ">
        이 대시보드에서는 <b>캠페인 페이지</b>에서 발생한 사용자 행동을, 
        전체 흐름과 유입별 구조로 나누어 전환 관점에서 분석합니다.<br>
        </div>
        <div style="
            color:#6c757d;        
            font-size:14px;       
            line-height:2.0;      
        ">
        ※ 설명...
        </div>
        """,
        unsafe_allow_html=True
    )
    st.divider()

    # # ──────────────────────────────────
    # # 공통 필터
    # # ──────────────────────────────────
    # with st.expander("공통 필터", expanded=False):
    #     f0, f1, f2, f3 = st.columns([1.8, 2.2, 2.0, 3.5], vertical_alignment="bottom")

    #     with f0:
    #         view_grain = st.radio(
    #             "표/그래프 기준",
    #             ["주별", "일별"],
    #             index=0,
    #             horizontal=True,
    #             key="view_grain_main"
    #         )

    #         sel_iso_weeks = None
    #         if view_grain == "일별":
    #             iso_opts = week_order[:]  # 이미 정렬된 iso_week
    #             default_iso = [iso_opts[-1]] if iso_opts else []
    #             sel_iso_weeks = st.multiselect(
    #                 "일별 표시할 ISO Week",
    #                 iso_opts,
    #                 default=default_iso,
    #                 key="sel_iso_weeks_main"
    #             )
    #             if not sel_iso_weeks:
    #                 sel_iso_weeks = default_iso

    #     with f1:
    #         agg_choice = st.radio("집계 기준", ["세션수", "유저수"], index=0, horizontal=True, key="agg_choice_main")
    #         value_col = "session_cnt" if agg_choice == "세션수" else "user_cnt"

    #     with f2:
    #         y_scale = st.radio("그래프 Y축 스케일", ["원값", "로그"], index=0, horizontal=True, key="y_scale_main")

    #     with f3:
    #         st.markdown(
    #             "<div style='font-size:14px; margin-bottom:6px;'>집계 범위 선택 (페이지 진입 방식을 선택합니다)</div>",
    #             unsafe_allow_html=True
    #         )
    #         c0, c1 = st.columns(2)
    #         with c0:
    #             sel_is0 = st.checkbox("거쳐간 경우만 (page_view)", value=True, key="is0_main")
    #         with c1:
    #             sel_is1 = st.checkbox("랜딩한 경우만 (session_start)", value=True, key="is1_main")

    #         if (not sel_is0) and (not sel_is1):
    #             sel_is0, sel_is1 = True, True

    #         allow_is = set()
    #         if sel_is0:
    #             allow_is.add(0)
    #         if sel_is1:
    #             allow_is.add(1)

    # ──────────────────────────────────
    # 공통 필터 (원 코드 유지 + "한 줄" 간격만 조정)
    # ──────────────────────────────────
    with st.expander("공통 필터", expanded=False):

        # ✅ 4칸 → 5칸(ISO Week 전용칸)으로만 확장해서 한 줄 유지
        #    - 일별이 아닐 때도 빈 칸(비활성 selectbox)로 자리 유지
        f0, f0b, f1, f2, f3 = st.columns([1.3, 2.4, 2.0, 1.6, 3.2], vertical_alignment="bottom")

        with f0:
            view_grain = st.radio(
                "표/그래프 기준",
                ["주별", "일별"],
                index=0,
                horizontal=True,
                key="view_grain_main"
            )

        # ✅ (NEW) ISO week 셀을 f0에서 분리해서 '줄바꿈' 없애기
        with f0b:
            sel_iso_weeks = None
            iso_opts = week_order[:]  # 이미 정렬된 iso_week
            default_iso = [iso_opts[-1]] if iso_opts else []

            if view_grain == "일별":
                sel_iso_weeks = st.multiselect(
                    "일별 표시할 ISO Week",
                    iso_opts,
                    default=default_iso,
                    key="sel_iso_weeks_main"
                )
                if not sel_iso_weeks:
                    sel_iso_weeks = default_iso
            else:
                # 자리만 유지(한 줄 유지용)
                st.selectbox(
                    "일별 표시할 ISO Week",
                    ["(일별 선택 시 활성화)"],
                    index=0,
                    disabled=True,
                    key="sel_iso_weeks_main__disabled"
                )

        with f1:
            agg_choice = st.radio("집계 기준", ["세션수", "유저수"], index=0, horizontal=True, key="agg_choice_main")
            value_col = "session_cnt" if agg_choice == "세션수" else "user_cnt"

        with f2:
            y_scale = st.radio("그래프 Y축 스케일", ["원값", "로그"], index=0, horizontal=True, key="y_scale_main")

        with f3:
            st.markdown(
                "<div style='font-size:14px; margin-bottom:6px;'>집계 범위 선택 (페이지 진입 방식을 선택합니다)</div>",
                unsafe_allow_html=True
            )
            c0, c1 = st.columns(2)
            with c0:
                sel_is0 = st.checkbox("거쳐간 경우만 (page_view)", value=True, key="is0_main")
            with c1:
                sel_is1 = st.checkbox("랜딩한 경우만 (session_start)", value=True, key="is1_main")

            if (not sel_is0) and (not sel_is1):
                sel_is0, sel_is1 = True, True

            allow_is = set()
            if sel_is0:
                allow_is.add(0)
            if sel_is1:
                allow_is.add(1)



    # ──────────────────────────────────
    # ✅ (NEW) 주별/일별 피벗 공통 스위치
    # ──────────────────────────────────
    pivot_col = "iso_week" if view_grain == "주별" else "event_day"

    # (A) 일별이면 선택된 iso_week만 남김
    if view_grain == "일별":
        sel_iso_weeks = [str(x) for x in (sel_iso_weeks or [])]
        df = df[df["iso_week"].astype(str).isin(sel_iso_weeks)].copy()

    # (B) 컬럼 순서(order_cols)
    if view_grain == "주별":
        order_cols = week_order[:]  # iso_week 정렬 리스트
    else:
        day_order = (
            df.loc[df["event_day"].astype(str).str.len() > 0, "event_day"]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .sort_values()
            .tolist()
        )
        order_cols = day_order

    # (C) 멀티헤더(MultiIndex)
    def _build_day_mi(day_list: list[str]) -> pd.MultiIndex:
        m = (
            df.loc[df["event_day"].astype(str).isin(day_list), ["event_day", "iso_week"]]
            .drop_duplicates()
            .set_index("event_day")["iso_week"]
            .to_dict()
        )
        top, bot = [], []
        for d0 in day_list:
            w = str(m.get(d0, ""))
            monday = _iso_week_to_monday(w) if w else None
            top.append(_passed_label(monday) or "")
            bot.append(d0)
        return pd.MultiIndex.from_arrays([top, bot], names=["cmp_theme_passed", "event_day"])

    if view_grain == "주별":
        col_mi = _build_week_mi(order_cols)
    else:
        col_mi = _build_day_mi(order_cols)

    # (D) 테마별 비교에 쓰는 "테마 컬럼 리스트"도 동일하게 맞춤
    theme_cols = {}
    for t in theme_opts:
        if view_grain == "주별":
            w = df.loc[df["cmp_theme"] == t, "iso_week"].dropna().astype(str).unique().tolist()
            theme_cols[t] = sorted([x for x in w if x in week_order])
        else:
            d = df.loc[df["cmp_theme"] == t, "event_day"].dropna().astype(str).unique().tolist()
            theme_cols[t] = sorted([x for x in d if x in order_cols])

    # ──────────────────────────────────
    # 공통 유틸 (중복 제거)
    # ──────────────────────────────────
    def _pivot_counts(dfx: pd.DataFrame, idx, cols, val_col, order_cols_: list[str]):
        return (
            dfx.pivot_table(index=idx, columns=cols, values=val_col, aggfunc="sum", fill_value=0)
            .reindex(columns=order_cols_)
        )

    def _format_pivot_disp(
        pvt_counts: pd.DataFrame,
        den: pd.Series,
        wlist: list[str],
        raw_rows: set[str] | None = None,
        pct_decimals: int = 1,
        pct_round0_for_scroll: bool = False,
    ) -> pd.DataFrame:
        raw_rows = raw_rows or set()
        out = pvt_counts.copy().astype(object)

        for rname in pvt_counts.index:
            if str(rname) in raw_rows:
                for w in wlist:
                    out.loc[rname, w] = _to_int_safe(pvt_counts.loc[rname, w])
            else:
                conv = _safe_div_pct(pvt_counts.loc[rname], den.replace(0, np.nan))
                for w in wlist:
                    cnt = _to_int_safe(pvt_counts.loc[rname, w])
                    if pct_round0_for_scroll:
                        out.loc[rname, w] = f"{cnt:,} ({float(conv.loc[w]):.0f}%)"
                    else:
                        out.loc[rname, w] = f"{cnt:,} ({float(conv.loc[w]):.{pct_decimals}f}%)"
        return out

    def _theme_bar_compare(
        *,
        pvt_counts: pd.DataFrame,
        theme_opts: list[str],
        default_left: str,
        default_right: str,
        theme_weeks: dict[str, list[str]],
        x_rows: list[str],
        conv_rows: set[str],
        den_row: str,
        y_scale: str,
        key_prefix: str,
        right_color: str = "#F58518",
    ):
        if not theme_opts:
            return

        l, r = st.columns(2)
        with l:
            th_l = st.selectbox("이전 테마", theme_opts, index=theme_opts.index(default_left), key=f"{key_prefix}__th_l")
        with r:
            th_r = st.selectbox("비교 테마", theme_opts, index=theme_opts.index(default_right), key=f"{key_prefix}__th_r")

        def _mean_row(r0: str, wlist: list[str]) -> float:
            if r0 not in pvt_counts.index:
                return 0.0
            cols = [c for c in wlist if c in pvt_counts.columns]
            if not cols:
                return 0.0
            s = pd.to_numeric(pvt_counts.loc[r0, cols], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)
            return float(s.mean())

        def _plot(theme: str, bar_color: str | None = None):
            wlist = theme_weeks.get(theme, [])
            if not wlist:
                fig = go.Figure()
                fig.update_layout(
                    title=f"{theme} (데이터 없음)",
                    xaxis_visible=False, yaxis_visible=False,
                    annotations=[dict(
                        text="집계할 데이터가 없습니다.",
                        x=0.5, y=0.5, xref="paper", yref="paper",
                        showarrow=False, font=dict(size=14, color="#888")
                    )]
                )
                return fig

            y = [_mean_row(r0, wlist) for r0 in x_rows]
            den_mean = _mean_row(den_row, wlist)

            text = []
            for r0, v0 in zip(x_rows, y):
                base = f"{int(round(v0, 0)):,}"
                if (r0 in conv_rows) and (den_mean > 0):
                    base = f"{base} ({round(v0/den_mean*100, 1):.1f}%)"
                text.append(base)

            bar_kwargs = {}
            if bar_color:
                bar_kwargs["marker"] = dict(color=bar_color)

            fig = go.Figure([go.Bar(x=x_rows, y=y, text=text, textposition="outside", cliponaxis=False, **bar_kwargs)])
            fig.update_layout(
                title="",
                xaxis_title="",
                yaxis_title="기간 평균",
                showlegend=False,
                bargap=0.35,
                margin=dict(l=30, r=30, t=60, b=30),
            )

            if y_scale == "로그":
                eps = 0.1
                fig.data[0].y = [max(float(v), eps) for v in fig.data[0].y]
                fig.update_yaxes(type="log")

            fig.update_yaxes(automargin=True)
            return fig

        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(_plot(th_l), use_container_width=False, key=f"{key_prefix}__bar_l")
        with c2:
            st.plotly_chart(_plot(th_r, bar_color=right_color), use_container_width=False, key=f"{key_prefix}__bar_r")

    def _rank_opts_inline(dfx: pd.DataFrame, value_col_: str, dim_col: str) -> list[str]:
        d = dfx[dfx["event_name"].astype(str) == "cmp_session_start"].copy()
        if d.empty:
            return sorted(dfx.get(dim_col, pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        s = d.groupby(dim_col, dropna=False)[value_col_].sum().sort_values(ascending=False)
        opts = [str(x) for x in s.index.tolist()]
        return [x for x in opts if x.strip() != ""]

    def _fill_direct(dfx: pd.DataFrame, cols: list[str]):
        for c in cols:
            dfx[c] = dfx.get(c, "").astype(str).replace({"nan": "", "None": ""}).fillna("")
            dfx.loc[dfx[c].str.strip() == "", c] = "direct"
        return dfx

    # ──────────────────────────────────
    # 8) 첫번째 영역
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>전체 행동 흐름</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ캠페인 페이지에서 유저가 얼마나 다음 행동으로 이어졌는지 전체 흐름을 확인합니다. ")

    # ✅ df 필터 적용 (allow_is)
    mask_total2 = (df["event_name"].astype(str) == "total_session_start") | (df["isSessionStart"] == 2)
    mask_01 = df["isSessionStart"].isin(list(allow_is))
    df = df[mask_total2 | mask_01].copy()

    tab1, tab2 = st.tabs(["Scroll 액션", "CTA 액션"])

    # ──────────────────────────────────
    # tab1: CMP랜딩_SCROLL
    # ──────────────────────────────────
    with tab1:
        pvt_ev = _pivot_counts(df, "event_name", pivot_col, value_col, order_cols).reindex(SCROLL_ROW_ORDER)

        den = (
            pvt_ev.loc["cmp_session_start"]
            if "cmp_session_start" in pvt_ev.index
            else pd.Series(0, index=order_cols)
        )

        pvt_ev_disp = _format_pivot_disp(
            pvt_counts=pvt_ev,
            den=den,
            wlist=order_cols,
            raw_rows={"cmp_session_start", "total_session_start"},
            pct_decimals=0,
            pct_round0_for_scroll=True,
        )

        _theme_bar_compare(
            pvt_counts=pvt_ev,
            theme_opts=theme_opts,
            default_left=default_left,
            default_right=default_right,
            theme_weeks=theme_cols,
            x_rows=["cmp_session_start", "scroll_10", "scroll_20", "scroll_30", "scroll_40", "scroll_50"],
            conv_rows=set(["scroll_10", "scroll_20", "scroll_30", "scroll_40", "scroll_50"]),
            den_row="cmp_session_start",
            y_scale=y_scale,
            key_prefix="ev",
        )

        pvt_ev_disp_show = pvt_ev_disp.copy()
        pvt_ev_disp_show.columns = col_mi

        pvt_ev_cnt_show = pvt_ev.copy()
        pvt_ev_cnt_show.columns = col_mi

        sty = apply_heatmap(pvt_ev_disp_show, pvt_ev_cnt_show, HEAT_ROWS__TAB1)
        st.dataframe(sty, use_container_width=True, height=500)

    # ──────────────────────────────────
    # tab2: CMP랜딩_CTA
    # ──────────────────────────────────
    with tab2:
        df_cta = df[df["event_name"].astype(str) == "click_cta_everyone"].copy()

        if not df_cta.empty:
            df_cta["cta_type"] = df_cta["cta_type"].astype(str).apply(
                lambda v: ("3. 혜택/이벤트 보기" if v in CTA_PROMO_SET else CTA_MAP.get(v, None))
            )
            df_cta = df_cta[df_cta["cta_type"].isin(CTA_ORDER)].copy()

        if df_cta.empty:
            st.info("집계할 데이터가 없습니다.")
        else:
            # ✅ 주별/일별에 따라 CTA 컬럼(order) 따로 잡기
            if view_grain == "주별":
                order_cols_cta = [c for c in order_cols if c in df_cta["iso_week"].astype(str).unique()]
                if not order_cols_cta:
                    order_cols_cta = order_cols
                col_mi_cta = _build_week_mi(order_cols_cta)
            else:
                order_cols_cta = [c for c in order_cols if c in df_cta["event_day"].astype(str).unique()]
                if not order_cols_cta:
                    order_cols_cta = order_cols
                col_mi_cta = _build_day_mi(order_cols_cta)

            cmp_row = _pivot_counts(
                df[df["event_name"].astype(str) == "cmp_session_start"],
                "event_name", pivot_col, value_col, order_cols_cta
            )
            den = (
                cmp_row.loc["cmp_session_start"]
                if "cmp_session_start" in cmp_row.index
                else pd.Series(0, index=order_cols_cta)
            )

            pvt_cta = _pivot_counts(df_cta, "cta_type", pivot_col, value_col, order_cols_cta).reindex(CTA_ORDER)
            pvt_full = pd.concat([pd.DataFrame([den], index=["cmp_session_start"]), pvt_cta], axis=0)

            pvt_disp = _format_pivot_disp(
                pvt_counts=pvt_full,
                den=den,
                wlist=order_cols_cta,
                raw_rows={"cmp_session_start"},
                pct_decimals=1,
                pct_round0_for_scroll=False,
            )

            _theme_bar_compare(
                pvt_counts=pvt_full,
                theme_opts=theme_opts,
                default_left=default_left,
                default_right=default_right,
                theme_weeks=theme_cols,
                x_rows=["cmp_session_start"] + CTA_ORDER,
                conv_rows=set(CTA_ORDER),
                den_row="cmp_session_start",
                y_scale=y_scale,
                key_prefix="cta",
            )

            pvt_disp_show = pvt_disp.copy()
            pvt_disp_show.columns = col_mi_cta

            pvt_cnt_show = pvt_full.copy()
            pvt_cnt_show.columns = col_mi_cta

            sty_cta = apply_heatmap(pvt_disp_show, pvt_cnt_show, HEAT_ROWS__TAB2)
            st.dataframe(sty_cta, use_container_width=True, height=500)

    # ──────────────────────────────────
    # 9) 두번째 영역: 드릴다운 + breakdown
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>매체·캠페인별 분석 </h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ캠페인 페이지에서의 행동이 어떤 매체·캠페인에서 발생했는지 확인합니다. ")

    df_break = df.copy()
    dim_col = "utm_source"
    force_source = None
    sel_campaign = "(전체)"

    c1, c2, c3 = st.columns([3.0, 2.2, 2.2], vertical_alignment="top")

    with c1:
        view_mode = st.radio(
            "드릴다운 선택",
            ["매체별로 보기", "캠페인별로 보기"],
            index=0,
            horizontal=True,
            key="bd_view_mode"
        )

    with c2:
        if view_mode == "캠페인별로 보기":
            src_opts = _rank_opts_inline(df_break, value_col, "utm_source")
            if not src_opts:
                src_opts = ["direct"]
            default_src = "meta" if "meta" in src_opts else src_opts[0]
            force_source = st.selectbox(
                "매체 선택",
                src_opts,
                index=src_opts.index(default_src),
                key="bd_force_source"
            )
        else:
            st.selectbox(
                "매체 선택",
                ["(전체)"],
                index=0,
                disabled=True,
                key="bd_force_source__disabled"
            )

    with c3:
        if view_mode == "캠페인별로 보기":
            dim_col = "utm_campaign"
            df_break = df_break[df_break["utm_source"].astype(str) == str(force_source)].copy()

            camp_opts = _rank_opts_inline(df_break, value_col, "utm_campaign")
            camp_opts = ["(전체)"] + (camp_opts if camp_opts else ["direct"])
            sel_campaign = st.selectbox(
                "캠페인 선택",
                camp_opts,
                index=0,
                key="bd_force_campaign"
            )
            if sel_campaign != "(전체)":
                df_break = df_break[df_break["utm_campaign"].astype(str) == str(sel_campaign)].copy()
        else:
            st.selectbox(
                "캠페인 선택",
                ["(전체)"],
                index=0,
                disabled=True,
                key="bd_force_campaign__disabled"
            )

    bd_tab1, bd_tab2 = st.tabs(["Scroll 액션", "CTA 액션"])

    # ──────────────────────────────────
    # bd_tab1: breakdown 이벤트표 + ✅ 히트맵(코드 지정 행만)
    # ──────────────────────────────────
    with bd_tab1:
        base_rows = [
            "cmp_session_start",
            "scroll_10", "scroll_20", "scroll_30", "scroll_40",
            "scroll_50", "scroll_60", "scroll_70", "scroll_80", "scroll_90", "scroll_100",
        ]

        pvt_main = _pivot_counts(df_break, "event_name", pivot_col, value_col, order_cols).reindex(base_rows)

        den_main = (
            pvt_main.loc["cmp_session_start"]
            if "cmp_session_start" in pvt_main.index
            else pd.Series(0, index=order_cols)
        )

        pvt_disp_main = _format_pivot_disp(
            pvt_counts=pvt_main,
            den=den_main,
            wlist=order_cols,
            raw_rows={"cmp_session_start"},
            pct_decimals=1,
            pct_round0_for_scroll=False,
        )

        df_bd = df_break[df_break["event_name"].isin(pvt_main.index.tolist())].copy()

        if df_bd.empty:
            pvt_cnt = pvt_main.copy()
            pvt_disp = pvt_disp_main.copy()
        else:
            df_bd = _fill_direct(df_bd, [dim_col])

            pvt_bd = (
                df_bd.pivot_table(index=["event_name", dim_col], columns=pivot_col, values=value_col, aggfunc="sum", fill_value=0)
                .reindex(columns=order_cols)
            )

            out_rows, out_cnt, out_disp = [], [], []

            def _add_row(name: str, cnt_series: pd.Series, disp_series: pd.Series):
                out_rows.append(name)
                out_cnt.append(cnt_series.reindex(order_cols))
                out_disp.append(disp_series.reindex(order_cols))

            for rname in pvt_main.index.tolist():
                base_cnt = pd.to_numeric(pvt_main.loc[rname], errors="coerce").fillna(0).astype(float)

                if rname == "cmp_session_start":
                    base_disp = pd.Series([f"{_to_int_safe(base_cnt.loc[w]):,}" for w in order_cols], index=order_cols, dtype=object)
                else:
                    conv = _safe_div_pct(base_cnt, den_main.replace(0, np.nan))
                    base_disp = pd.Series(
                        [f"{_to_int_safe(base_cnt.loc[w]):,} ({float(conv.loc[w]):.1f}%)" for w in order_cols],
                        index=order_cols, dtype=object
                    )

                _add_row(rname, base_cnt, base_disp)

                if rname in pvt_bd.index.get_level_values(0):
                    sub = pvt_bd.xs(rname, level=0)
                    sub = sub.loc[sub.sum(axis=1).sort_values(ascending=False).index]

                    parent_total = base_cnt.replace(0, np.nan)
                    for dim_v, row in sub.iterrows():
                        cnt = pd.to_numeric(row, errors="coerce").fillna(0).astype(float)
                        share = _safe_div_pct(cnt, parent_total)

                        sub_label = f"   └ {dim_v} [{rname}]"
                        disp = pd.Series(
                            [f"{_to_int_safe(cnt.loc[w]):,} ({float(share.loc[w]):.1f}%)" for w in order_cols],
                            index=order_cols, dtype=object
                        )
                        _add_row(sub_label, cnt, disp)

            pvt_cnt = pd.DataFrame(out_cnt, index=out_rows, columns=order_cols)
            pvt_disp = pd.DataFrame(out_disp, index=out_rows, columns=order_cols)

        _theme_bar_compare(
            pvt_counts=pvt_cnt,
            theme_opts=theme_opts,
            default_left=default_left,
            default_right=default_right,
            theme_weeks=theme_cols,
            x_rows=["cmp_session_start", "scroll_10", "scroll_20", "scroll_30", "scroll_40"],
            conv_rows=set(["scroll_10", "scroll_20", "scroll_30", "scroll_40"]),
            den_row="cmp_session_start",
            y_scale=y_scale,
            key_prefix="bd_ev",
        )

        pvt_disp_show = pvt_disp.copy()
        pvt_disp_show.columns = col_mi

        pvt_cnt_show = pvt_cnt.copy()
        pvt_cnt_show.columns = col_mi

        sty_bd = apply_heatmap(pvt_disp_show, pvt_cnt_show, HEAT_ROWS__BD_TAB1)
        st.dataframe(sty_bd, use_container_width=True, height=700)

    # ──────────────────────────────────
    # bd_tab2: breakdown CTA + ✅ 히트맵
    # ──────────────────────────────────
    with bd_tab2:
        df_cta_b = df_break[df_break["event_name"].astype(str) == "click_cta_everyone"].copy()

        if not df_cta_b.empty:
            df_cta_b["cta_type"] = df_cta_b["cta_type"].astype(str).apply(
                lambda v: ("3. 혜택/이벤트 보기" if v in CTA_PROMO_SET else CTA_MAP.get(v, None))
            )
            df_cta_b = df_cta_b[df_cta_b["cta_type"].isin(CTA_ORDER)].copy()

        if df_cta_b.empty:
            st.info("집계할 데이터가 없습니다.")
        else:
            # ✅ 주별/일별에 따라 breakdown-CTA 컬럼(order) 따로 잡기
            if view_grain == "주별":
                order_cols_cta = [c for c in order_cols if c in df_cta_b["iso_week"].astype(str).unique()]
                if not order_cols_cta:
                    order_cols_cta = order_cols
                col_mi_cta = _build_week_mi(order_cols_cta)
            else:
                order_cols_cta = [c for c in order_cols if c in df_cta_b["event_day"].astype(str).unique()]
                if not order_cols_cta:
                    order_cols_cta = order_cols
                col_mi_cta = _build_day_mi(order_cols_cta)

            df_break2 = _fill_direct(df_break.copy(), [dim_col])
            df_cta_b2 = _fill_direct(df_cta_b.copy(), [dim_col])

            df_cmp = df_break2[df_break2["event_name"].astype(str) == "cmp_session_start"].copy()
            cmp_dim = _pivot_counts(df_cmp, dim_col, pivot_col, value_col, order_cols_cta)
            cmp_total = pd.to_numeric(cmp_dim.sum(axis=0), errors="coerce").fillna(0).astype(float)

            pvt_main = _pivot_counts(df_cta_b2, "cta_type", pivot_col, value_col, order_cols_cta).reindex(CTA_ORDER)
            pvt_dim = (
                df_cta_b2.pivot_table(index=["cta_type", dim_col], columns=pivot_col, values=value_col, aggfunc="sum", fill_value=0)
                .reindex(columns=order_cols_cta)
            )

            out_rows, out_cnt, out_disp = [], [], []

            def _add(name, cnt_s: pd.Series, disp_s: pd.Series):
                out_rows.append(name)
                out_cnt.append(cnt_s.reindex(order_cols_cta))
                out_disp.append(disp_s.reindex(order_cols_cta))

            disp_cmp = pd.Series([f"{_to_int_safe(cmp_total.loc[w]):,}" for w in order_cols_cta], index=order_cols_cta, dtype=object)
            _add("cmp_session_start", cmp_total, disp_cmp)

            for dim_v in cmp_dim.sum(axis=1).sort_values(ascending=False).index.tolist():
                cnt = pd.to_numeric(cmp_dim.loc[dim_v], errors="coerce").fillna(0).astype(float)
                disp = pd.Series([f"{_to_int_safe(cnt.loc[w]):,}" for w in order_cols_cta], index=order_cols_cta, dtype=object)
                _add(f"   └ {dim_v} [cmp_session_start]", cnt, disp)

            for cta in CTA_ORDER:
                cnt = (
                    pd.to_numeric(pvt_main.loc[cta], errors="coerce").fillna(0).astype(float)
                    if cta in pvt_main.index
                    else pd.Series(0, index=order_cols_cta, dtype=float)
                )
                conv = _safe_div_pct(cnt, cmp_total.replace(0, np.nan))
                disp = pd.Series(
                    [f"{_to_int_safe(cnt.loc[w]):,} ({float(conv.loc[w]):.1f}%)" for w in order_cols_cta],
                    index=order_cols_cta, dtype=object
                )
                _add(cta, cnt, disp)

                if cta in pvt_dim.index.get_level_values(0):
                    sub = pvt_dim.xs(cta, level=0)
                    sub = sub.loc[sub.sum(axis=1).sort_values(ascending=False).index]

                    parent_cta_total = (
                        pd.to_numeric(pvt_main.loc[cta], errors="coerce").replace(0, np.nan).astype(float)
                        if cta in pvt_main.index
                        else pd.Series(np.nan, index=order_cols_cta, dtype=float)
                    )

                    for dim_v, row in sub.iterrows():
                        cnt_d = pd.to_numeric(row, errors="coerce").fillna(0).astype(float)
                        share_d = _safe_div_pct(cnt_d, parent_cta_total)

                        sub_label = f"   └ {dim_v} [{cta}]"
                        disp_d = pd.Series(
                            [f"{_to_int_safe(cnt_d.loc[w]):,} ({float(share_d.loc[w]):.1f}%)" for w in order_cols_cta],
                            index=order_cols_cta, dtype=object
                        )
                        _add(sub_label, cnt_d, disp_d)

            pvt_cnt = pd.DataFrame(out_cnt, index=out_rows, columns=order_cols_cta)
            pvt_disp = pd.DataFrame(out_disp, index=out_rows, columns=order_cols_cta)

            _theme_bar_compare(
                pvt_counts=pvt_cnt,
                theme_opts=theme_opts,
                default_left=default_left,
                default_right=default_right,
                theme_weeks=theme_cols,
                x_rows=["cmp_session_start"] + CTA_ORDER,
                conv_rows=set(CTA_ORDER),
                den_row="cmp_session_start",
                y_scale=y_scale,
                key_prefix="bd_cta",
            )

            pvt_disp_show = pvt_disp.copy()
            pvt_disp_show.columns = col_mi_cta

            pvt_cnt_show = pvt_cnt.copy()
            pvt_cnt_show.columns = col_mi_cta

            sty_bd_cta = apply_heatmap(pvt_disp_show, pvt_cnt_show, HEAT_ROWS__BD_TAB2)
            st.dataframe(sty_bd_cta, use_container_width=True, height=700)


if __name__ == "__main__":
    main()
