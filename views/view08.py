# 서희_최신수정일_25-08-20
# REFAC 25-12-26 (fix Streamlit session_state error)
# ✅ widget key(session_state) 직접 대입 금지 -> pop(삭제)로 리셋
# ✅ 기간추이/일별 + 테마 "전체" 선택 시 ISO Week가 홀리데이만 뜨는 버그 보정 유지
# ✅ 모든 multiselect에 "전체" 옵션
# ✅ Tab1/Tab2도 Tab3처럼 "그래프 표시 지표" multiselect 적용
# ✅ 로그 선택 시 bar/line 모두 eps 처리로 깨짐 방지
# ✅ [요청반영] 3번째 영역(캠페인별 행동 흐름) 삭제
# ✅ [요청반영] 2번째 영역(유입별/매체)에서 "테마추이" 옵션 제거(기간추이만)
# ✅ [요청반영] 2번째 영역 표 구조: 이벤트(상위) → ㄴ 매체(하위) 로 변경
# ✅ [요청반영] (추가) 유입별(매체) 그래프: 범례가 그래프를 가리지 않게(상단 바깥)
# ✅ [요청반영] (추가) 유입별(매체) 그래프: bar/line 매체 색상 통일
# ✅ [요청반영] (추가) 유입별(매체) 표: 이벤트별 "전체" 행(수치) 추가 + 그 수치 기준으로 매체 % 재계산(소수점 없음)
# ✅ [추가요청] "전체" 행은 매체 선택과 무관하게 같은 값 유지 (매체 선택으로 sum 변하면 안됨)

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta, date
import plotly.graph_objects as go
import plotly.colors as pc  # ✅ NEW

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery


# ──────────────────────────────────
# CONFIG
# ──────────────────────────────────
THEME_START_ISO = {
    "에브리원": (2025, 35, 1),
    "홀리데이": (2025, 49, 1),
    "뉴이어"  : (2026,  1, 1),
}

SCROLL_ROW_ORDER = [
    "total_session_start",
    "cmp_session_start",
    "scroll_10", "scroll_20", "scroll_30", "scroll_40", "scroll_50", "scroll_60", "scroll_70", "scroll_80", "scroll_90", "scroll_100",
]

CTA_ORDER = [
    "1. 네비게이션",
    "2. 쇼룸 클릭",
    "3. 혜택/이벤트 보기",
    "4. 제품 바로가기",
    "5. 하단 PLP 탐색",
]

CTA_MAP = {
    "cta_nav"         : "1. 네비게이션",
    "cta_showroom"    : "2. 쇼룸 클릭",
    "cta_product_main": "4. 제품 바로가기",
    "cta_product_sub" : "5. 하단 PLP 탐색",
}
CTA_PROMO_SET = {"cta", "cta_promo_campaign", "cta_promo_event", "cta_promo_wedding"}  # 3. 혜택/이벤트 보기

HEATMAP_RGBA = (255, 107, 107)
HEATMAP_ALPHA_MULT = 0.45

HEAT_ROWS__TAB1 = ["ALL"]
HEAT_ROWS__TAB2 = ["ALL"]
HEAT_ROWS__TAB3 = ["ALL"]

ALL_LABEL = "전체"
LOG_EPS = 0.1


LEGEND_LAYOUT = dict(
    orientation="h",
    yanchor="bottom",
    y=1.18,                 # ✅ plot 영역 밖
    xanchor="left",
    x=0,
    bgcolor="rgba(255,255,255,0.85)",  # ✅ 혹시 겹쳐도 가독성
)



# ──────────────────────────────────
# helpers
# ──────────────────────────────────
def _reset_keys(keys: list[str]):
    """위젯 key는 직접 대입하지 말고 pop으로 리셋."""
    for k in keys:
        if k in st.session_state:
            st.session_state.pop(k, None)


def _iso_week_to_monday(iso_week: str) -> date | None:
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


def _apply_select_all(selected: list[str] | None, options: list[str], *, all_label: str = ALL_LABEL) -> list[str]:
    selected = [str(x) for x in (selected or [])]
    if all_label in selected:
        return [x for x in options if x != all_label]
    return [x for x in selected if x in options]


def apply_heatmap(pvt_disp_show: pd.DataFrame, pvt_cnt_show: pd.DataFrame, heat_rows: list[str] | None):
    if heat_rows is None:
        heat_rows_set = set()
    else:
        idx_str = pvt_cnt_show.index.map(lambda x: str(x))
        heat_rows_set = (
            set(idx_str.tolist())
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

            row = pvt_cnt_show.loc[row_name]
            if isinstance(row, pd.DataFrame):
                row = row.sum(axis=0)

            row_vals = pd.to_numeric(np.asarray(row.values).ravel(), errors="coerce")
            row_vals = row_vals[~np.isnan(row_vals)]
            if len(row_vals) == 0:
                continue

            rmin, rmax = float(row_vals.min()), float(row_vals.max())
            if rmax <= rmin:
                continue

            norm = (pd.to_numeric(row, errors="coerce").astype(float) - rmin) / (rmax - rmin)
            norm = norm.replace([np.inf, -np.inf], 0).fillna(0).clip(0, 1)

            for j, col in enumerate(pvt_cnt_show.columns):
                a = float(norm.loc[col]) if col in norm.index else 0.0
                css.iat[i, j] = f"background-color: rgba({r}, {g}, {b}, {a * alpha_mult:.3f});"

        return css

    return (
        pvt_disp_show.style
        .apply(_heat_css, axis=None)
        .set_properties(**{"white-space": "nowrap"})
    )


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


def _build_theme_meta(df: pd.DataFrame):
    starts = {k: date.fromisocalendar(*v) for k, v in THEME_START_ISO.items()}
    everyone_start = starts.get("에브리원", date.max)
    holiday_start  = starts.get("홀리데이", date.max)
    newyear_start  = starts.get("뉴이어",  date.max)

    iso_monday = df["iso_week"].astype(str).map(_iso_week_to_monday)

    df["cmp_theme"] = np.where(
        iso_monday >= newyear_start, "뉴이어",
        np.where(
            iso_monday >= holiday_start, "홀리데이",
            np.where(iso_monday >= everyone_start, "에브리원", None)
        )
    )

    def _passed_label(monday: date | None) -> str | None:
        if monday is None:
            return None
        if monday >= newyear_start:
            n = (monday - newyear_start).days // 7 + 1
            return f"뉴이어 {n}주차"
        if monday >= holiday_start:
            n = (monday - holiday_start).days // 7 + 1
            return f"홀리데이 {n}주차"
        if monday >= everyone_start:
            n = (monday - everyone_start).days // 7 + 1
            return f"에브리원 {n}주차"
        return None

    df["cmp_theme_passed"] = iso_monday.map(_passed_label)
    return df


def _build_week_mi(df: pd.DataFrame, weeks: list[str]) -> pd.MultiIndex:
    m = (
        df.loc[df["iso_week"].astype(str).isin(weeks), ["iso_week", "cmp_theme_passed"]]
        .drop_duplicates()
        .set_index("iso_week")["cmp_theme_passed"]
        .to_dict()
    )
    top = [str(m.get(w, "") or "") for w in weeks]
    bot = [str(w) for w in weeks]
    return pd.MultiIndex.from_arrays([top, bot], names=["cmp_theme_passed", "iso_week"])


def _build_day_mi(df: pd.DataFrame, days: list[str]) -> pd.MultiIndex:
    m = (
        df.loc[df["event_day"].astype(str).isin(days), ["event_day", "cmp_theme_passed"]]
        .drop_duplicates()
        .set_index("event_day")["cmp_theme_passed"]
        .to_dict()
    )
    top = [str(m.get(d, "") or "") for d in days]
    bot = [str(d) for d in days]
    return pd.MultiIndex.from_arrays([top, bot], names=["cmp_theme_passed", "event_day"])


def _get_latest_weeks(df: pd.DataFrame, n: int = 2) -> list[str]:
    w = (
        df[["iso_week"]].drop_duplicates()
        .sort_values("iso_week")["iso_week"].astype(str).tolist()
    )
    return w[-n:] if len(w) >= n else w


def _get_themes_for_weeks(df: pd.DataFrame, weeks: list[str]) -> list[str]:
    sub = df[df["iso_week"].astype(str).isin([str(x) for x in weeks])].copy()
    t = sub["cmp_theme"].dropna().astype(str).unique().tolist()
    order = ["에브리원", "홀리데이", "뉴이어"]
    return [x for x in order if x in t] + [x for x in t if x not in order]


def _plot_dual_trend(
    *,
    x_order: list[str],
    pvt_counts: pd.DataFrame,
    bar_row: str,
    line_rows: list[str],
    y_scale: str,
    title: str,
    height: int = 340,
    chart_key: str | None = None,
):
    fig = go.Figure()

    if bar_row in pvt_counts.index:
        y_bar = pd.to_numeric(pvt_counts.loc[bar_row, x_order], errors="coerce").fillna(0).astype(float).tolist()
    else:
        y_bar = [0.0 for _ in x_order]

    if y_scale == "로그":
        y_bar = [max(float(v), LOG_EPS) for v in y_bar]

    fig.add_trace(
        go.Bar(
            x=x_order,
            y=y_bar,
            name=str(bar_row),
            opacity=0.22,
            yaxis="y"
        )
    )

    for r in [r for r in (line_rows or []) if r in pvt_counts.index and r != bar_row]:
        y = pd.to_numeric(pvt_counts.loc[r, x_order], errors="coerce").fillna(0).astype(float).tolist()
        if y_scale == "로그":
            y = [max(float(v), LOG_EPS) for v in y]
        fig.add_trace(
            go.Scatter(
                x=x_order,
                y=y,
                mode="lines+markers",
                name=str(r),
                yaxis="y2"
            )
        )

    fig.update_layout(
        height=height,
        title=title,
        margin=dict(l=20, r=20, t=90, b=10),
        legend=LEGEND_LAYOUT,
        barmode="overlay",
        yaxis=dict(title="Session", side="left"),
        yaxis2=dict(title="Action", side="right", overlaying="y"),
    )

    if y_scale == "로그":
        fig.update_layout(yaxis_type="log", yaxis2_type="log")

    if chart_key:
        st.plotly_chart(fig, use_container_width=True, key=str(chart_key))
    else:
        st.plotly_chart(fig, use_container_width=True)


def _theme_bar_compare(
    *,
    pvt_counts: pd.DataFrame,
    theme_pick: list[str],
    theme_cols: dict[str, list[str]],
    x_rows: list[str],
    conv_rows: set[str],
    den_row: str,
    y_scale: str,
    key_prefix: str,
):
    if len(theme_pick) < 2:
        st.info("비교할 테마를 2개 선택해주세요.")
        return

    def _mean_row(r0: str, wlist: list[str]) -> float:
        if r0 not in pvt_counts.index:
            return 0.0
        cols = [c for c in wlist if c in pvt_counts.columns]
        if not cols:
            return 0.0
        s = pd.to_numeric(pvt_counts.loc[r0, cols], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)
        return float(s.mean())

    def _plot(theme: str):
        wlist = theme_cols.get(theme, [])
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

        if y_scale == "로그":
            y = [max(float(v), LOG_EPS) for v in y]

        fig = go.Figure([go.Bar(x=x_rows, y=y, text=text, textposition="outside", cliponaxis=False)])
        fig.update_layout(
            title="",
            xaxis_title="",
            yaxis_title="On Average",
            showlegend=False,
            bargap=0.35,
            margin=dict(l=20, r=20, t=90, b=10),
        )

        if y_scale == "로그":
            fig.update_yaxes(type="log")

        fig.update_yaxes(automargin=True)
        return fig

    th_l, th_r = theme_pick[0], theme_pick[1]
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**{th_l}**")
        st.plotly_chart(_plot(th_l), use_container_width=True, key=f"{key_prefix}__bar_l")
    with c2:
        st.markdown(f"**{th_r}**")
        st.plotly_chart(_plot(th_r), use_container_width=True, key=f"{key_prefix}__bar_r")


# ──────────────────────────────────
# Main
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
            [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
        """,
        unsafe_allow_html=True
    )

    @st.cache_data(ttl=3600)
    def load_all():
        cs = "20000101"
        ce = (datetime.now().date() - timedelta(days=1)).strftime("%Y%m%d")
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        return bq.get_data("tb_sleeper_sessionCMP")

    st.subheader("GA CMP 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    st.markdown(
        """
        <div style="font-size:14px; line-height:1.5;">
        이 대시보드에서는 <b>캠페인 페이지</b>에서 발생한 사용자 행동을, 
        전체 흐름과 유입별 구조로 나누어 전환 관점에서 분석합니다.<br>
        </div>
        <div style="color:#6c757d; font-size:14px; line-height:2.0;">
        ※ 설명...
        </div>
        """,
        unsafe_allow_html=True
    )
    st.divider()

    with st.spinner("BigQuery에서 전체 데이터를 불러오는 중…"):
        df = load_all()
    if df is None or df.empty:
        st.stop()
    df = df.copy()

    for c in ["user_cnt", "session_cnt", "event_cnt"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0).astype(int)

    df["event_name"] = df.get("event_name", "").astype(str)

    for c in ["utm_source", "utm_campaign"]:
        df[c] = df.get(c, "").astype(str).replace({"nan": "", "None": ""}).fillna("")
        df.loc[df[c].astype(str).str.strip() == "", c] = "direct"

    if "isSessionStart" not in df.columns:
        df["isSessionStart"] = np.nan
    df["isSessionStart"] = pd.to_numeric(df["isSessionStart"], errors="coerce")
    df.loc[df["event_name"].astype(str) == "total_session_start", "isSessionStart"] = 2

    if "event_date" not in df.columns:
        df["event_date"] = ""
    s = df["event_date"].astype(str)
    mask8 = s.str.fullmatch(r"\d{8}", na=False)
    dt = pd.Series(pd.NaT, index=df.index)
    dt.loc[mask8] = pd.to_datetime(s.loc[mask8], format="%Y%m%d", errors="coerce")
    dt.loc[~mask8] = pd.to_datetime(s.loc[~mask8], errors="coerce")
    df["event_day"] = dt.dt.strftime("%Y-%m-%d").fillna("").astype(str)

    if "iso_week" not in df.columns or df["iso_week"].isna().all():
        iso = dt.dt.isocalendar()
        df["iso_week"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    df["iso_week"] = df["iso_week"].astype(str)

    df = _build_theme_meta(df)

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

    if "prev_compare_mode" not in st.session_state:
        st.session_state["prev_compare_mode"] = None
    if "prev_view_grain" not in st.session_state:
        st.session_state["prev_view_grain"] = None

    # ──────────────────────────────────
    # 8) 첫번째 영역
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>전체 행동 흐름</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ캠페인 페이지에서 유저가 얼마나 다음 행동으로 이어졌는지 전체 흐름을 확인합니다. ")

    with st.container(border=True):
        a1, a2, a3, a4, a5 = st.columns([2.2, 2, 4, 2, 2], vertical_alignment="top")

        with a1:
            compare_mode = st.radio("비교 모드", ["기간 추이", "테마 추이"], index=0, horizontal=True, key="compare_mode_main")

        with a2:
            view_grain = st.radio("표/그래프 기준", ["주별", "일별"], index=1, horizontal=True, key="view_grain_main")

        with a3:
            st.markdown("<div style='font-size:14px; margin-bottom:6px;'>집계 범위(진입 방식)</div>", unsafe_allow_html=True)
            c0, c1, c2 = st.columns([2, 2, 0.4])
            with c0:
                sel_is0 = st.checkbox("거쳐간 경우(page_view)", value=True, key="is0_main")
            with c1:
                sel_is1 = st.checkbox("랜딩한 경우(session_start)", value=True, key="is1_main")
            with c2:
                pass
            if (not sel_is0) and (not sel_is1):
                sel_is0, sel_is1 = True, True

            allow_is = set()
            if sel_is0:
                allow_is.add(0)
            if sel_is1:
                allow_is.add(1)

        with a4:
            agg_choice = st.radio("집계 기준", ["세션수", "유저수"], index=0, horizontal=True, key="agg_choice_main")
            value_col = "session_cnt" if agg_choice == "세션수" else "user_cnt"

        with a5:
            y_scale = st.radio("Y축", ["원값", "로그"], index=0, horizontal=True, key="y_scale_main")

        df_base = df.copy()
        mask_total2 = (df_base["event_name"].astype(str) == "total_session_start") | (df_base["isSessionStart"] == 2)
        mask_01 = df_base["isSessionStart"].isin(list(allow_is))
        df_base = df_base[mask_total2 | mask_01].copy()

        theme_opts_raw = sorted(df_base["cmp_theme"].dropna().astype(str).unique().tolist())
        theme_opts = [ALL_LABEL] + theme_opts_raw

        all_weeks_asc = (
            df_base[["iso_week"]].drop_duplicates()
            .sort_values("iso_week")["iso_week"].astype(str).tolist()
        )
        all_weeks_desc = all_weeks_asc[::-1]

        latest_2w = _get_latest_weeks(df_base, 2)
        latest_themes = _get_themes_for_weeks(df_base, latest_2w)

        if st.session_state["prev_compare_mode"] != compare_mode:
            if compare_mode == "테마 추이":
                _reset_keys(["theme_pick_main", "sel_iso_weeks_main", "themeA_iso_weeks", "themeB_iso_weeks"])
            st.session_state["prev_compare_mode"] = compare_mode

        if st.session_state["prev_view_grain"] != view_grain:
            if compare_mode == "기간 추이" and view_grain == "일별":
                _reset_keys(["sel_iso_weeks_main"])
            st.session_state["prev_view_grain"] = view_grain

        b1, b2 = st.columns([4.0, 4.0], vertical_alignment="bottom")

        with b1:
            if compare_mode == "기간 추이":
                default_theme = latest_themes[:2] if len(latest_themes) >= 2 else latest_themes
                default_theme_ui = [ALL_LABEL] if (set(default_theme) == set(theme_opts_raw) and theme_opts_raw) else default_theme

                theme_pick_ui = st.multiselect("테마 선택", theme_opts, default=default_theme_ui, key="theme_pick_main")
                theme_pick = _apply_select_all(theme_pick_ui, theme_opts, all_label=ALL_LABEL)
            else:
                st.caption("비교할 테마를 2개 선택해주세요. 원하는 주만 보려면 [ 일별 ]로 전환하세요.")
                theme_pick_ui = st.multiselect("테마 선택", theme_opts_raw, default=[], key="theme_pick_main")  # ✅ '전체' 제거
                theme_pick = [x for x in theme_pick_ui if x in theme_opts_raw]

        with b2:
            if compare_mode == "기간 추이":
                if view_grain == "일별":
                    if (not theme_pick) or (set(theme_pick) == set(theme_opts_raw)):
                        iso_opts_desc = [ALL_LABEL] + all_weeks_desc
                    else:
                        weeks_by_theme = (
                            df_base.loc[df_base["cmp_theme"].astype(str).isin(theme_pick), "iso_week"]
                            .drop_duplicates().sort_values().astype(str).tolist()
                        )
                        iso_opts_desc = [ALL_LABEL] + weeks_by_theme[::-1]

                    sel_iso_ui = st.multiselect("ISO Week", iso_opts_desc, default=[ALL_LABEL], key="sel_iso_weeks_main")
                    sel_iso_weeks = _apply_select_all(sel_iso_ui, iso_opts_desc, all_label=ALL_LABEL)
                    sel_iso_weeks = [x for x in sel_iso_weeks if x != ALL_LABEL]
                else:
                    st.multiselect("ISO Week", ["(일별 선택 시 활성화)"], default=[], disabled=True, key="sel_iso_weeks_main__disabled")
                    sel_iso_weeks = []
            else:
                if len(theme_pick) < 2:
                    st.multiselect("ISO Week", ["(테마 2개 선택 시 활성화)"], default=[], disabled=True, key="theme_iso_disabled")
                    sel_iso_weeks = []
                else:
                    if view_grain == "일별":
                        tA, tB = theme_pick[0], theme_pick[1]
                        wA_asc = df_base.loc[df_base["cmp_theme"].astype(str) == str(tA), "iso_week"].drop_duplicates().sort_values().astype(str).tolist()
                        wB_asc = df_base.loc[df_base["cmp_theme"].astype(str) == str(tB), "iso_week"].drop_duplicates().sort_values().astype(str).tolist()
                        wA_desc = [ALL_LABEL] + wA_asc[::-1]
                        wB_desc = [ALL_LABEL] + wB_asc[::-1]

                        cA, cB = st.columns(2)
                        with cA:
                            uiA = st.multiselect(f"{tA} ISO Week", wA_desc, default=[ALL_LABEL], key="themeA_iso_weeks")
                            wA_sel = _apply_select_all(uiA, wA_desc, all_label=ALL_LABEL)
                            wA_sel = [x for x in wA_sel if x != ALL_LABEL]

                        with cB:
                            uiB = st.multiselect(f"{tB} ISO Week", wB_desc, default=[ALL_LABEL], key="themeB_iso_weeks")
                            wB_sel = _apply_select_all(uiB, wB_desc, all_label=ALL_LABEL)
                            wB_sel = [x for x in wB_sel if x != ALL_LABEL]

                        sel_iso_weeks = list(dict.fromkeys(wA_sel + wB_sel))
                    else:
                        st.multiselect("ISO Week", ["(일별 선택 시 활성화)"], default=[], disabled=True, key="theme_iso_disabled__2")
                        sel_iso_weeks = []

    df_f = df_base.copy()

    if theme_pick:
        df_f = df_f[df_f["cmp_theme"].astype(str).isin(theme_pick)].copy()
        if df_f.empty:
            df_f = df_base.copy()

    pivot_col = "iso_week" if view_grain == "주별" else "event_day"

    if view_grain == "일별":
        if sel_iso_weeks:
            df_f = df_f[df_f["iso_week"].astype(str).isin([str(x) for x in sel_iso_weeks])].copy()

    if view_grain == "주별":
        order_cols = df_f[["iso_week"]].drop_duplicates().sort_values("iso_week")["iso_week"].astype(str).tolist()
        col_mi = _build_week_mi(df_f, order_cols)
    else:
        order_cols = df_f.loc[df_f["event_day"].astype(str).str.len() > 0, "event_day"].drop_duplicates().sort_values().astype(str).tolist()
        col_mi = _build_day_mi(df_f, order_cols)

    theme_cols = {}
    for t in theme_pick:
        if view_grain == "주별":
            w = df_f.loc[df_f["cmp_theme"].astype(str) == str(t), "iso_week"].drop_duplicates().sort_values().astype(str).tolist()
            theme_cols[t] = [x for x in w if x in order_cols]
        else:
            d = df_f.loc[df_f["cmp_theme"].astype(str) == str(t), "event_day"].drop_duplicates().sort_values().astype(str).tolist()
            theme_cols[t] = [x for x in d if x in order_cols]

    if compare_mode == "테마 추이" and (not theme_pick or len(theme_pick) < 2):
        st.stop()

    tab1, tab2, tab3 = st.tabs(["Scroll 액션", "CTA 액션", "이후 액션"])

    with tab1:
        pvt_ev = _pivot_counts(df_f, "event_name", pivot_col, value_col, order_cols).reindex(SCROLL_ROW_ORDER)
        den = pvt_ev.loc["cmp_session_start"] if "cmp_session_start" in pvt_ev.index else pd.Series(0, index=order_cols)

        pvt_ev_disp = _format_pivot_disp(
            pvt_counts=pvt_ev,
            den=den,
            wlist=order_cols,
            raw_rows={"cmp_session_start", "total_session_start"},
            pct_decimals=0,
            pct_round0_for_scroll=True,
        )

        if compare_mode == "기간 추이":
            scroll_line_opts = [ALL_LABEL] + [x for x in SCROLL_ROW_ORDER if x.startswith("scroll_") and x in pvt_ev.index]
            sel_lines_ui = st.multiselect("선그래프로 나타낼 액션", scroll_line_opts, default=[ALL_LABEL], key="tab1_lines")
            sel_lines = _apply_select_all(sel_lines_ui, scroll_line_opts, all_label=ALL_LABEL)
            sel_lines = [x for x in sel_lines if x != ALL_LABEL]

            _plot_dual_trend(x_order=order_cols, pvt_counts=pvt_ev, bar_row="cmp_session_start", line_rows=sel_lines, y_scale=y_scale, title="")
        else:
            _theme_bar_compare(
                pvt_counts=pvt_ev,
                theme_pick=theme_pick,
                theme_cols=theme_cols,
                x_rows=["cmp_session_start", "scroll_10", "scroll_20", "scroll_30", "scroll_40", "scroll_50"],
                conv_rows=set(["scroll_10", "scroll_20", "scroll_30", "scroll_40", "scroll_50"]),
                den_row="cmp_session_start",
                y_scale=y_scale,
                key_prefix="tab1_theme_bar",
            )

        pvt_disp_show = pvt_ev_disp.copy()
        pvt_disp_show.columns = col_mi
        pvt_cnt_show = pvt_ev.copy()
        pvt_cnt_show.columns = col_mi
        sty = apply_heatmap(pvt_disp_show, pvt_cnt_show, HEAT_ROWS__TAB1)
        st.dataframe(sty, use_container_width=True, height=520)

    with tab2:
        df_cta = df_f[df_f["event_name"].astype(str) == "click_cta_everyone"].copy()

        if not df_cta.empty:
            df_cta["cta_type"] = df_cta["cta_type"].astype(str).apply(
                lambda v: ("3. 혜택/이벤트 보기" if v in CTA_PROMO_SET else CTA_MAP.get(v, None))
            )
            df_cta = df_cta[df_cta["cta_type"].isin(CTA_ORDER)].copy()

        if df_cta.empty:
            st.info("집계할 데이터가 없습니다.")
        else:
            cmp_row = _pivot_counts(df_f[df_f["event_name"].astype(str) == "cmp_session_start"], "event_name", pivot_col, value_col, order_cols)
            den = cmp_row.loc["cmp_session_start"] if "cmp_session_start" in cmp_row.index else pd.Series(0, index=order_cols)

            pvt_cta = _pivot_counts(df_cta, "cta_type", pivot_col, value_col, order_cols).reindex(CTA_ORDER)
            pvt_full = pd.concat([pd.DataFrame([den], index=["cmp_session_start"]), pvt_cta], axis=0)

            pvt_disp = _format_pivot_disp(pvt_counts=pvt_full, den=den, wlist=order_cols, raw_rows={"cmp_session_start"}, pct_decimals=1)

            if compare_mode == "기간 추이":
                cta_line_opts = [ALL_LABEL] + [x for x in CTA_ORDER if x in pvt_full.index]
                cta_sel_ui = st.multiselect("선그래프로 나타낼 액션", cta_line_opts, default=[ALL_LABEL], key="tab2_cta_lines")
                cta_sel = _apply_select_all(cta_sel_ui, cta_line_opts, all_label=ALL_LABEL)
                cta_sel = [x for x in cta_sel if x != ALL_LABEL]

                _plot_dual_trend(x_order=order_cols, pvt_counts=pvt_full, bar_row="cmp_session_start", line_rows=cta_sel, y_scale=y_scale, title="")
            else:
                _theme_bar_compare(
                    pvt_counts=pvt_full,
                    theme_pick=theme_pick,
                    theme_cols=theme_cols,
                    x_rows=["cmp_session_start"] + CTA_ORDER,
                    conv_rows=set(CTA_ORDER),
                    den_row="cmp_session_start",
                    y_scale=y_scale,
                    key_prefix="tab2_theme_bar",
                )

            pvt_disp_show = pvt_disp.copy()
            pvt_disp_show.columns = col_mi
            pvt_cnt_show = pvt_full.copy()
            pvt_cnt_show.columns = col_mi
            sty = apply_heatmap(pvt_disp_show, pvt_cnt_show, HEAT_ROWS__TAB2)
            st.dataframe(sty, use_container_width=True, height=520)

    with tab3:
        AFTER_ACTION_ORDER = [
            "cmp_session_start",
            "view_item",
            "view_item_list",
            "product_page_scroll_25",
            "product_option_price",
            "find_nearby_showroom",
            "showroom_10s",
            "showroom_leads",
            "add_to_cart",
        ]

        df_a = df_f[df_f["event_name"].astype(str).isin(AFTER_ACTION_ORDER)].copy()
        if df_a.empty:
            st.info("집계할 데이터가 없습니다.")
        else:
            pvt_a = _pivot_counts(df_a, "event_name", pivot_col, value_col, order_cols).reindex(AFTER_ACTION_ORDER)
            den = pvt_a.loc["cmp_session_start"] if "cmp_session_start" in pvt_a.index else pd.Series(0, index=order_cols)

            pvt_a_disp = _format_pivot_disp(pvt_counts=pvt_a, den=den, wlist=order_cols, raw_rows={"cmp_session_start"}, pct_decimals=1)

            if compare_mode == "기간 추이":
                opts = [x for x in AFTER_ACTION_ORDER if x != "cmp_session_start" and x in pvt_a.index]
                line_opts = [ALL_LABEL] + opts
                lines_ui = st.multiselect("선그래프로 나타낼 액션", line_opts, default=[ALL_LABEL], key="tab3_lines_sel")
                lines_sel = _apply_select_all(lines_ui, line_opts, all_label=ALL_LABEL)
                lines_sel = [x for x in lines_sel if x != ALL_LABEL]

                _plot_dual_trend(x_order=order_cols, pvt_counts=pvt_a, bar_row="cmp_session_start", line_rows=lines_sel, y_scale=y_scale, title="")
            else:
                _theme_bar_compare(
                    pvt_counts=pvt_a,
                    theme_pick=theme_pick,
                    theme_cols=theme_cols,
                    x_rows=["cmp_session_start", "view_item", "product_option_price", "find_nearby_showroom", "showroom_leads", "add_to_cart"],
                    conv_rows=set(["view_item", "product_option_price", "find_nearby_showroom", "showroom_leads", "add_to_cart"]),
                    den_row="cmp_session_start",
                    y_scale=y_scale,
                    key_prefix="tab3_theme_bar",
                )

            pvt_disp_show = pvt_a_disp.copy()
            pvt_disp_show.columns = col_mi
            pvt_cnt_show = pvt_a.copy()
            pvt_cnt_show.columns = col_mi
            sty = apply_heatmap(pvt_disp_show, pvt_cnt_show, HEAT_ROWS__TAB3)
            st.dataframe(sty, use_container_width=True, height=520)

    # ──────────────────────────────────
    # 9) 두번째 영역 (드릴다운: 이벤트 → 매체)
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>유입별(매체) 행동 흐름</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ매체(기본) 단위로 행동 흐름을 드릴다운해 확인합니다. ")

    MEDIA_COL = "utm_source"
    CAMP_COL  = "utm_campaign"

    with st.container(border=True):
        a1, a2, a3, a4, a5 = st.columns([2.2, 2, 4, 2, 2], vertical_alignment="top")

        with a1:
            st.markdown("<div style='font-size:14px; margin-bottom:6px;'>비교 모드</div>", unsafe_allow_html=True)
            st.markdown("**기간 추이**")  # ✅ 고정

        with a2:
            view_grain__m = st.radio("표/그래프 기준", ["주별", "일별"], index=1, horizontal=True, key="view_grain_m")

        with a3:
            st.markdown("<div style='font-size:14px; margin-bottom:6px;'>집계 범위(진입 방식)</div>", unsafe_allow_html=True)
            c0, c1, c2 = st.columns([2, 2, 0.4])
            with c0:
                sel_is0__m = st.checkbox("거쳐간 경우(page_view)", value=True, key="is0_m")
            with c1:
                sel_is1__m = st.checkbox("랜딩한 경우(session_start)", value=True, key="is1_m")
            with c2:
                pass
            if (not sel_is0__m) and (not sel_is1__m):
                sel_is0__m, sel_is1__m = True, True

            allow_is__m = set()
            if sel_is0__m:
                allow_is__m.add(0)
            if sel_is1__m:
                allow_is__m.add(1)

        with a4:
            agg_choice__m = st.radio("집계 기준", ["세션수", "유저수"], index=0, horizontal=True, key="agg_choice_m")
            value_col__m = "session_cnt" if agg_choice__m == "세션수" else "user_cnt"

        with a5:
            y_scale__m = st.radio("Y축", ["원값", "로그"], index=0, horizontal=True, key="y_scale_m")

        # base (진입 방식 필터까지만)
        df_base_m = df.copy()
        mask_total2 = (df_base_m["event_name"].astype(str) == "total_session_start") | (df_base_m["isSessionStart"] == 2)
        mask_01 = df_base_m["isSessionStart"].isin(list(allow_is__m))
        df_base_m = df_base_m[mask_total2 | mask_01].copy()

        # options
        media_opts_raw = sorted(df_base_m.get(MEDIA_COL, "direct").astype(str).replace("", "direct").unique().tolist())
        media_opts = [ALL_LABEL] + media_opts_raw

        camp_opts_raw = sorted(df_base_m.get(CAMP_COL, "direct").astype(str).replace("", "direct").unique().tolist())
        camp_opts = [ALL_LABEL] + camp_opts_raw

        theme_opts_raw_m = sorted(df_base_m["cmp_theme"].dropna().astype(str).unique().tolist())
        theme_opts_m = [ALL_LABEL] + theme_opts_raw_m

        latest_2w_m = _get_latest_weeks(df_base_m, 2)
        latest_themes_m = _get_themes_for_weeks(df_base_m, latest_2w_m)

        f3, f4 = st.columns([2, 2], vertical_alignment="top")
        with f3:
            default_theme_m = latest_themes_m[:2] if len(latest_themes_m) >= 2 else latest_themes_m
            default_theme_ui_m = [ALL_LABEL] if (set(default_theme_m) == set(theme_opts_raw_m) and theme_opts_raw_m) else default_theme_m

            theme_pick_ui_m = st.multiselect("테마 선택", theme_opts_m, default=default_theme_ui_m, key="theme_pick_m")
            theme_pick_m = _apply_select_all(theme_pick_ui_m, theme_opts_m, all_label=ALL_LABEL)
            theme_pick_m = [x for x in theme_pick_m if x != ALL_LABEL]

        with f4:
            all_weeks_asc_m = df_base_m[["iso_week"]].drop_duplicates().sort_values("iso_week")["iso_week"].astype(str).tolist()
            all_weeks_desc_m = all_weeks_asc_m[::-1]

            if view_grain__m == "일별":
                if (not theme_pick_m) or (set(theme_pick_m) == set(theme_opts_raw_m)):
                    iso_opts_desc_m = [ALL_LABEL] + all_weeks_desc_m
                else:
                    weeks_by_theme_m = (
                        df_base_m.loc[df_base_m["cmp_theme"].astype(str).isin(theme_pick_m), "iso_week"]
                        .drop_duplicates().sort_values().astype(str).tolist()
                    )
                    iso_opts_desc_m = [ALL_LABEL] + weeks_by_theme_m[::-1]

                sel_iso_ui_m = st.multiselect("ISO Week", iso_opts_desc_m, default=[ALL_LABEL], key="sel_iso_weeks_m")
                sel_iso_weeks_m = _apply_select_all(sel_iso_ui_m, iso_opts_desc_m, all_label=ALL_LABEL)
                sel_iso_weeks_m = [x for x in sel_iso_weeks_m if x != ALL_LABEL]
            else:
                st.multiselect("ISO Week", ["(일별 선택 시 활성화)"], default=[], disabled=True, key="sel_iso_weeks_m__disabled")
                sel_iso_weeks_m = []

        f1, f2 = st.columns([2, 2], vertical_alignment="top")
        with f1:
            sel_media_ui = st.multiselect("매체 선택", media_opts, default=[ALL_LABEL], key="sel_media_m")
            sel_media = _apply_select_all(sel_media_ui, media_opts, all_label=ALL_LABEL)
            sel_media = [x for x in sel_media if x != ALL_LABEL]

        with f2:
            sel_camp_ui = st.multiselect("캠페인 선택", camp_opts, default=[ALL_LABEL], key="sel_camp_m")
            sel_camp = _apply_select_all(sel_camp_ui, camp_opts, all_label=ALL_LABEL)
            sel_camp = [x for x in sel_camp if x != ALL_LABEL]

    # ✅ totals용 df (매체 필터 제외) : "전체" 행은 여기서만 계산
    df_m_total = df_base_m.copy()

    if theme_pick_m:
        df_m_total = df_m_total[df_m_total["cmp_theme"].astype(str).isin(theme_pick_m)].copy()
        if df_m_total.empty:
            df_m_total = df_base_m.copy()

    if sel_camp:
        df_m_total = df_m_total[df_m_total[CAMP_COL].astype(str).isin([str(x) for x in sel_camp])].copy()

    pivot_col_m = "iso_week" if view_grain__m == "주별" else "event_day"

    if view_grain__m == "일별" and sel_iso_weeks_m:
        df_m_total = df_m_total[df_m_total["iso_week"].astype(str).isin([str(x) for x in sel_iso_weeks_m])].copy()

    # ✅ 화면용 df (매체 선택 반영) : 매체별 하위행/그래프는 여기서
    df_m = df_m_total.copy()
    if sel_media:
        df_m = df_m[df_m[MEDIA_COL].astype(str).isin([str(x) for x in sel_media])].copy()

    if view_grain__m == "주별":
        order_cols_m = df_m_total[["iso_week"]].drop_duplicates().sort_values("iso_week")["iso_week"].astype(str).tolist()
        col_mi_m = _build_week_mi(df_m_total, order_cols_m)
    else:
        order_cols_m = (
            df_m_total.loc[df_m_total["event_day"].astype(str).str.len() > 0, "event_day"]
            .drop_duplicates().sort_values().astype(str).tolist()
        )
        col_mi_m = _build_day_mi(df_m_total, order_cols_m)

    # ──────────────────────────────────
    # helper: media stacked + media lines (색상 통일 + 범례 상단 바깥)
    # ──────────────────────────────────
    def _plot_media_stacked_with_action(
        *,
        df_src: pd.DataFrame,
        media_col: str,
        pivot_col: str,
        value_col: str,
        order_cols: list[str],
        action_event: str,
        y_scale: str,
        chart_key: str,
        height: int = 360,
    ):
        medias = sorted(df_src[media_col].astype(str).unique().tolist())

        palette = []
        for name in ["Plotly", "D3", "Set2", "Set3", "Dark24"]:
            palette += getattr(pc.qualitative, name, [])
        palette = palette or pc.qualitative.Plotly
        color_map = {m: palette[i % len(palette)] for i, m in enumerate(medias)}

        fig = go.Figure()

        for m in medias:
            df_m0 = df_src[(df_src[media_col].astype(str) == str(m)) & (df_src["event_name"].astype(str) == "cmp_session_start")].copy()
            if df_m0.empty:
                y = [0.0 for _ in order_cols]
            else:
                pvt = _pivot_counts(df_m0, "event_name", pivot_col, value_col, order_cols)
                y = pd.to_numeric(pvt.loc["cmp_session_start", order_cols], errors="coerce").fillna(0).astype(float).tolist()

            if y_scale == "로그":
                y = [max(float(v), LOG_EPS) for v in y]

            fig.add_trace(
                go.Bar(
                    x=order_cols,
                    y=y,
                    name=str(m),
                    marker=dict(color=color_map.get(m)),
                    yaxis="y",
                    opacity=0.22
                )
            )

        if action_event:
            for m in medias:
                df_m1 = df_src[(df_src[media_col].astype(str) == str(m)) & (df_src["event_name"].astype(str) == str(action_event))].copy()
                if df_m1.empty:
                    y = [0.0 for _ in order_cols]
                else:
                    pvt = _pivot_counts(df_m1, "event_name", pivot_col, value_col, order_cols)
                    y = pd.to_numeric(pvt.loc[str(action_event), order_cols], errors="coerce").fillna(0).astype(float).tolist()

                if y_scale == "로그":
                    y = [max(float(v), LOG_EPS) for v in y]

                fig.add_trace(
                    go.Scatter(
                        x=order_cols,
                        y=y,
                        mode="lines+markers",
                        name=f"{m} · {action_event}",
                        line=dict(color=color_map.get(m), width=2),
                        marker=dict(color=color_map.get(m)),
                        yaxis="y2",
                        showlegend=False,
                    )
                )

        # 수정 수정 ~
        fig.update_layout(
            height=height,
            margin=dict(l=20, r=20, t=90, b=10),  # ✅ top 여백 더 확보 (70→110 권장)
            barmode="stack",
            legend=LEGEND_LAYOUT,
            yaxis=dict(title="cmp_session_start (by media)", side="left"),
            yaxis2=dict(title=f"{action_event} (by media)", side="right", overlaying="y"),
        )


        if y_scale == "로그":
            fig.update_layout(yaxis_type="log", yaxis2_type="log")

        st.plotly_chart(fig, use_container_width=True, key=chart_key)

    # ──────────────────────────────────
    # ✅ 이벤트(상위) → ㄴ 매체(하위) 표 생성
    #     - "전체" 행은 df_total에서 계산 (매체 선택과 무관)
    #     - 하위 매체행은 df_media에서 계산 (매체 선택 반영)
    #     - 매체 % = (매체수치 / 전체수치) * 100, 소수점 없음
    #     - total_session_start는 매체 분기 없음
    # ──────────────────────────────────
    def _make_event_drill_table(
        df_total: pd.DataFrame,   # ✅ 매체 필터 없는 기준
        df_media: pd.DataFrame,   # ✅ 매체 필터 적용(선택된 매체만)
        *,
        media_col: str,
        event_order: list[str],
        pivot_col: str,
        value_col: str,
        order_cols: list[str],
        no_split_event: str = "total_session_start",
    ):
        frames_disp = []
        frames_cnt  = []

        medias = sorted(df_media[media_col].astype(str).unique().tolist())

        for ev in event_order:
            ev = str(ev)

            # ─────────────────────────
            # A) 전체(매체 분기 없음)
            # ─────────────────────────
            df_ev_total = df_total[df_total["event_name"].astype(str) == ev].copy()

            if df_ev_total.empty:
                cnt_total = pd.Series(0, index=order_cols)
            else:
                pvt_tot = _pivot_counts(df_ev_total, "event_name", pivot_col, value_col, order_cols)
                cnt_total = (
                    pd.to_numeric(pvt_tot.loc[ev, order_cols], errors="coerce").fillna(0).astype(int)
                    if ev in pvt_tot.index else pd.Series(0, index=order_cols)
                )

            mi_tot = pd.MultiIndex.from_arrays([[ev], ["전체"]], names=["이벤트", ""])
            disp_tot = [f"{_to_int_safe(cnt_total[c]):,}" for c in order_cols]
            disp_tot_row = pd.DataFrame([disp_tot], index=mi_tot, columns=order_cols).astype(object)
            cnt_tot_row  = pd.DataFrame([[int(cnt_total[c]) for c in order_cols]], index=mi_tot, columns=order_cols).astype(int)

            frames_disp.append(disp_tot_row)
            frames_cnt.append(cnt_tot_row)

            # total_session_start는 여기서 끝 (매체 하위행 없음)
            if ev == str(no_split_event):
                continue

            # ─────────────────────────
            # B) 매체 하위행 (선택된 매체만)
            # ─────────────────────────
            for m in medias:
                df_em = df_media[
                    (df_media["event_name"].astype(str) == ev) &
                    (df_media[media_col].astype(str) == str(m))
                ].copy()

                if df_em.empty:
                    cnt = pd.Series(0, index=order_cols)
                else:
                    pvt = _pivot_counts(df_em, "event_name", pivot_col, value_col, order_cols)
                    cnt = (
                        pd.to_numeric(pvt.loc[ev, order_cols], errors="coerce").fillna(0).astype(int)
                        if ev in pvt.index else pd.Series(0, index=order_cols)
                    )

                base = pd.to_numeric(cnt_total, errors="coerce").replace(0, np.nan)
                pct  = (pd.to_numeric(cnt, errors="coerce") / base * 100).replace([np.inf, -np.inf], 0).fillna(0)

                disp = [f"{_to_int_safe(cnt[c]):,} ({float(pct.loc[c]):.0f}%)" for c in order_cols]

                mi = pd.MultiIndex.from_arrays([[ev], [f"ㄴ {str(m)}"]], names=["이벤트", ""])
                disp_row = pd.DataFrame([disp], index=mi, columns=order_cols).astype(object)
                cnt_row  = pd.DataFrame([[int(cnt[c]) for c in order_cols]], index=mi, columns=order_cols).astype(int)

                frames_disp.append(disp_row)
                frames_cnt.append(cnt_row)

        if not frames_disp:
            return None, None

        return pd.concat(frames_disp, axis=0), pd.concat(frames_cnt, axis=0)

    # ──────────────────────────────────
    # 2번 영역 tabs
    # ──────────────────────────────────
    tab1_m, tab2_m, tab3_m = st.tabs(["Scroll 액션(매체)", "CTA 액션(매체)", "이후 액션(매체)"])

    with tab1_m:
        scroll_line_opts_m = [x for x in SCROLL_ROW_ORDER if x.startswith("scroll_")]
        scroll_pick = st.selectbox("그래프 표시 액션(1개)", scroll_line_opts_m, index=0, key="tab1_m_line_one")

        df_m_sc_total = df_m_total[df_m_total["event_name"].astype(str).isin(SCROLL_ROW_ORDER)].copy()
        df_m_sc_media = df_m[df_m["event_name"].astype(str).isin(SCROLL_ROW_ORDER)].copy()

        if df_m_sc_total.empty:
            st.info("집계할 데이터가 없습니다.")
        else:
            if df_m_sc_media.empty:
                st.info("선택한 매체에 해당하는 데이터가 없습니다. (표의 '전체' 행은 유지됩니다.)")
            else:
                _plot_media_stacked_with_action(
                    df_src=df_m_sc_media,
                    media_col=MEDIA_COL,
                    pivot_col=pivot_col_m,
                    value_col=value_col__m,
                    order_cols=order_cols_m,
                    action_event=scroll_pick,
                    y_scale=y_scale__m,
                    chart_key="m_tab1_trend",
                    height=360
                )

            pvt_disp_all, pvt_cnt_all = _make_event_drill_table(
                df_total=df_m_sc_total,
                df_media=df_m_sc_media,
                media_col=MEDIA_COL,
                event_order=SCROLL_ROW_ORDER,
                pivot_col=pivot_col_m,
                value_col=value_col__m,
                order_cols=order_cols_m,
                no_split_event="total_session_start",
            )

            if pvt_disp_all is None:
                st.info("집계할 데이터가 없습니다.")
            else:
                pvt_disp_show = pvt_disp_all.copy()
                pvt_disp_show.columns = col_mi_m
                pvt_cnt_show = pvt_cnt_all.copy()
                pvt_cnt_show.columns = col_mi_m
                sty = apply_heatmap(pvt_disp_show, pvt_cnt_show, HEAT_ROWS__TAB1)
                st.dataframe(sty, use_container_width=True, height=720)

    with tab2_m:
        cta_pick = st.selectbox("그래프 표시 액션(1개)", CTA_ORDER, index=0, key="tab2_m_line_one")

        # total 기준
        df_cta_total = df_m_total[df_m_total["event_name"].astype(str) == "click_cta_everyone"].copy()
        if not df_cta_total.empty:
            df_cta_total["cta_type"] = df_cta_total["cta_type"].astype(str).apply(
                lambda v: ("3. 혜택/이벤트 보기" if v in CTA_PROMO_SET else CTA_MAP.get(v, None))
            )
            df_cta_total = df_cta_total[df_cta_total["cta_type"].isin(CTA_ORDER)].copy()

        # media 기준
        df_cta_media = df_m[df_m["event_name"].astype(str) == "click_cta_everyone"].copy()
        if not df_cta_media.empty:
            df_cta_media["cta_type"] = df_cta_media["cta_type"].astype(str).apply(
                lambda v: ("3. 혜택/이벤트 보기" if v in CTA_PROMO_SET else CTA_MAP.get(v, None))
            )
            df_cta_media = df_cta_media[df_cta_media["cta_type"].isin(CTA_ORDER)].copy()

        if df_cta_total.empty:
            st.info("집계할 데이터가 없습니다.")
        else:
            df_cmp_total = df_m_total[df_m_total["event_name"].astype(str) == "cmp_session_start"].copy()
            df_cmp_media = df_m[df_m["event_name"].astype(str) == "cmp_session_start"].copy()

            df_cta2_total = df_cta_total.copy().assign(event_name=df_cta_total["cta_type"].astype(str))
            df_cta2_media = df_cta_media.copy().assign(event_name=df_cta_media["cta_type"].astype(str))

            df_cta_all_total = pd.concat([df_cmp_total.assign(event_name="cmp_session_start"), df_cta2_total], axis=0, ignore_index=True)
            df_cta_all_media = pd.concat([df_cmp_media.assign(event_name="cmp_session_start"), df_cta2_media], axis=0, ignore_index=True)

            if df_cta_all_media.empty:
                st.info("선택한 매체에 해당하는 데이터가 없습니다. (표의 '전체' 행은 유지됩니다.)")
            else:
                _plot_media_stacked_with_action(
                    df_src=df_cta_all_media,
                    media_col=MEDIA_COL,
                    pivot_col=pivot_col_m,
                    value_col=value_col__m,
                    order_cols=order_cols_m,
                    action_event=cta_pick,
                    y_scale=y_scale__m,
                    chart_key="m_tab2_trend",
                    height=360
                )

            CTA_ROW_ORDER_FULL = ["cmp_session_start"] + CTA_ORDER
            pvt_disp_all, pvt_cnt_all = _make_event_drill_table(
                df_total=df_cta_all_total,
                df_media=df_cta_all_media,
                media_col=MEDIA_COL,
                event_order=CTA_ROW_ORDER_FULL,
                pivot_col=pivot_col_m,
                value_col=value_col__m,
                order_cols=order_cols_m,
                no_split_event="total_session_start",  # 사실상 없음
            )

            pvt_disp_show = pvt_disp_all.copy()
            pvt_disp_show.columns = col_mi_m
            pvt_cnt_show = pvt_cnt_all.copy()
            pvt_cnt_show.columns = col_mi_m
            sty = apply_heatmap(pvt_disp_show, pvt_cnt_show, HEAT_ROWS__TAB2)
            st.dataframe(sty, use_container_width=True, height=720)

    with tab3_m:
        AFTER_ACTION_ORDER = [
            "cmp_session_start",
            "view_item",
            "view_item_list",
            "product_page_scroll_25",
            "product_option_price",
            "find_nearby_showroom",
            "showroom_10s",
            "showroom_leads",
            "add_to_cart",
        ]
        after_pick = st.selectbox("그래프 표시 액션(1개)", [x for x in AFTER_ACTION_ORDER if x != "cmp_session_start"], index=0, key="tab3_m_line_one")

        df_m_af_total = df_m_total[df_m_total["event_name"].astype(str).isin(AFTER_ACTION_ORDER)].copy()
        df_m_af_media = df_m[df_m["event_name"].astype(str).isin(AFTER_ACTION_ORDER)].copy()

        if df_m_af_total.empty:
            st.info("집계할 데이터가 없습니다.")
        else:
            if df_m_af_media.empty:
                st.info("선택한 매체에 해당하는 데이터가 없습니다. (표의 '전체' 행은 유지됩니다.)")
            else:
                _plot_media_stacked_with_action(
                    df_src=df_m_af_media,
                    media_col=MEDIA_COL,
                    pivot_col=pivot_col_m,
                    value_col=value_col__m,
                    order_cols=order_cols_m,
                    action_event=after_pick,
                    y_scale=y_scale__m,
                    chart_key="m_tab3_trend",
                    height=360
                )

            pvt_disp_all, pvt_cnt_all = _make_event_drill_table(
                df_total=df_m_af_total,
                df_media=df_m_af_media,
                media_col=MEDIA_COL,
                event_order=AFTER_ACTION_ORDER,
                pivot_col=pivot_col_m,
                value_col=value_col__m,
                order_cols=order_cols_m,
                no_split_event="total_session_start",
            )

            pvt_disp_show = pvt_disp_all.copy()
            pvt_disp_show.columns = col_mi_m
            pvt_cnt_show = pvt_cnt_all.copy()
            pvt_cnt_show.columns = col_mi_m
            sty = apply_heatmap(pvt_disp_show, pvt_cnt_show, HEAT_ROWS__TAB3)
            st.dataframe(sty, use_container_width=True, height=720)


if __name__ == "__main__":
    main()
