# SEOHEE
# 2026-03-10 ver.

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta, date
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
                "res_cnt","visit_total", "visit_reserved", "visit_reserved_rate", "visit_reserved_rate_2","visit_walkin", "visit_walkin_rate",]

COLS_LABELMAP = {
    "look_cnt": "조회",
    "res_cnt": "예약",
    "bookreq_cnt": "예약신청",
    "visit_reserved": "방문(예약)",
    "visit_walkin": "방문(워크인)",
    "visit_total": "총방문",
    "bookreq_rate": "예약신청률",
    "visit_walkin_rate": "방문(워크인) 비율",
    "visit_reserved_rate": "방문(예약) 비율",
    "visit_reserved_rate_2": "예약 이행률",
}

ALL_LABEL = "전체"


# ──────────────────────────────────
# HELPER
# ──────────────────────────────────
def parse_shrm(df: pd.DataFrame) -> pd.DataFrame:
    # 쇼룸 이름에서 유형, 권역, 지점 파싱
    if "shrm_name" in df.columns:
        ss = (
            df["shrm_name"]
            .astype("string")
            .fillna("")
            .astype(str)
            .str.strip()
        )
        parts = ss.str.split("_", n=2, expand=True)
        df["shrm_type"] = parts[0].fillna("").str.strip()
        df["shrm_branch"] = parts[1].fillna("").str.strip()
        df["shrm_region"] = parts[2].fillna("").str.strip()
    else:
        df["shrm_type"] = ""
        df["shrm_branch"] = ""
        df["shrm_region"] = ""

    return df


def get_dim_options(df: pd.DataFrame, col: str, *, all_label: str = "전체") -> list[str]:
    # 지정한 컬럼에서 빈값/결측 제외
    # [전체] + 빈도순 옵션 생성
    if df is None or df.empty or col not in df.columns:
        return [all_label]

    s = df[col].astype("string").fillna("").astype(str).str.strip()
    s = s[s != ""]
    o = s.value_counts(dropna=False).index.astype(str).tolist()
    return [all_label] + o


def filter_dim(df: pd.DataFrame, dim_col: str, dim_val: str, *, all_label: str = "전체") -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if dim_val == all_label:
        return df

    s = df[dim_col].astype("string").fillna("").astype(str).str.strip()
    return df[s == str(dim_val).strip()].copy()


def _safe_sum(_df: pd.DataFrame, col: str) -> float:
    if _df is None or _df.empty or col not in _df.columns:
        return 0.0
    return float(pd.to_numeric(_df[col], errors="coerce").fillna(0).sum())


def _safe_rate(n: float, d: float) -> float:
    return (n / d * 100.0) if d > 0 else 0.0


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
            "https://docs.google.com/spreadsheets/d/1g2HWpm3Le3t3P3Hb9nm2owoiaxywaXv--L0SHEDx3rQ/edit"
        )
        # 2개의 시트 필요함. 
        df1 = pd.DataFrame(sh.worksheet("shrm_data").get_all_records())
        df2 = pd.DataFrame(sh.worksheet("shrm_nplace").get_all_records())
        
        # 전처리
        
        # (pass) {쇼룸 유형}_{쇼룸 지점}_{쇼룸 권역}, 쇼룸 정보가 없으면 라인 삭제
        df1 = parse_shrm(df1)
        df2 = parse_shrm(df2)
        df1 = df1[df1["shrm_name"].astype("string").fillna("").str.strip() != ""]
        df2 = df2[df2["shrm_name"].astype("string").fillna("").str.strip() != ""]

        # (pass) 카테고리컬 변환
        for d in [df1, df2]:
            for c in COLS_CATEGORICAL:
                if c in d.columns:
                    d[c] = d[c].astype("category")

        # (pass) 날짜 깨짐 방지
        if "event_date" in df1.columns:
            df1["event_date"] = pd.to_datetime(df1["event_date"], errors="coerce").dt.normalize()
        if "event_date" in df2.columns:
            df2["event_date"] = pd.to_datetime(df2["event_date"], errors="coerce").dt.normalize()

        return df1, df2

    # ──────────────────────────────────
    # C-1) tb_max -> get max date
    # ──────────────────────────────────
    # (pass)


    # ──────────────────────────────────
    # C-2) Progress Bar
    # ──────────────────────────────────
    df1, df2 = load_data()
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
            쇼룸 시트와 네이버 Place 데이터를 기반으로 <b>조회부터 방문까지의 고객 현황 및 데모그래픽</b>을 확인하는 대시보드입니다.<br> 
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
    # 전처리 - 일단 ...
    # ──────────────────────────────────
    today = datetime.now().date()
    _def_s = today - timedelta(days=14)  # 시작일
    _def_e = today - timedelta(days=1)   # 종료일
    _min_d = today - timedelta(days=365)
    _max_d = today + timedelta(days=365)


    # ──────────────────────────────────
    # 1) 전체 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>전체 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ온라인 조회·신청·예약 퍼널과 오프라인 방문(예약방문/워크인) 성과를 함께 확인합니다.", unsafe_allow_html=True)
    st.markdown(
        """
        <style>
        .flow-kpi-card{background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:14px 16px;}
        .flow-kpi-title{font-size:15px;color:#64748b;margin:0 0 8px}
        .flow-kpi-value{font-size:25px;font-weight:500;line-height:1.05;margin:0;white-space:nowrap}
        .flow-kpi-main{
            display:flex;
            align-items:flex-end;
            justify-content:space-between;
            gap:10px;
            flex-wrap:nowrap;
        }
        .flow-kpi-meta{
            display:flex;
            align-items:center;
            gap:8px;
            flex-wrap:wrap;
            justify-content:flex-end;
        }
        .flow-kpi-meta-item{
            font-size:13px;
            color:#6c757d;
            white-space:nowrap;
            line-height:1.2;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # 영역 함수
    def _build_long_df1(df1: pd.DataFrame) -> pd.DataFrame:
        """
        - df1 데이터 -> long 데이터
        - visit_type 값은 '예약' 또는 '워크인' 이어야 함
        - 생성 컬럼 : event_type, cnt
        """
        base = [c for c in COLS_SHOWROOM if c in df1.columns]
        out = df1.loc[:, base + ["visit_type"]]

        out["visit_type"] = out["visit_type"].astype("string").fillna("").str.strip()
        out = out[out["visit_type"].isin(["예약", "워크인"])]
        out["event_type"] = out["visit_type"].map({
            "예약": "visit_reserved",
            "워크인": "visit_walkin",
        })
        out["cnt"] = 1

        out = (out.groupby(base + ["event_type"], dropna=False, as_index=False)["cnt"].sum())
        return out

    def _build_long_df2(df2: pd.DataFrame) -> pd.DataFrame:
        """
        - df2 데이터 -> long 데이터
        - 생성 컬럼 : event_type, cnt
        """
        base = [c for c in COLS_SHOWROOM if c in df2.columns]
        m_cols = [c for c in COLS_BASICSRC if c in df2.columns]

        out = df2.loc[:, base + m_cols]

        if not m_cols:
            return pd.DataFrame(columns=base + ["event_type", "cnt"])

        out[m_cols] = out[m_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        out = out.melt(
            id_vars=base,
            value_vars=m_cols,
            var_name="event_type",
            value_name="cnt"
        )
        out["cnt"] = pd.to_numeric(out["cnt"], errors="coerce").fillna(0)
        return out

    def build_total_table(
        df: pd.DataFrame,
        start_dt,
        end_dt,
        dim_col: str | None = None,
        dim_val: str | None = None,
    ) -> pd.DataFrame:

        daily = df[
            (df["event_date"] >= pd.to_datetime(start_dt)) &
            (df["event_date"] <= pd.to_datetime(end_dt))
        ]

        if dim_col and dim_col in daily.columns and dim_val not in (None, ALL_LABEL):
            daily = daily[daily[dim_col].astype("string").fillna("").str.strip() == str(dim_val)]

        daily = (
            daily.groupby(["event_date", "event_type"], dropna=False)["cnt"]
            .sum()
            .reset_index()
            .pivot(index="event_date", columns="event_type", values="cnt")
            .fillna(0)
            .reset_index()
            .rename(columns={"event_date": "날짜"})
            .sort_values("날짜")
            .reset_index(drop=True)
        )

        for c in COLS_TOTALSRC:
            if c not in daily.columns:
                daily[c] = 0

        daily[COLS_TOTALSRC] = daily[COLS_TOTALSRC].apply(pd.to_numeric, errors="coerce").fillna(0)

        daily["visit_total"] = daily["visit_reserved"] + daily["visit_walkin"] # 총방문
        daily["visit_walkin_rate"]   = np.where(daily["visit_total"] > 0, daily["visit_walkin"] / daily["visit_total"] * 100, 0) # 방문(워크인) 비율
        daily["visit_reserved_rate"] = np.where(daily["visit_total"] > 0, daily["visit_reserved"] / daily["visit_total"] * 100, 0) # 방문(예약) 비율
        daily["visit_reserved_rate_2"] = np.where(daily["res_cnt"] > 0, daily["visit_reserved"] / daily["res_cnt"] * 100, 0) # 예약 이행률
        daily["bookreq_rate"] = np.where(daily["look_cnt"] > 0, daily["bookreq_cnt"] / daily["look_cnt"] * 100, 0) # 예약 신청률

        daily = daily[[c for c in COLS_TOTAL if c in daily.columns]]
        
        return daily

    def render_total_card(df: pd.DataFrame):
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
        kpi["visit_reserved_rate_2"] = _safe_rate(kpi["visit_reserved"], kpi["res_cnt"])
        kpi["visit_walkin_rate"] = _safe_rate(kpi["visit_walkin"], kpi["visit_total"])

        cards = [
            {
                "title": "조회수",
                "value": f'{kpi["look_cnt"]:,.0f}',
                "meta": [],
            },
            {
                "title": "예약신청수",
                "value": f'{kpi["bookreq_cnt"]:,.0f}',
                "meta": [f'신청 비중 {kpi["bookreq_rate"]:.1f}%'],
            },
            {
                "title": "예약이용수",
                "value": f'{kpi["res_cnt"]:,.0f}',
                "meta": [f'노쇼 비중 {100 - kpi["visit_reserved_rate_2"]:.1f}%'],
            },
            {
                "title": "방문수",
                "value": f'{kpi["visit_total"]:,.0f}',
                "meta": [],
            },
            {
                "title": "방문(예약)수",
                "value": f'{kpi["visit_reserved"]:,.0f}',
                "meta": [f'예약 비중 {kpi["visit_reserved_rate"]:.1f}%'],
            },
            {
                "title": "방문(워크인)수",
                "value": f'{kpi["visit_walkin"]:,.0f}',
                "meta": [f'워크인 비중 {kpi["visit_walkin_rate"]:.1f}%'],
            },
        ]

        cols = st.columns(6, vertical_alignment="top")
        for col, card in zip(cols, cards):
            with col:
                meta_html = ""
                if card["meta"]:
                    meta_html = "".join(
                        [f'<span class="flow-kpi-meta-item">{txt}</span>' for txt in card["meta"]]
                    )

                html = f"""
                <div class="flow-kpi-card">
                    <div class="flow-kpi-title">{card["title"]}</div>
                    <div class="flow-kpi-main">
                        <div class="flow-kpi-value">{card["value"]}</div>
                        <div class="flow-kpi-meta">{meta_html}</div>
                    </div>
                </div>
                """

                st.markdown(html, unsafe_allow_html=True)

        st.markdown(" ")
        return kpi

    def render_total_graph(daily: pd.DataFrame, tab_key: str):
        g1, _p, g2 = st.columns([1, 0.03, 1], vertical_alignment="top")
        with g1:
            st.markdown("""
                        <h6 style="margin:0;">📊 온라인 관심도 추이</h6>
                        <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">조회수와 예약신청수의 일자별 흐름입니다.</p>
                        """,
                        unsafe_allow_html=True)
            fig_online = go.Figure()
            fig_online.add_trace(go.Scatter(x=daily["날짜"], y=daily["look_cnt"], mode="lines+markers", name="조회", yaxis="y1"))
            fig_online.add_trace(go.Scatter(x=daily["날짜"], y=daily["bookreq_rate"], mode="lines+markers", name="예약신청률", yaxis="y2"))
            fig_online.update_layout(
                height=300,
                margin=dict(l=10, r=10, t=10, b=10),
                yaxis=dict(title="조회"),
                yaxis2=dict(title="예약신청률", overlaying="y", side="right", showgrid=False),
                legend=dict(orientation="h", y=1.08, x=1, xanchor="right",),
            )
            st.plotly_chart(fig_online, use_container_width=True, key=f"flow_online_{tab_key}")

        with g2:
            st.markdown("""
                        <h6 style="margin:0;">📊 오프라인 성과 추이</h6>
                        <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">실제 방문(예약방문/워크인)과 사전 예약 추이입니다.</p>
                        """,
                        unsafe_allow_html=True)
            fig_offline = go.Figure()
            fig_offline.add_trace(go.Bar(x=daily["날짜"], y=daily["visit_reserved"], name="예약방문"))
            fig_offline.add_trace(go.Bar(x=daily["날짜"], y=daily["visit_walkin"], name="워크인"))
            fig_offline.add_trace(go.Scatter(x=daily["날짜"], y=daily["res_cnt"], mode="lines+markers", name="예약",)) # yaxis="y2"
            fig_offline.update_layout(
                barmode="stack",
                height=300,
                margin=dict(l=10, r=10, t=10, b=10),
                yaxis=dict(title="방문"),
                yaxis2=dict(title="예약", overlaying="y", side="right", showgrid=False),
                legend=dict(orientation="h", y=1.08, x=1, xanchor="right",),
            )
            st.plotly_chart(fig_offline, use_container_width=True, key=f"flow_offline_{tab_key}")

    def render_total_table(daily: pd.DataFrame):
        tbl = daily.copy()
        tbl["날짜"] = pd.to_datetime(tbl["날짜"], errors="coerce").dt.strftime("%Y-%m-%d")

        tbl = (
            tbl.set_index("날짜")
            .T
            .reset_index()
            .rename(columns={"index": "지표"})
        )
        st.dataframe(tbl, use_container_width=True, row_height=30, hide_index=True)


    # 데이터 생성
    v = _build_long_df1(df1)
    m = _build_long_df2(df2)
    df = pd.concat([v, m], ignore_index=True)
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.normalize()
    df["cnt"] = pd.to_numeric(df["cnt"], errors="coerce").fillna(0)
    df = df.dropna(subset=["event_date"])

    # 렌더링
    with st.expander("공통 Filter", expanded=False):
        f1, f2 = st.columns([1.4, 2], vertical_alignment="bottom")
        with f1:
            flow_date = st.date_input(
                "기간 선택",
                value=[_def_s, _def_e],
                min_value=_min_d,
                max_value=_max_d,
                key="flow_date_rng",
            )

    if isinstance(flow_date, (list, tuple)) and len(flow_date) == 2:
        flow_start, flow_end = flow_date
    else:
        flow_start, flow_end = _def_s, _def_e

    top_tabs = st.tabs(["지점별", "권역별", "유형별"])

    with top_tabs[0]:
        branch_opts = get_dim_options(df, "shrm_branch", all_label=ALL_LABEL)
        sel_branch = st.selectbox("", branch_opts, index=0, label_visibility="collapsed", key="flow_branch_sel")

        daily = build_total_table(df, flow_start, flow_end, dim_col="shrm_branch", dim_val=sel_branch)
        render_total_card(daily)
        render_total_graph(daily, tab_key=f"branch_{sel_branch}")
        render_total_table(daily)

    with top_tabs[1]:
        region_opts = [x for x in get_dim_options(df, "shrm_region", all_label=ALL_LABEL) if x != ALL_LABEL]
        sub_tabs = st.tabs(region_opts)
        for i, opt in enumerate(region_opts):
            with sub_tabs[i]:
                daily = build_total_table(df, flow_start, flow_end, dim_col="shrm_region", dim_val=opt)
                render_total_card(daily)
                render_total_graph(daily, tab_key=f"region_{opt}")
                render_total_table(daily)

    with top_tabs[2]:
        type_opts = [x for x in get_dim_options(df, "shrm_type", all_label=ALL_LABEL) if x != ALL_LABEL]
        sub_tabs = st.tabs(type_opts)
        for i, opt in enumerate(type_opts):
            with sub_tabs[i]:
                daily = build_total_table(df, flow_start, flow_end, dim_col="shrm_type", dim_val=opt)
                render_total_card(daily)
                render_total_graph(daily, tab_key=f"type_{opt}")
                render_total_table(daily)


    # ──────────────────────────────────
    # 2) 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>📅 예약 분포 현황</h5>", unsafe_allow_html=True)
    st.info("제작중, 지점별 미래 예약 현황 + 기존 데이터 기반 워크인 예측 = 방문수")

    # # 1. 영역 1에서 계산된 df(병합 데이터) 및 날짜 변수 활용
    # curr_weekday = today.weekday() 
    # this_week_start = today - timedelta(days=curr_weekday)
    # next_week_end = this_week_start + timedelta(days=13)

    # with st.expander("예약 분포 Filter", expanded=False):
    #     res_date_rng = st.date_input(
    #         "조회 기간",
    #         value=[this_week_start, next_week_end],
    #         min_value=_min_d,
    #         max_value=_max_d,
    #         key="res_dist_date"
    #     )

    # if isinstance(res_date_rng, (list, tuple)) and len(res_date_rng) == 2:
    #     r_start, r_end = res_date_rng
    # else:
    #     r_start, r_end = this_week_start, next_week_end

    # # 영역 1의 get_dim_options 재사용
    # res_branch_opts = get_dim_options(df, "shrm_branch", all_label=ALL_LABEL)
    # sel_res_branch = st.selectbox("", res_branch_opts, index=0, label_visibility="collapsed", key="res_branch_sel")

    # # 영역 1의 핵심 함수 build_total_table 재사용
    # # 이 함수는 내부적으로 날짜/지점 필터링 및 res_cnt 집계를 모두 수행합니다.
    # res_daily = build_total_table(df, r_start, r_end, dim_col="shrm_branch", dim_val=sel_res_branch)

    # # 3. 차트 시각화 (영역 1의 Plotly 스타일 재사용)
    # fig_res = go.Figure()
    # fig_res.add_trace(go.Bar(
    #     x=res_daily["날짜"], 
    #     y=res_daily["res_cnt"],
    #     marker_color="#636EFA",
    #     name="예약 수",
    #     text=res_daily["res_cnt"],
    #     textposition='auto',
    # ))
    # fig_res.update_layout(
    #     height=350,
    #     margin=dict(l=10, r=10, t=30, b=10),
    #     xaxis=dict(tickformat="%m-%d (%a)"),
    #     yaxis=dict(title="예약 수"),
    #     hovermode="x unified"
    # )
    # st.plotly_chart(fig_res, use_container_width=True, key="res_dist_chart_v2")
    
    # render_total_table(res_daily[["날짜", "res_cnt"]])



    # ──────────────────────────────────
    # 3) 방문 고객 특성 요약
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>방문 고객 특성 요약</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    # 영역 함수
    def build_block(
        df: pd.DataFrame,
        row_col: str,
        val_col: str | None = None,
        round_n: int = 0,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        if df is None or df.empty or row_col not in df.columns:
            return pd.DataFrame(), pd.DataFrame()

        cols = ["event_date", row_col] + ([val_col] if val_col else [])
        d = df[cols].copy()

        d["event_date"] = pd.to_datetime(d["event_date"], errors="coerce")
        d = d[d["event_date"].notna()]

        d[row_col] = (
            d[row_col]
            .astype("string")
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", "기타")
        )

        if d.empty:
            return pd.DataFrame(), pd.DataFrame()

        d["event_date"] = d["event_date"].dt.strftime("%Y-%m-%d")

        if val_col:
            d[val_col] = pd.to_numeric(d[val_col], errors="coerce").fillna(0)
            g = (
                d.groupby(["event_date", row_col], dropna=False)[val_col]
                .sum()
                .reset_index(name="value")
            )
        else:
            g = (
                d.groupby(["event_date", row_col], dropna=False)
                .size()
                .reset_index(name="value")
            )

        g["value"] = pd.to_numeric(g["value"], errors="coerce").fillna(0)
        g = g.sort_values(["event_date", row_col]).reset_index(drop=True)
        g["cum_value"] = g.groupby(row_col, dropna=False)["value"].cumsum()

        if round_n == 0:
            g["value"] = g["value"].round(0).astype(int)
            g["cum_value"] = g["cum_value"].round(0).astype(int)
            pt = (
                g.pivot(index=row_col, columns="event_date", values="value")
                .fillna(0)
                .astype(int)
                .reset_index()
            )
        else:
            pt = (
                g.pivot(index=row_col, columns="event_date", values="value")
                .fillna(0)
                .round(round_n)
                .reset_index()
            )

        return g, pt

    def render_block(df_base: pd.DataFrame, df_aw_base: pd.DataFrame, key_tag: str):
        cfgs = [
            ("연령대", df_base, "demo_age", None, 0),
            ("구매목적", df_base, "purchase_purpose", None, 0),
            ("인지단계", df_aw_base, "awareness_type_a", "weight", 0),
            ("인지채널", df_aw_base, "awareness_type_b", "weight", 0),
        ]

        r1c1, _p, r1c2 = st.columns([1, 0.03, 1], vertical_alignment="top")
        r2c1, _p, r2c2 = st.columns([1, 0.03, 1], vertical_alignment="top")
        cols = [r1c1, r1c2, r2c1, r2c2]

        for col, (ttl, src, row_col, val_col, round_n) in zip(cols, cfgs):
            g, pt = build_block(src, row_col=row_col, val_col=val_col, round_n=round_n)

            with col:
                st.markdown(f"""
                            <h6 style="margin:0;">📊 {ttl}</h6>
                            <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">설명</p>
                            """,
                            unsafe_allow_html=True)

                if g.empty:
                    st.warning("표시할 데이터가 없습니다.")
                    continue

                fig = px.bar(
                    g,
                    x="event_date",
                    y="value",
                    color=row_col,
                    barmode="stack",
                )
                fig.update_layout(
                    height=240,
                    margin=dict(l=0, r=0, t=10, b=10),
                    legend_title=None,
                )
                fig.update_xaxes(title=None)
                fig.update_yaxes(title=None)
                st.plotly_chart(
                    fig,
                    use_container_width=True,
                    key=f"cum::{key_tag}::{ttl}",
                )

                st.dataframe(pt, use_container_width=True, hide_index=True, row_height=30, height=200)

    def render_block_card(df_base: pd.DataFrame, df_aw_base: pd.DataFrame):
        def _top2_share(
            df: pd.DataFrame,
            col: str,
            val_col: str | None = None,
        ) -> tuple[str, str]:
            if df is None or df.empty or col not in df.columns:
                return "-", "-"

            d = df[[col] + ([val_col] if val_col else [])]
            d[col] = (
                d[col]
                .astype("string")
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", "기타")
            )

            if val_col:
                d[val_col] = pd.to_numeric(d[val_col], errors="coerce").fillna(0)
                g = (
                    d.groupby(col, dropna=False)[val_col]
                    .sum()
                    .reset_index(name="value")
                )
            else:
                g = (
                    d.groupby(col, dropna=False)
                    .size()
                    .reset_index(name="value")
                )

            if g.empty:
                return "-", "-"

            g["value"] = pd.to_numeric(g["value"], errors="coerce").fillna(0)
            g = g.sort_values("value", ascending=False).reset_index(drop=True)

            total = g["value"].sum()
            if total <= 0:
                return "-", "-"

            g["pct"] = (g["value"] / total * 100).round(0)

            top1 = f'{g.iloc[0][col]} {g.iloc[0]["pct"]:.0f}%'
            top2 = f'{g.iloc[1][col]} {g.iloc[1]["pct"]:.0f}%' if len(g) > 1 else "-"

            return top1, top2

        cards = [
            ("연령대",) + _top2_share(df_base, "demo_age"),
            ("구매목적",) + _top2_share(df_base, "purchase_purpose"),
            ("인지단계",) + _top2_share(df_aw_base, "awareness_type_a", "weight"),
            ("인지채널",) + _top2_share(df_aw_base, "awareness_type_b", "weight"),
        ]

        cols = st.columns(4, vertical_alignment="top")
        for col, (title, value, meta) in zip(cols, cards):
            with col:
                meta_html = f'<span class="flow-kpi-meta-item">{meta}</span>' if meta != "-" else ""

                html = f"""
                <div class="flow-kpi-card">
                    <div class="flow-kpi-title">{title}</div>
                    <div class="flow-kpi-main">
                        <div class="flow-kpi-value">{value}</div>
                        <div class="flow-kpi-meta">{meta_html}</div>
                    </div>
                </div>
                """
                st.markdown(html, unsafe_allow_html=True)

        st.markdown(" ")

    # 데이터 생성
    df1 = df1[["event_date","shrm_branch","shrm_region","shrm_type","demo_gender","demo_age","purchase_purpose","awareness_type",]]
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
        df_aw = df_aw[["event_date","shrm_branch","shrm_region","shrm_type","awareness_type","awareness_type_a","awareness_type_b", "weight",]]
    
    # 렌더링
    with st.expander("공통 Filter", expanded=False):
        f1, f2 = st.columns([1.4, 2], vertical_alignment="bottom")
        with f1:
            flow_date = st.date_input(
                "기간 선택",
                value=[_def_s, _def_e],
                min_value=_min_d,
                max_value=_max_d,
                key="wnd",
            )

    if isinstance(flow_date, (list, tuple)) and len(flow_date) == 2:
        flow_start, flow_end = flow_date
    else:
        flow_start, flow_end = _def_s, _def_e

    # ???
    flow_start = pd.to_datetime(flow_start)
    flow_end = pd.to_datetime(flow_end)
    df1["event_date"] = pd.to_datetime(df1["event_date"], errors="coerce")
    df1_v = df1[(df1["event_date"] >= flow_start) & (df1["event_date"] <= flow_end)].copy()
    if df_aw is not None and not df_aw.empty:
        df_aw["event_date"] = pd.to_datetime(df_aw["event_date"], errors="coerce")
        df_aw_v = df_aw[(df_aw["event_date"] >= flow_start) & (df_aw["event_date"] <= flow_end)].copy()
    else:
        df_aw_v = None

    top_tabs = st.tabs(["지점별", "권역별", "유형별"])

    with top_tabs[0]:
        branch_opts = get_dim_options(df1_v, "shrm_branch", all_label="전체")
        sel_branch = st.selectbox(
            "",
            branch_opts,
            index=0,
            label_visibility="collapsed",
            key="detail_branch_sel",
        )
        df1_f = filter_dim(df1_v, "shrm_branch", sel_branch, all_label="전체")
        df_aw_f = filter_dim(df_aw_v, "shrm_branch", sel_branch, all_label="전체") if df_aw_v is not None else None
        render_block_card(df1_f, df_aw_f)
        render_block(df1_f, df_aw_f, key_tag=f"branch::{sel_branch}")

    with top_tabs[1]:
        region_opts = [x for x in get_dim_options(df1_v, "shrm_region", all_label="전체") if x != "전체"]
        sub_tabs = st.tabs(region_opts)
        for i, opt in enumerate(region_opts):
            with sub_tabs[i]:
                df1_f = filter_dim(df1_v, "shrm_region", opt, all_label="전체")
                df_aw_f = filter_dim(df_aw_v, "shrm_region", opt, all_label="전체") if df_aw_v is not None else None
                render_block_card(df1_f, df_aw_f)
                render_block(df1_f, df_aw_f, key_tag=f"region::{opt}")

    with top_tabs[2]:
        type_opts = [x for x in get_dim_options(df1_v, "shrm_type", all_label="전체") if x != "전체"]
        sub_tabs = st.tabs(type_opts)
        for i, opt in enumerate(type_opts):
            with sub_tabs[i]:
                df1_f = filter_dim(df1_v, "shrm_type", opt, all_label="전체")
                df_aw_f = filter_dim(df_aw_v, "shrm_type", opt, all_label="전체") if df_aw_v is not None else None
                render_block_card(df1_f, df_aw_f)
                render_block(df1_f, df_aw_f, key_tag=f"type::{opt}")

    # ──────────────────────────────────
    # 4) 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>cross insight</h5>", unsafe_allow_html=True)
    st.info("제작중, 이관 예정")


if __name__ == "__main__":
    main()
