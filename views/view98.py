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
    # 5) 지점별 퍼포먼스 사분면 분석 (온라인 조회 vs 오프라인 방문)
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>🎯 지점별 퍼포먼스 사분면 분석</h5>", unsafe_allow_html=True)
    st.markdown(
        ":gray-badge[:material/Info: Info]ㅤ온라인 관심도(네이버 플레이스 조회수)와 실제 매장 방문수를 교차 분석하여, "
        "각 지점의 현재 위치와 개선 포인트를 4가지 그룹으로 진단합니다.", 
        unsafe_allow_html=True
    )

    with st.expander("사분면 분석 Filter", expanded=True):
        q_f1, q_f2, q_f3 = st.columns([1.4, 1.3, 1.3], vertical_alignment="bottom")
        with q_f1:
            q_date = st.date_input("조회 기간", value=[_def_s, _def_e], min_value=_min_d, max_value=_max_d, key="q_date")
        with q_f2:
            q_region = st.selectbox("쇼룸 권역", get_dim_options(df1, "shrm_region", all_label="전체"), key="q_region")
        with q_f3:
            q_type = st.selectbox("쇼룸 유형", get_dim_options(df1, "shrm_type", all_label="전체"), key="q_type")

    if isinstance(q_date, (list, tuple)) and len(q_date) == 2:
        q_start, q_end = q_date
    else:
        q_start, q_end = _def_s, _def_e

    q_start = pd.to_datetime(q_start)
    q_end = pd.to_datetime(q_end)

    # df2(네이버 플레이스 - 조회수) 필터링 및 집계
    df2_q = df2[(pd.to_datetime(df2["event_date"], errors="coerce") >= q_start) & 
                (pd.to_datetime(df2["event_date"], errors="coerce") <= q_end)].copy()
    
    if q_region != "전체":
        df2_q = df2_q[df2_q["shrm_region"].astype(str).str.strip() == str(q_region)]
    if q_type != "전체":
        df2_q = df2_q[df2_q["shrm_type"].astype(str).str.strip() == str(q_type)]

    df2_agg = df2_q.groupby("shrm_branch", dropna=False)["look_cnt"].sum().reset_index()
    df2_agg["look_cnt"] = pd.to_numeric(df2_agg["look_cnt"], errors="coerce").fillna(0)

    # df1(실제 방문) 필터링 및 집계
    df1_q = df1[(df1["event_date"] >= q_start) & (df1["event_date"] <= q_end)].copy()
    
    if q_region != "전체":
        df1_q = df1_q[df1_q["shrm_region"].astype(str).str.strip() == str(q_region)]
    if q_type != "전체":
        df1_q = df1_q[df1_q["shrm_type"].astype(str).str.strip() == str(q_type)]

    # df1은 방문자 로우 데이터이므로 size()로 방문 건수를 집계
    df1_agg = df1_q.groupby("shrm_branch", dropna=False).size().reset_index(name="visit_cnt")

    # 병합
    df_quad = pd.merge(df2_agg, df1_agg, on="shrm_branch", how="inner")
    df_quad = df_quad[(df_quad["shrm_branch"].astype(str).str.strip() != "") & (df_quad["shrm_branch"].notna())]

    if df_quad.empty or len(df_quad) < 2:
        st.warning("분석할 지점 데이터가 충분하지 않습니다. (최소 2개 지점 이상 필요)")
    else:
        # 중앙값(Median)을 기준으로 사분면 분할
        x_mid = df_quad["look_cnt"].median()
        y_mid = df_quad["visit_cnt"].median()

        # 그룹 라벨링
        def assign_quadrant(row):
            if row["look_cnt"] >= x_mid and row["visit_cnt"] >= y_mid:
                return "1사분면 (스타 지점: 온라인/오프라인 모두 우수)"
            elif row["look_cnt"] < x_mid and row["visit_cnt"] >= y_mid:
                return "2사분면 (오프라인 강세: 단골/입지 우수, 온라인 홍보 필요)"
            elif row["look_cnt"] < x_mid and row["visit_cnt"] < y_mid:
                return "3사분면 (개선 필요: 전반적인 트래픽 부족)"
            else:
                return "4사분면 (온라인 강세: 조회는 많으나 방문 전환율 부족)"

        df_quad["quadrant"] = df_quad.apply(assign_quadrant, axis=1)

        # 차트 렌더링
        fig_quad = px.scatter(
            df_quad,
            x="look_cnt",
            y="visit_cnt",
            text="shrm_branch",
            color="quadrant",
            color_discrete_map={
                "1사분면 (스타 지점: 온라인/오프라인 모두 우수)": "#2ECA6A",
                "2사분면 (오프라인 강세: 단골/입지 우수, 온라인 홍보 필요)": "#FFA15A",
                "3사분면 (개선 필요: 전반적인 트래픽 부족)": "#EF553B",
                "4사분면 (온라인 강세: 조회는 많으나 방문 전환율 부족)": "#636EFA"
            },
            custom_data=["shrm_branch", "look_cnt", "visit_cnt", "quadrant"]
        )

        fig_quad.update_traces(
            textposition='top center',
            marker=dict(size=12, opacity=0.8, line=dict(width=1, color='DarkSlateGrey')),
            hovertemplate="<b>%{customdata[0]}</b><br>온라인 조회수: %{customdata[1]:,.0f}회<br>오프라인 방문: %{customdata[2]:,.0f}건<br>진단: %{customdata[3]}<extra></extra>"
        )

        fig_quad.add_hline(y=y_mid, line_dash="dash", line_color="#8B92A0", annotation_text="방문수 중앙값", annotation_position="bottom right")
        fig_quad.add_vline(x=x_mid, line_dash="dash", line_color="#8B92A0", annotation_text="조회수 중앙값", annotation_position="top left")

        fig_quad.update_layout(
            height=500,
            margin=dict(l=20, r=20, t=30, b=20),
            xaxis_title="온라인 관심도 (네이버 플레이스 조회수)",
            yaxis_title="오프라인 성과 (실제 총 방문수)",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.2,
                xanchor="center",
                x=0.5,
                title=None
            )
        )

        st.plotly_chart(fig_quad, use_container_width=True, key="quad_chart")


    # ──────────────────────────────────
    # 2) 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>📅 예약 분포 현황</h5>", unsafe_allow_html=True)
    st.warning("제작중")
    # 예약 현황 + 기존 데이터 기반 워크인 예측 => 방문 예측

    # # 날짜
    # today = datetime.now().date()
    # _def_s2 = today - timedelta(days=0)  # 시작일
    # _def_e2 = today + timedelta(days=13)   # 종료일

    # with st.expander("공통 Filter", expanded=False):
    #     res_date_rng = st.date_input(
    #         "조회 기간",
    #         value=[_def_s2, _def_e2],
    #         min_value=_min_d,
    #         max_value=_max_d,
    #         key="res_dist_date"
    #     )

    # if isinstance(res_date_rng, (list, tuple)) and len(res_date_rng) == 2:
    #     r_start, r_end = res_date_rng
    # else:
    #     r_start, r_end = _def_s2, _def_e2

    # # 영역 1의 get_dim_options 재사용
    # res_branch_opts = get_dim_options(df, "shrm_branch", all_label=ALL_LABEL)
    # sel_res_branch = st.selectbox("", res_branch_opts, index=0, label_visibility="collapsed", key="res_branch_sel")

    # # 데이터
    # res_daily = build_total_table(df, r_start, r_end, dim_col="shrm_branch", dim_val=sel_res_branch)

    # # 가중평균
    # def predict_walkin_same_dow(hist_df, target_dates, value_col="visit_walkin", n_weeks=8):
    #     hist_df["날짜"] = pd.to_datetime(hist_df["날짜"]).dt.date
    #     hist_df = hist_df.sort_values("날짜")

    #     preds = []
    #     for d in pd.to_datetime(pd.Series(target_dates)).dt.date:
    #         cand = hist_df[hist_df["날짜"].apply(lambda x: x.weekday() == d.weekday())].tail(n_weeks)

    #         if len(cand) == 0:
    #             pred = 0
    #         else:
    #             w = np.arange(1, len(cand) + 1)
    #             pred = int(round(np.average(cand[value_col].fillna(0), weights=w)))

    #         preds.append(pred)

    #     return preds


    # # 과거 데이터 범위: 최근 12주
    # hist_start = today - timedelta(weeks=12)
    # hist_end = today - timedelta(days=1)
    # hist_daily = build_total_table(df, hist_start, hist_end, dim_col="shrm_branch", dim_val=sel_res_branch)

    # res_daily["날짜"] = pd.to_datetime(res_daily["날짜"]).dt.date
    # hist_daily["날짜"] = pd.to_datetime(hist_daily["날짜"]).dt.date

    # # 미래 구간만 대상으로 예측 워크인 생성
    # res_daily["pred_walkin"] = predict_walkin_same_dow(hist_daily, res_daily["날짜"], value_col="visit_walkin", n_weeks=8)

    # # 실제 예약수 + 예측 워크인수 = 예상 방문수
    # res_daily["pred_visit_total"] = (
    #     res_daily["visit_reserved"].fillna(0).astype(int)
    #     + res_daily["pred_walkin"].fillna(0).astype(int)
    # )


    # # 렌더링
    # fig_res = go.Figure()

    # fig_res.add_trace(go.Bar(
    #     x=res_daily["날짜"],
    #     y=res_daily["res_cnt"],
    #     marker_color="#636EFA",
    #     name="예약 수",
    #     text=res_daily["res_cnt"],
    #     textposition="auto",
    # ))

    # fig_res.add_trace(go.Bar(
    #     x=res_daily["날짜"],
    #     y=res_daily["pred_walkin"],
    #     marker_color="rgba(99,110,250,0.28)",
    #     name="예측 워크인수",
    #     text=res_daily["pred_walkin"],
    #     textposition="auto",
    # ))

    # fig_res.update_layout(
    #     barmode="stack",
    #     height=350,
    #     margin=dict(l=10, r=10, t=30, b=10),
    #     xaxis=dict(tickformat="%m-%d (%a)"),
    #     yaxis=dict(title="방문 수"),
    #     hovermode="x unified",
    #     legend=dict(
    #         orientation="h",
    #         yanchor="bottom",
    #         y=1.02,
    #         xanchor="right",
    #         x=1
    #     )
    # )

    # st.plotly_chart(fig_res, use_container_width=True, key="res_dist_chart_v2")

    # render_total_table(
    #     res_daily[["날짜", "res_cnt", "pred_walkin", "pred_visit_total"]].rename(columns={
    #         "res_cnt": "예약 수",
    #         "pred_walkin": "예측 워크인수",
    #         "pred_visit_total": "예상 방문수"
    #     })
    # )


    # ──────────────────────────────────
    # 3) 방문 고객 특성 요약
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>방문 고객 특성 요약</h5>", unsafe_allow_html=True)
    st.markdown(
        ":gray-badge[:material/Info: Info]ㅤ선택 기간 기준으로 방문 고객의 연령·성별·구매목적·인지구조 비중과 주요 크로스 인사이트를 확인합니다.",
        unsafe_allow_html=True
    )

    def _clean_dim(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        d = df[cols].copy()
        for c in cols:
            if c == "weight":
                d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)
            else:
                d[c] = d[c].astype("string").fillna("").astype(str).str.strip()
        return d

    def _render_dynamic_insight(df_base: pd.DataFrame, df_aw_base: pd.DataFrame):
            """가장 비중이 높은 데이터를 추출하여 자연스러운 한 줄 요약 텍스트로 렌더링"""
            if df_base is None or df_base.empty:
                return

            # 1. 주요 항목 추출 (빈도수 기반)
            age_counts = df_base[df_base["demo_age"] != ""]["demo_age"].value_counts()
            gender_counts = df_base[df_base["demo_gender"] != ""]["demo_gender"].value_counts()
            purpose_counts = df_base[df_base["purchase_purpose"] != ""]["purchase_purpose"].value_counts()

            top_age = age_counts.idxmax() if not age_counts.empty else "알 수 없음"
            top_gender = gender_counts.idxmax() if not gender_counts.empty else "알 수 없음"
            top_purpose = purpose_counts.idxmax() if not purpose_counts.empty else "알 수 없음"

            # 2. 주요 인지 채널 추출 (가중치 기반)
            top_channel = "알 수 없음"
            if df_aw_base is not None and not df_aw_base.empty:
                aw_grp = df_aw_base[df_aw_base["awareness_type_b"] != ""].groupby("awareness_type_b")["weight"].sum()
                if not aw_grp.empty:
                    top_channel = aw_grp.idxmax()

            # 3. 주요 항목의 비율 계산
            age_pct = (age_counts.max() / age_counts.sum() * 100) if not age_counts.empty else 0
            purpose_pct = (purpose_counts.max() / purpose_counts.sum() * 100) if not purpose_counts.empty else 0

            # 4. 자연스러운 인사이트 문장 조합
            msg = (
                f"현재 조건에서 가장 주축이 되는 방문 고객은 **{top_age} {top_gender}**({age_pct:.1f}%)이며, "
                f"주로 **{top_purpose}**({purpose_pct:.1f}%)을(를) 위해 매장을 찾았습니다."
            )
            if top_channel != "알 수 없음":
                msg += f" 이들의 주요 유입 경로는 **{top_channel}**인 것으로 나타납니다."

            # Streamlit Info 박스로 렌더링
            st.info(msg, icon="💡")
            st.markdown("<br>", unsafe_allow_html=True) # 차트와의 간격 확보

    def _get_age_order(vals: list[str]) -> list[str]:
        base = ["20대", "30대", "40대", "50대", "60대 이상"]
        base_in = [x for x in base if x in vals]
        rest = [x for x in sorted(vals) if x not in base_in]
        return base_in + rest

    def _render_1d_bar(
        df_base: pd.DataFrame,
        col: str,
        key: str,
        weight_col: str | None = None
    ):
        """단일 컬럼(1D) 비율 가로막대 차트"""
        cols = [col] + ([weight_col] if weight_col else [])
        d = _clean_dim(df_base, cols)
        d = d[d[col] != ""]
        if d.empty:
            st.warning("표시할 데이터가 없습니다.")
            return

        if weight_col:
            g = (
                d.groupby(col, dropna=False)[weight_col]
                .sum()
                .reset_index(name="cnt")
            )
        else:
            g = (
                d.groupby(col, dropna=False)
                .size()
                .reset_index(name="cnt")
            )

        if g.empty:
            st.warning("표시할 데이터가 없습니다.")
            return

        g["pct"] = g["cnt"] / g["cnt"].sum() * 100

        # 연령대만 고정 순서
        if col == "demo_age":
            age_order = _get_age_order(g[col].astype(str).unique().tolist())
            g[col] = pd.Categorical(g[col], categories=age_order, ordered=True)
            g = g.sort_values(col, ascending=False)
        else:
            g = g.sort_values("pct", ascending=True)

        fig = px.bar(
            g,
            y=col,
            x="pct",
            orientation="h",
            text="pct",
            custom_data=[col, "cnt", "pct"],
        )
        fig.update_traces(
            texttemplate="%{x:.1f}%",
            textposition="outside",
            hovertemplate="%{customdata[0]}<br>값=%{customdata[1]:,.0f}<br>비중=%{customdata[2]:.1f}%<extra></extra>",
        )
        fig.update_layout(
            height=200,
            margin=dict(l=10, r=30, t=10, b=10),
            showlegend=False,
            xaxis_title=None,
            yaxis_title=None,
            xaxis=dict(ticksuffix="%"),
        )
        st.plotly_chart(fig, use_container_width=True, key=key)

    def _render_2d_stack(
        df_base: pd.DataFrame,
        row_col: str,
        col_col: str,
        key: str,
        weight_col: str | None = None
    ):
        """교차 컬럼(2D) 100% 누적 가로막대 차트"""
        cols = [row_col, col_col] + ([weight_col] if weight_col else [])
        d = _clean_dim(df_base, cols)
        d = d[(d[row_col] != "") & (d[col_col] != "")]
        if d.empty:
            st.warning("표시할 데이터가 없습니다.")
            return

        if weight_col:
            g = (
                d.groupby([row_col, col_col], dropna=False)[weight_col]
                .sum()
                .reset_index(name="cnt")
            )
        else:
            g = (
                d.groupby([row_col, col_col], dropna=False)
                .size()
                .reset_index(name="cnt")
            )

        if g.empty:
            st.warning("표시할 데이터가 없습니다.")
            return

        # 정렬: 연령대일 때만 고정 순서
        if row_col == "demo_age":
            row_order = _get_age_order(g[row_col].astype(str).unique().tolist())
        else:
            row_order = (
                g.groupby(row_col, dropna=False)["cnt"]
                .sum()
                .sort_values(ascending=True)
                .index
                .tolist()
            )

        if col_col == "demo_age":
            col_order = _get_age_order(g[col_col].astype(str).unique().tolist())
        else:
            col_order = (
                g.groupby(col_col, dropna=False)["cnt"]
                .sum()
                .sort_values(ascending=False)
                .index
                .tolist()
            )

        pt = (
            g.pivot_table(
                index=row_col,
                columns=col_col,
                values="cnt",
                aggfunc="sum",
                fill_value=0,
            )
            .reindex(row_order)
            .fillna(0)
        )

        for c in col_order:
            if c not in pt.columns:
                pt[c] = 0
        pt = pt[col_order]

        share = pt.div(pt.sum(axis=1).replace(0, np.nan), axis=0).fillna(0) * 100
        share = share.reset_index()

        long_share = share.melt(
            id_vars=row_col,
            var_name=col_col,
            value_name="pct",
        )
        long_cnt = pt.reset_index().melt(
            id_vars=row_col,
            var_name=col_col,
            value_name="cnt",
        )

        plot_df = long_share.merge(long_cnt, on=[row_col, col_col], how="left")

        # y축 정렬 강제용 categorical 지정
        plot_df[row_col] = pd.Categorical(
            plot_df[row_col],
            categories=row_order,
            ordered=True
        )
        plot_df[col_col] = pd.Categorical(
            plot_df[col_col],
            categories=col_order,
            ordered=True
        )

        fig = px.bar(
            plot_df,
            y=row_col,
            x="pct",
            color=col_col,
            orientation="h",
            barmode="stack",
            custom_data=[col_col, "cnt", "pct"],
            category_orders={row_col: row_order, col_col: col_order},
        )
        fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
        fig.update_traces(
            hovertemplate="%{customdata[0]}<br>값=%{customdata[1]:,.0f}<br>비중=%{customdata[2]:.1f}%<extra></extra>"
        )
        fig.update_layout(
            height=200,
            margin=dict(l=10, r=10, t=10, b=10),
            legend_title=None,
            xaxis_title=None,
            yaxis_title=None,
            xaxis=dict(range=[0, 100], ticksuffix="%"),
        )
        if row_col == "demo_age":
            y_order = row_order[::-1]
        else:
            y_order = row_order

        fig.update_yaxes(
            categoryorder="array",
            categoryarray=y_order
        )
        
        st.plotly_chart(fig, use_container_width=True, key=key)

    def render_block(df_base: pd.DataFrame, df_aw_base: pd.DataFrame, key_tag: str):
        _render_dynamic_insight(df_base, df_aw_base)
        
        c1, c2, c3, c4 = st.columns(4, vertical_alignment="top")

        with c1:
            st.markdown(
                """
                <h6 style="margin:0;">📊 연령대</h6>
                <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">전체 방문 고객의 연령대 비중</p>
                """,
                unsafe_allow_html=True
            )
            _render_1d_bar(df_base, col="demo_age", key=f"profile_age::{key_tag}")

        with c2:
            st.markdown(
                """
                <h6 style="margin:0;">📊 연령대 × 성별</h6>
                <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">연령대별 성별 구성비</p>
                """,
                unsafe_allow_html=True
            )
            _render_2d_stack(df_base, row_col="demo_age", col_col="demo_gender", key=f"profile_age_gender::{key_tag}")

        with c3:
            st.markdown(
                """
                <h6 style="margin:0;">📊 구매목적</h6>
                <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">전체 방문 고객의 구매목적 비중</p>
                """,
                unsafe_allow_html=True
            )
            _render_1d_bar(df_base, col="purchase_purpose", key=f"profile_purpose::{key_tag}")

        with c4:
            st.markdown(
                """
                <h6 style="margin:0;">📊 구매목적 × 연령대</h6>
                <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">구매목적별 연령대 구성비</p>
                """,
                unsafe_allow_html=True
            )
            _render_2d_stack(df_base, row_col="purchase_purpose", col_col="demo_age", key=f"cross_purpose_age::{key_tag}")

        st.markdown(" ")

        c5, c6, c7, c8 = st.columns(4, vertical_alignment="top")

        with c5:
            st.markdown(
                """
                <h6 style="margin:0;">📊 인지단계</h6>
                <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">전체 방문 고객의 인지단계 비중</p>
                """,
                unsafe_allow_html=True
            )
            _render_1d_bar(df_aw_base, col="awareness_type_a", key=f"profile_stage::{key_tag}", weight_col="weight")

        with c6:
            st.markdown(
                """
                <h6 style="margin:0;">📊 인지단계 × 인지채널</h6>
                <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">인지단계별 인지채널 구성비</p>
                """,
                unsafe_allow_html=True
            )
            _render_2d_stack(df_aw_base, row_col="awareness_type_a", col_col="awareness_type_b", key=f"profile_stage_channel::{key_tag}", weight_col="weight")

        with c7:
            st.markdown(
                """
                <h6 style="margin:0;">📊 구매목적 × 인지채널</h6>
                <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">구매목적별 인지채널 구성비</p>
                """,
                unsafe_allow_html=True
            )
            _render_2d_stack(df_aw_base, row_col="purchase_purpose", col_col="awareness_type_b", key=f"cross_purpose_stage::{key_tag}", weight_col="weight")

        with c8:
            st.markdown(
                """
                <h6 style="margin:0;">📊 연령대 × 인지채널</h6>
                <p style="margin:-10px 0 12px 0; color:#6c757d; font-size:13px;">연령대별 인지채널 구성비</p>
                """,
                unsafe_allow_html=True
            )
            _render_2d_stack(df_aw_base, row_col="demo_age", col_col="awareness_type_b", key=f"cross_age_channel::{key_tag}", weight_col="weight")

    # 데이터 생성
    df1 = df1[[
        "event_date", "shrm_branch", "shrm_region", "shrm_type",
        "demo_gender", "demo_age", "purchase_purpose", "awareness_type", "visit_type" # <- visit_type 추가
    ]]
    df_aw = None
    if df1 is not None and not df1.empty and "awareness_type" in df1.columns:
        _rid = np.arange(len(df1))
        s = df1["awareness_type"].astype("string").fillna("").astype(str)

        lst = s.apply(lambda x: [t.strip() for t in str(x).split(",") if t.strip()])
        lst = lst.apply(lambda x: x if len(x) > 0 else [])
        n = lst.apply(len).astype(float)

        df_aw = df1.assign(_rid=_rid, awareness_type_list=lst, _n=n)
        df_aw = df_aw[df_aw["_n"] > 0]
        df_aw = df_aw.explode("awareness_type_list", ignore_index=True)

        df_aw["awareness_type"] = df_aw["awareness_type_list"].astype(str).str.strip()
        df_aw = df_aw[df_aw["awareness_type"] != ""]
        df_aw["weight"] = (1.0 / df_aw["_n"]).astype(float)

        df_aw["awareness_type_a"] = (
            df_aw["awareness_type"]
            .astype(str)
            .str.extract(r"\((.*?)\)", expand=False)
            .fillna("")
            .astype(str)
            .str.strip()
        )
        df_aw["awareness_type_b"] = (
            df_aw["awareness_type"]
            .astype(str)
            .str.replace(r"\(.*?\)", "", regex=True)
            .str.strip()
        )

        df_aw = df_aw[[
            "event_date", "shrm_branch", "shrm_region", "shrm_type",
            "demo_age", "demo_gender", "purchase_purpose",
            "awareness_type", "awareness_type_a", "awareness_type_b", "weight", "visit_type" # <- visit_type 추가
        ]]

    # 렌더링
    with st.expander("공통 Filter", expanded=False):
        f1, f2, f3 = st.columns([1.4, 1.3, 1.3], vertical_alignment="bottom") # 컬럼 3개로 분할
        with f1:
            flow_date = st.date_input(
                "기간 선택",
                value=[_def_s, _def_e],
                min_value=_min_d,
                max_value=_max_d,
                key="wnd",
            )
        with f2:
            vt_opts = get_dim_options(df1, "visit_type", all_label="전체")
            sel_vt = st.selectbox("방문 유형", vt_opts, index=0, key="detail_vt_sel")

    if isinstance(flow_date, (list, tuple)) and len(flow_date) == 2:
        flow_start, flow_end = flow_date
    else:
        flow_start, flow_end = _def_s, _def_e

    flow_start = pd.to_datetime(flow_start)
    flow_end = pd.to_datetime(flow_end)

    df1["event_date"] = pd.to_datetime(df1["event_date"], errors="coerce")
    df1_v = df1[(df1["event_date"] >= flow_start) & (df1["event_date"] <= flow_end)]
    
    # 방문 유형 필터 적용
    if sel_vt != "전체":
        df1_v = df1_v[df1_v["visit_type"].astype(str).str.strip() == str(sel_vt)]

    if df_aw is not None and not df_aw.empty:
        df_aw["event_date"] = pd.to_datetime(df_aw["event_date"], errors="coerce")
        df_aw_v = df_aw[(df_aw["event_date"] >= flow_start) & (df_aw["event_date"] <= flow_end)]
        # 방문 유형 필터 적용
        if sel_vt != "전체":
            df_aw_v = df_aw_v[df_aw_v["visit_type"].astype(str).str.strip() == str(sel_vt)]
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
        render_block(df1_f, df_aw_f, key_tag=f"branch::{sel_branch}")

    with top_tabs[1]:
        region_opts = [x for x in get_dim_options(df1_v, "shrm_region", all_label="전체") if x != "전체"]
        sub_tabs = st.tabs(region_opts)
        for i, opt in enumerate(region_opts):
            with sub_tabs[i]:
                df1_f = filter_dim(df1_v, "shrm_region", opt, all_label="전체")
                df_aw_f = filter_dim(df_aw_v, "shrm_region", opt, all_label="전체") if df_aw_v is not None else None
                render_block(df1_f, df_aw_f, key_tag=f"region::{opt}")

    with top_tabs[2]:
        type_opts = [x for x in get_dim_options(df1_v, "shrm_type", all_label="전체") if x != "전체"]
        sub_tabs = st.tabs(type_opts)
        for i, opt in enumerate(type_opts):
            with sub_tabs[i]:
                df1_f = filter_dim(df1_v, "shrm_type", opt, all_label="전체")
                df_aw_f = filter_dim(df_aw_v, "shrm_type", opt, all_label="전체") if df_aw_v is not None else None
                render_block(df1_f, df_aw_f, key_tag=f"type::{opt}")

    # ──────────────────────────────────
    # 4) 타겟별 트렌드 추이 (크로스 분석)
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>📈 타겟별 트렌드 추이 (크로스 분석)</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ원하는 모든 조건을 조합하여(예: 워크인 + 30대 + 여성 + 신혼/혼수) 특정 타겟의 일자별 유입 및 방문 트렌드를 정밀하게 추적합니다.", unsafe_allow_html=True)

    with st.expander("크로스 분석 Filter (다중 조건 선택)", expanded=True):
        st.markdown("<p style='font-size:13px; color:#6c757d; margin-bottom:10px;'>※ '전체'가 아닌 특정 값을 선택하면 해당 조건에 맞는 데이터만 교집합으로 필터링됩니다.</p>", unsafe_allow_html=True)
        
        # 필터 1열 (기간 및 방문자 데모그래픽)
        t_f1, t_f6, t_f7, t_f8 = st.columns(4, vertical_alignment="bottom")
        with t_f1:
            trend_date = st.date_input("조회 기간", value=[_def_s, _def_e], min_value=_min_d, max_value=_max_d, key="t_date")
        with t_f6:
            sel_t_region = st.selectbox("쇼룸 권역", get_dim_options(df1, "shrm_region", all_label="전체"), key="t_region")
        with t_f7:
            sel_t_branch = st.selectbox("쇼룸 지점", get_dim_options(df1, "shrm_branch", all_label="전체"), key="t_branch")
        with t_f8:
            sel_t_type = st.selectbox("쇼룸 유형", get_dim_options(df1, "shrm_type", all_label="전체"), key="t_type")


        # 필터 2열 (목적 및 쇼룸 정보)
        t_f2, t_f3, t_f4, t_f5 = st.columns(4, vertical_alignment="bottom")
        with t_f2:
            sel_t_vt = st.selectbox("방문 유형", get_dim_options(df1, "visit_type", all_label="전체"), key="t_vt")
        with t_f3:
            sel_t_gender = st.selectbox("성별", get_dim_options(df1, "demo_gender", all_label="전체"), key="t_gender")
        with t_f4:
            sel_t_age = st.selectbox("연령대", get_dim_options(df1, "demo_age", all_label="전체"), key="t_age")
        with t_f5:
            sel_t_purp = st.selectbox("구매목적", get_dim_options(df1, "purchase_purpose", all_label="전체"), key="t_purp")


    # 기간 설정
    if isinstance(trend_date, (list, tuple)) and len(trend_date) == 2:
        t_start, t_end = trend_date
    else:
        t_start, t_end = _def_s, _def_e

    # 필터 적용 데이터셋 복사 및 날짜 필터링
    df1_t = df1[(df1["event_date"] >= pd.to_datetime(t_start)) & (df1["event_date"] <= pd.to_datetime(t_end))].copy()
    if df_aw is not None and not df_aw.empty:
        df_aw_t = df_aw[(df_aw["event_date"] >= pd.to_datetime(t_start)) & (df_aw["event_date"] <= pd.to_datetime(t_end))].copy()
    else:
        df_aw_t = pd.DataFrame()

    # 다중 차원 필터 적용 함수
    def _apply_trend_filter(df_target, col_name, selected_val):
        if selected_val != "전체" and not df_target.empty and col_name in df_target.columns:
            return df_target[df_target[col_name].astype(str).str.strip() == str(selected_val)]
        return df_target

    # 필터 매핑 및 일괄 적용
    filter_map = {
        "visit_type": sel_t_vt,
        "demo_gender": sel_t_gender,
        "demo_age": sel_t_age,
        "purchase_purpose": sel_t_purp,
        "shrm_region": sel_t_region,
        "shrm_branch": sel_t_branch,
        "shrm_type": sel_t_type
    }

    for col, val in filter_map.items():
        df1_t = _apply_trend_filter(df1_t, col, val)
        df_aw_t = _apply_trend_filter(df_aw_t, col, val)

    # df1_t 카운트용 컬럼 추가
    if not df1_t.empty:
        df1_t["cnt"] = 1

    # 차트 렌더링 헬퍼 함수
    def _render_trend_chart(df_plot, x_col, color_col, val_col, key):        
        if df_plot is None or df_plot.empty:
            st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
            return
            
        grp = df_plot.groupby([x_col, color_col], dropna=False)[val_col].sum().reset_index()
        grp = grp[grp[color_col].astype(str).str.strip() != ""] # 빈값 제외
        
        if grp.empty:
            st.warning("표시할 데이터가 없습니다.")
            return
            
        grp = grp.sort_values(x_col)
        
        fig = px.bar(
            grp, 
            x=x_col, 
            y=val_col, 
            color=color_col, 
            barmode="stack",
            custom_data=[color_col]
        )
        fig.update_traces(hovertemplate="%{x}<br>%{customdata[0]}: %{y:,.1f}<extra></extra>")
        fig.update_layout(
            height=350,
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None),
            xaxis_title=None,
            yaxis_title=None
        )
        st.plotly_chart(fig, use_container_width=True, key=key)

    # 8개 탭 구성
    tab_names = ["성별 추이", "연령대 추이", "구매목적 추이", "인지단계 추이", "인지채널 추이", "지점방문 추이", "권역 추이", "유형 추이"]
    trend_tabs = st.tabs(tab_names)

    with trend_tabs[0]:
        _render_trend_chart(df1_t, "event_date", "demo_gender", "cnt", "tr_gen")
    with trend_tabs[1]:
        _render_trend_chart(df1_t, "event_date", "demo_age", "cnt", "tr_age")
    with trend_tabs[2]:
        _render_trend_chart(df1_t, "event_date", "purchase_purpose", "cnt", "tr_purp")
    with trend_tabs[3]:
        _render_trend_chart(df_aw_t, "event_date", "awareness_type_a", "weight", "tr_aw_stg")
    with trend_tabs[4]:
        _render_trend_chart(df_aw_t, "event_date", "awareness_type_b", "weight", "tr_aw_chn")
    with trend_tabs[5]:
        _render_trend_chart(df1_t, "event_date", "shrm_branch", "cnt", "tr_brn")
    with trend_tabs[6]:
        _render_trend_chart(df1_t, "event_date", "shrm_region", "cnt", "tr_reg")
    with trend_tabs[7]:
        _render_trend_chart(df1_t, "event_date", "shrm_type", "cnt", "tr_typ")




if __name__ == "__main__":
    main()
