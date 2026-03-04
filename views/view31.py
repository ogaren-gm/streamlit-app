# 서희_리뉴얼_키워드대시보드_2026-02-27 (fix: period/label/shading/weekly-range)
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import importlib, json
from datetime import datetime, timedelta, date
import gspread
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo
import html
from scipy.spatial import ConvexHull  # 추가된 부분

import modules.ui_common as ui
importlib.reload(ui)


# ──────────────────────────────────
# CONFIG
# ──────────────────────────────────
CFG = {
    # 기본
    "TZ": "Asia/Seoul",
    "CACHE_TTL": 3600,
    "DEFAULT_LOOKBACK_DAYS": 14,
    
    "TOPK_OPTS": [10, 15, 20, 25, 30, 35, 40],

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

AGE_ORDER = ['19-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59']


# ──────────────────────────────────
# HELPER
# ──────────────────────────────────
def _calc_ranges(end_date: date, n_days: int):
    cur_e = end_date
    cur_s = (pd.to_datetime(cur_e) - timedelta(days=n_days - 1)).date()
    prev_e = (pd.to_datetime(cur_s) - timedelta(days=1)).date()
    prev_s = (pd.to_datetime(prev_e) - timedelta(days=n_days - 1)).date()
    return prev_s, prev_e, cur_s, cur_e

def _kw_total_base(df0: pd.DataFrame) -> pd.DataFrame:
    # kw_total은 age 중복이므로 max로 대표값 1개만
    return (
        df0.groupby(["날짜", "키워드유형", "키워드"], as_index=False)
        .agg(kw_total=("키워드_abs_total(일)", "max"))
    )

def _to_pct(g: pd.DataFrame, x_col: str, val_col: str) -> pd.DataFrame:
    denom = g.groupby(x_col, dropna=False)[val_col].transform("sum")
    g["val"] = np.where(denom > 0, g[val_col] / denom, 0.0)
    return g

def _clean_key(s: str) -> str:
    return (
        str(s)
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace(".", "_")
        .replace("-", "_")
        .replace("(", "")
        .replace(")", "")
    )


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
    st.sidebar.caption("영역마다 개별로 기간을 조정하세요.")
    
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
            "https://docs.google.com/spreadsheets/d/1HFPuxQSJqIY7VY_3YcAwEPfw_SjnApH_txRPS69s4xk/edit?gid=1274042914#gid=1274042914"
        )
        wsa = sh.worksheet("query_demographic")
        data = wsa.get("A1:Z")
        df = pd.DataFrame(data[1:], columns=data[0])
        
        # 1) 컬럼 전처리
        need_cols = ["날짜", "키워드", "키워드유형", "age_info", "abs_age", "키워드_abs_total(일)"]
        df = df[[c for c in need_cols if c in df.columns]]

        # 1-1) 날짜
        df["날짜"] = (
            df["날짜"]
            .astype("string")
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA})
        )
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")

        # 1-2) 날짜 이외
        df["키워드"]    = df["키워드"].astype(str).fillna("").str.strip()
        df["키워드유형"] = df["키워드유형"].astype(str).fillna("").str.strip()
        df["age_info"] = df["age_info"].astype(str).fillna("").str.strip()
        df["abs_age"]  = pd.to_numeric(df["abs_age"], errors="coerce").fillna(0)
        df["키워드_abs_total(일)"] = pd.to_numeric(df["키워드_abs_total(일)"], errors="coerce").fillna(0)
        df["age_info"] = pd.Categorical(df["age_info"], categories=AGE_ORDER, ordered=True)

        # 유효 데이터만
        df = df[(df["날짜"].notna()) & (df["키워드"] != "") & (df["키워드유형"] != "") & (df["age_info"].notna())]

        return df


    # PROGRESS BAR
    spacer_placeholder = st.empty()
    progress_placeholder = st.empty()

    spacer_placeholder.markdown("<br>", unsafe_allow_html=True)
    progress_bar = progress_placeholder.progress(0, text="데이터베이스 연결 확인 중입니다...")
    
    import time
    time.sleep(0.2)
    
    for i in range(1, 80, 5):
        progress_bar.progress(i, text=f"데이터를 불러오고 있습니다...{i}%")
        time.sleep(0.1)
    
    df = load_data()
    
    progress_bar.progress(95, text="데이터 분석 및 시각화를 구성 중입니다...")
    time.sleep(0.4)
    
    progress_bar.progress(100, text="데이터 로드 완료!")
    time.sleep(0.6)

    progress_placeholder.empty()
    spacer_placeholder.empty()


    # ──────────────────────────────────
    # D) Header
    # ──────────────────────────────────
    st.subheader("키워드 대시보드")
    
    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px;line-height:1.5;">
            네이버 DataLab 데이터를 기반으로 <b>키워드 유형별 트렌드와 연령대별 관심도</b>를 분석하는 대시보드입니다.<br>
            </div>
            <div style="color:#6c757d;font-size:14px;line-height:2.0;">
            ※ 전일 데이터가 오전 10시경 업데이트 됩니다.
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
            <a href="?refresh=1" title="캐시 초기화" style="text-decoration:none;vertical-align:middle;">
                <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 8px;
                font-size:13px;line-height:1;
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

    # 대시보드에서 쓸 날짜 기준값 4개를 한 번만 계산
        # _valid : df["날짜"]에서 결측 제거한 시리즈
        # _min_d : 데이터에 존재하는 가장 이른 날짜
        # _max_d : 데이터에 존재하는 가장 마지막 날짜
        # _def_s : 기본 시작일(룩백), DEFAULT_LOOKBACK_DAYS만큼 뒤로 가되, 데이터 최소일 _min_d보다 더 과거로는 못 가게 max()로 막음.
        # _def_e : 기본 종료일 = 데이터의 최신일(_max_d)
    _valid = df["날짜"].dropna()
    _min_d = _valid.min().date() if not _valid.empty else datetime.now().date()
    _max_d = _valid.max().date() if not _valid.empty else datetime.now().date()
    _def_s = max(_min_d, _max_d - timedelta(days=CFG["DEFAULT_LOOKBACK_DAYS"] - 1))
    _def_e = _max_d


    # 키워드 유형 대분류 / 소분류 분류
        # [ 전체 ] [ A ] [ B ] [ C ]
        # B 클릭 시 →
        #    [ 전체 ] [ sub1 ] [ sub2 ]
    type_all = sorted(df["키워드유형"].dropna().astype(str).unique().tolist())
    major_map = {}
    for t in type_all:
        if "_" in t:
            maj = t.split("_", 1)[0]
            major_map.setdefault(maj, {"plain": set(), "subs": set()})
            major_map[maj]["subs"].add(t)
        else:
            maj = t
            major_map.setdefault(maj, {"plain": set(), "subs": set()})
            major_map[maj]["plain"].add(t)

    majors = sorted(major_map.keys())
    outer_names = ["전체"] + majors


    # ──────────────────────────────────
    # 1) QUICK INSIGHT ( 트렌드 요약 )
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>QUICK INSIGHT</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ키워드 유형별 검색량 증감 흐름과 현재 핵심 키워드로 트렌드를 빠르게 확인합니다.", unsafe_allow_html=True)

    # Filter
    with st.expander("Filter", expanded=False):
        f1, f2, f3 = st.columns([1.3, 1.2, 1.2], vertical_alignment="bottom")

        if "qi_mode" not in st.session_state:
            st.session_state["qi_mode"] = "최근 7일"

        if "qi_prev" not in st.session_state or "qi_cur" not in st.session_state:
            ps, pe, cs, ce = _calc_ranges(_max_d, 7)
            st.session_state["qi_prev"] = [ps, pe]
            st.session_state["qi_cur"]  = [cs, ce]

        def _qi_apply_win():
            m = st.session_state["qi_mode"]
            if m == "커스텀":
                return
            n_map = {"최근 7일": 7, "최근 14일": 14, "최근 30일": 30}
            n = n_map.get(m, 7)
            ps, pe, cs, ce = _calc_ranges(_max_d, n)
            st.session_state["qi_prev"] = [ps, pe]
            st.session_state["qi_cur"]  = [cs, ce]

        with f1:
            st.radio(
                "기간 윈도우 설정",
                ["최근 7일", "최근 14일", "최근 30일", "커스텀"],
                horizontal=True,
                key="qi_mode",
                on_change=_qi_apply_win,
            )

        lock = (st.session_state["qi_mode"] != "커스텀")

        with f2:
            prev_rng = st.date_input("이전기간 선택", key="qi_prev", disabled=lock)
        with f3:
            cur_rng = st.date_input("비교기간(최근) 선택", key="qi_cur", disabled=lock)

    # range normalize
    if not (isinstance(prev_rng, (list, tuple)) and len(prev_rng) == 2):
        prev_rng = st.session_state["qi_prev"]
    if not (isinstance(cur_rng, (list, tuple)) and len(cur_rng) == 2):
        cur_rng = st.session_state["qi_cur"]

    prev_s, prev_e = pd.to_datetime(prev_rng[0]), pd.to_datetime(prev_rng[1])
    cur_s,  cur_e  = pd.to_datetime(cur_rng[0]),  pd.to_datetime(cur_rng[1])

    if prev_s > prev_e:
        prev_s, prev_e = prev_e, prev_s
    if cur_s > cur_e:
        cur_s, cur_e = cur_e, cur_s

    # base
    base = _kw_total_base(df)

    cur_type = (
        base[(base["날짜"] >= cur_s) & (base["날짜"] <= cur_e)]
        .groupby(["키워드유형"], as_index=False)["kw_total"].sum()
        .rename(columns={"kw_total": "비교"})
    )
    prev_type = (
        base[(base["날짜"] >= prev_s) & (base["날짜"] <= prev_e)]
        .groupby(["키워드유형"], as_index=False)["kw_total"].sum()
        .rename(columns={"kw_total": "이전"})
    )

    sum_type = cur_type.merge(prev_type, on="키워드유형", how="outer")
    sum_type["비교"] = pd.to_numeric(sum_type["비교"], errors="coerce").fillna(0)
    sum_type["이전"] = pd.to_numeric(sum_type["이전"], errors="coerce").fillna(0)
    sum_type["증감량"] = sum_type["비교"] - sum_type["이전"]
    sum_type["증감률"] = np.where(sum_type["이전"] > 0, sum_type["증감량"] / sum_type["이전"], np.nan)

    # 대표 키워드 Top3
    cur_kw = (
        base[(base["날짜"] >= cur_s) & (base["날짜"] <= cur_e)]
        .groupby(["키워드유형", "키워드"], as_index=False)["kw_total"].sum()
        .sort_values(["키워드유형", "kw_total"], ascending=[True, False])
    )
    kw_top3 = (
        cur_kw.groupby("키워드유형")["키워드"]
        .apply(lambda s: s.head(3).astype(str).tolist())
        .to_dict()
    )

    # 정렬
    sum_type["_sort"] = sum_type["증감률"].fillna(-1e18)
    sum_type = sum_type.sort_values("_sort", ascending=False).drop(columns=["_sort"]).reset_index(drop=True)

    if sum_type.empty:
        st.info("요약을 만들 데이터가 없습니다.")
    else:
        cols = st.columns(len(sum_type), vertical_alignment="top")

        for col, (_, r) in zip(cols, sum_type.iterrows()):
            t = html.escape(str(r["키워드유형"]), quote=True)
            rate = r["증감률"]
            delta = float(r["증감량"])
            delta_txt = f"(+{delta:,.0f})" if delta > 0 else f"({delta:,.0f})"
            cur_v = float(r["비교"])
            prev_v = float(r["이전"])

            if pd.isna(rate):
                arrow = "–"
                rate_txt = "N/A"
                color = "#64748B"
            else:
                arrow = "▲" if rate > 0 else ("▼" if rate < 0 else "–")
                rate_txt = f"{rate:.1%}"
                color = "#2E7D32" if rate > 0 else ("#D32F2F" if rate < 0 else "#64748B")

            kws = kw_top3.get(r["키워드유형"], [])
            kws = (kws + ["-", "-", "-"])[:3]
            kws = [html.escape(str(x), quote=True) for x in kws]

            with col:
                st.markdown(
                    f"""
                    <div style="
                        border:1px solid #e5e5e5;
                        border-radius:12px;
                        padding:12px 12px 10px 12px;
                        background:#ffffff;
                    ">
                        <div style="font-weight:700; font-size:13px; margin-bottom:6px;">{t}</div>
                        <div style="font-size:18px; font-weight:800; line-height:1.2; color:{color};">
                            {arrow} {rate_txt}
                        </div>
                        <div style="font-size:12px; font-weight:500; margin-top:10px;">
                            이전 {prev_v:,.0f}
                        </div>
                        <div style="font-size:12px; font-weight:500; color:{color};">
                            최근 {cur_v:,.0f} {delta_txt}
                        </div>
                        <div style="font-size:12px; margin-top:13px; line-height:1.2; opacity:0.75;">
                            <div style="font-weight:500; margin-bottom:4px;">Major Keyword</div>
                            - {kws[0]}<br>
                            - {kws[1]}<br>
                            - {kws[2]}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.markdown(" ")


    # ──────────────────────────────────
    # 2) 검색량 추이 
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>검색량 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ키워드 유형, 개별 키워드, 연령대별 검색량의 변화를 확인합니다.", unsafe_allow_html=True)

    def _render_block(d0: pd.DataFrame, tab_key: str, fixed_types: list[str] | None):
        tk = _clean_key(tab_key)

        # ---------- Filter
        with st.expander("Filter", expanded=True):
            c1, c2, _p, c3, c4, c5 = st.columns([2.2, 1.8, 0.2, 1, 1.4, 1.1], vertical_alignment="bottom")

            with _p: pass
            
            # ✅ 기간 선택
            with c1:
                rng = st.date_input(
                    "기간 선택",
                    value=[_def_s, _def_e],
                    min_value=_min_d,
                    max_value=_max_d,
                    key=f"rng_{tk}",
                )

                # date_input 결과 normalize
                if isinstance(rng, (list, tuple)) and len(rng) == 2:
                    s_day, e_day = rng[0], rng[1]
                else:
                    s_day, e_day = _def_s, _def_e

                if s_day > e_day:
                    s_day, e_day = e_day, s_day

            # ✅ 보기 선택
            if (fixed_types is None) or (len(fixed_types) > 1):
                with c2:
                    view = st.selectbox("보기 선택", ["키워드유형별", "연령대별", "키워드별"], index=0, key=f"v_{tk}")
            else:
                with c2:
                    view = st.selectbox("보기 선택", ["연령대별", "키워드별"], index=0, key=f"v_{tk}")

            # ✅ 집계 단위
            with c3:
                gran = st.radio("집계 단위", ["일", "주"], horizontal=True, index=0, key=f"g_{tk}")

            # ✅ 그래프
            with c4:
                chart = st.radio("그래프", ["꺾은선", "누적막대"], horizontal=True, index=0, key=f"c_{tk}")

            # ✅ 스케일
            with c5:
                scale = st.radio("스케일", ["절대값", "백분율"], horizontal=True, index=0, key=f"sc_{tk}")

        s_dt = pd.to_datetime(s_day)
        e_dt = pd.to_datetime(e_day)
        d = d0[(d0["날짜"] >= s_dt) & (d0["날짜"] <= e_dt)]

        if fixed_types is not None:
            d = d[d["키워드유형"].astype(str).isin([str(x) for x in fixed_types])]

        if d.empty:
            st.warning("선택 기간/조건에 데이터가 없습니다.")
            return

        # ---------- Advanced filter
        with st.expander("연령대 · 키워드 Filter", expanded=False):
        
            # 연령대
            a_all = [a for a in AGE_ORDER if a in d["age_info"].astype(str).unique().tolist()]
            a_sel = st.multiselect("연령대 선택", options=a_all, default=a_all, key=f"a_{tk}")
            
            # 키워드 NEW
            k_all = sorted(d["키워드"].astype(str).dropna().unique().tolist())
            use_custom_kw = st.checkbox(
                f"키워드 개별 선택 (전체 {len(k_all)}개 중)",
                value=False,
                key=f"k_toggle_{tk}"
            )
            if use_custom_kw:
                k_sel = st.multiselect(
                    "키워드 선택",
                    options=k_all,
                    default=k_all,
                    key=f"k_{tk}"
                )
            else:
                # ✅ UI는 안 보이지만 내부적으로는 전체 선택 유지
                k_sel = k_all

        d = d[
            (d["age_info"].astype(str).isin([str(x) for x in a_sel])) &
            (d["키워드"].astype(str).isin([str(x) for x in k_sel]))
        ]

        if d.empty:
            st.warning("고급 필터 적용 후 데이터가 없습니다.")
            return

        # ✅ period 컬럼은 2개만 만들고 재사용 (A 해결)
        mode_label = "일별" if gran == "일" else "주별"
        w_age = ui.add_period_columns(d, "날짜", mode_label)
        w_kw  = ui.add_period_columns(_kw_total_base(d), "날짜", mode_label)

        # ✅ 기간 마스터(dt_lbl): _period 기준으로 중복 제거 + 정렬 (B 해결)
        dt_lbl = (
            w_age[["_period_dt", "_period"]]
            .assign(_period_dt=lambda x: pd.to_datetime(x["_period_dt"], errors="coerce"))
            .dropna(subset=["_period_dt"])
            .drop_duplicates(subset=["_period"])   # ✅ 표시 라벨 기준
            .sort_values("_period_dt")
            .reset_index(drop=True)
        )
        if dt_lbl.empty:
            st.warning("기간 컬럼 생성에 실패했습니다. 날짜 컬럼을 점검하세요.")
            return

        # ✅ Plotly는 pandas datetime 그대로 사용 (D 해결)
        x_dt = pd.to_datetime(dt_lbl["_period_dt"], errors="coerce")
        tick_text = dt_lbl["_period"].astype(str).tolist()
        date_fmt = "%Y-%m-%d"

        # ---------- Build aggregated series by view
        if view == "키워드유형별":
            g = (
                w_kw.groupby(["_period_dt", "키워드유형"], as_index=False)
                    .agg(val=("kw_total", "sum"))
            )
            dim_col = "키워드유형"

        elif view == "연령대별":
            g = (
                w_age.groupby(["_period_dt", "age_info"], as_index=False)
                     .agg(val=("abs_age", "sum"))
            )
            dim_col = "age_info"

        else:  # "키워드별"
            g = (
                w_kw.groupby(["_period_dt", "키워드"], as_index=False)
                    .agg(val=("kw_total", "sum"))
            )
            dim_col = "키워드"

        # ✅ 축 기준을 dt_lbl(x_dt)로 강제 정렬/리인덱스
        g["_period_dt"] = pd.to_datetime(g["_period_dt"], errors="coerce")
        g = g.dropna(subset=["_period_dt"])

        # dims 순서
        if view == "연령대별":
            dims = [a for a in AGE_ORDER if a in g[dim_col].astype(str).unique().tolist()]
        else:
            dims = (
                g.groupby(dim_col)["val"].sum()
                 .sort_values(ascending=False)
                 .index.astype(str).tolist()
            )

        # 백분율
        if scale == "백분율":
            denom = g.groupby("_period_dt", dropna=False)["val"].transform("sum")
            g2 = g.copy()
            g2["val"] = np.where(denom > 0, g2["val"] / denom, 0.0).astype(float)
        else:
            g2 = g.copy()
            g2["val"] = pd.to_numeric(g2["val"], errors="coerce").fillna(0)

        # ---------- Chart (x는 _period_dt, 라벨은 ticktext로)
        # ✅ (NEW) dims × 날짜 매트릭스를 한 번만 만들어 재사용
        mat = (
            g2.pivot_table(index="_period_dt", columns=dim_col, values="val", aggfunc="sum", fill_value=0)
              .reindex(x_dt, fill_value=0)
        )
        
        fig = go.Figure()
        palette = px.colors.qualitative.Pastel
        c_map = {k: palette[i % len(palette)] for i, k in enumerate(dims)}

        if chart == "누적막대":
            for k in dims:
                s = mat[str(k)] if str(k) in mat.columns else pd.Series([0]*len(x_dt), index=x_dt)
                fig.add_bar(
                    x=x_dt,
                    y=s.values,
                    name=str(k),
                    marker_color=c_map[k],
                    opacity=0.8,
                    hovertemplate=f"{k}<br>%{{x|{date_fmt}}}<br>"
                                  + ("%{y:.1%}" if scale == "백분율" else "%{y:,.0f}")
                                  + "<extra></extra>",
                )
            fig.update_layout(barmode="relative")

        else:
            for k in dims:
                s = mat[str(k)] if str(k) in mat.columns else pd.Series([0]*len(x_dt), index=x_dt)
                fig.add_trace(go.Scatter(
                    x=x_dt,
                    y=s.values,
                    mode="lines+markers",
                    name=str(k),
                    marker=dict(size=4),
                    marker_color=c_map[k],
                    hovertemplate=f"{k}<br>%{{x|{date_fmt}}}<br>"
                                  + ("%{y:.1%}" if scale == "백분율" else "%{y:,.0f}")
                                  + "<extra></extra>",
                ))

        # ✅ 주말 쉐이딩: 일별에서만
        if mode_label == "일별":
            ui.add_weekend_shading(fig, x_dt)


        # ✅ x축 라벨: ticktext로 기간 문자열 표시
        fig.update_xaxes(
            type="date",
            tickmode="array",
            tickvals=x_dt,
            ticktext=tick_text,
        )
        if scale == "백분율":
            fig.update_yaxes(tickformat=".0%")

        fig.update_layout(
            height=300, margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom", title=None),
        )

        st.plotly_chart(fig, use_container_width=True, key=f"fig_{tk}")


        # ---------- Table : "날짜가 컬럼" (정렬은 _period_dt, 표시는 기간 문자열)
        pt = mat.T  # index=dim, columns=날짜(dt)

        col_map = dt_lbl.set_index("_period_dt")["_period"].to_dict()
        pt = pt.rename(columns=col_map)

        # 컬럼 순서 보장
        pt = pt[[c for c in tick_text if c in pt.columns]]

        if scale == "백분율":
            pt = np.round(pt.astype(float) * 100, 1)
            st.dataframe(pt.style.format("{:.1f}%"), row_height=30, use_container_width=True)
        else:
            pt = np.round(pt.astype(float)).astype(int)
            st.dataframe(pt, row_height=30, use_container_width=True)
        

    # ---- Tabs: 전체 + 대분류(바깥) / 소분류(안쪽)
    outer_tabs = st.tabs(outer_names)

    for ot, maj in zip(outer_tabs, outer_names):
        with ot:
            if maj == "전체":
                _render_block(df, tab_key="all", fixed_types=None)
                continue

            plain = sorted(list(major_map[maj]["plain"]))
            subs_full = sorted(list(major_map[maj]["subs"]))
            all_types = plain + subs_full

            if len(subs_full) == 0:
                _render_block(df, tab_key=f"maj_{maj}", fixed_types=all_types)
                continue

            inner_labels = ["전체"] + [t.split("_", 1)[1] for t in subs_full]
            inner_tabs = st.tabs(inner_labels)

            for it, sub_lbl in zip(inner_tabs, inner_labels):
                with it:
                    if sub_lbl == "전체":
                        _render_block(df, tab_key=f"maj_{maj}_all", fixed_types=all_types)
                    else:
                        full_type = f"{maj}_{sub_lbl}"
                        _render_block(df, tab_key=f"maj_{maj}_sub_{sub_lbl}", fixed_types=[full_type])


    # ──────────────────────────────────
    # 3) 검색량 급변 탐지
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>검색량 급변 탐지</span></h5>", unsafe_allow_html=True)
    st.markdown(
        ":gray-badge[:material/Info: Info]ㅤ검색량이 급격하게 증가하거나 감소한 키워드를 탐지하여 시장 트렌드를 확인합니다. ",
        unsafe_allow_html=True,
    )

    outer_tabs = st.tabs(outer_names)

    def _render_spike_kw(d0: pd.DataFrame, tab_key: str, fixed_types: list[str] | None, is_all_tab: bool):
        tk = _clean_key(tab_key)

        k_mode = f"sp_mode_{tk}"
        k_prev = f"sp_prev_{tk}"
        k_cur  = f"sp_cur_{tk}"

        if k_mode not in st.session_state:
            st.session_state[k_mode] = "최근 7일"

        if k_prev not in st.session_state or k_cur not in st.session_state:
            ps, pe, cs, ce = _calc_ranges(_max_d, 7)
            st.session_state[k_prev] = [ps, pe]
            st.session_state[k_cur]  = [cs, ce]

        def _sp_apply_win():
            m = st.session_state[k_mode]
            if m == "커스텀":
                return
            n_map = {"최근 7일": 7, "최근 14일": 14, "최근 30일": 30}
            n = n_map.get(m, 7)
            ps, pe, cs, ce = _calc_ranges(_max_d, n)
            st.session_state[k_prev] = [ps, pe]
            st.session_state[k_cur]  = [cs, ce]

        with st.expander("Filter", expanded=True):
            c1, c2, c3 = st.columns([1.3, 1.2, 1.2], vertical_alignment="bottom")
            c4, c5, c6 = st.columns([1.3, 1.2, 1.2], vertical_alignment="bottom")

            with c1:
                st.radio(
                    "기간 윈도우 설정",
                    ["최근 7일", "최근 14일", "최근 30일", "커스텀"],
                    horizontal=True,
                    key=k_mode,
                    on_change=_sp_apply_win,
                )

            lock = (st.session_state[k_mode] != "커스텀")

            with c2:
                prev_rng = st.date_input("이전기간 선택", key=k_prev, disabled=lock)
            with c3:
                cur_rng = st.date_input("비교기간(최근) 선택", key=k_cur, disabled=lock)

            with c4:
                show = st.radio(
                    "증감 방향",
                    ["증가", "감소", "둘다"],
                    horizontal=True,
                    index=0,
                    key=f"sp_show_{tk}",
                )
            with c5:
                min_diff = st.number_input(
                    "최소 증감량",
                    min_value=0,
                    value=50,
                    step=10,
                    key=f"sp_min_{tk}",
                )
            with c6:
                topn = st.selectbox(
                    "Top N",
                    options=CFG["TOPK_OPTS"],
                    index=CFG["TOPK_OPTS"].index(10) if 10 in CFG["TOPK_OPTS"] else 0,
                    key=f"sp_topn_{tk}",
                )

        # range normalize (QUICK INSIGHT 방식)
        if not (isinstance(prev_rng, (list, tuple)) and len(prev_rng) == 2):
            prev_rng = st.session_state[k_prev]
        if not (isinstance(cur_rng, (list, tuple)) and len(cur_rng) == 2):
            cur_rng = st.session_state[k_cur]

        prev_s, prev_e = pd.to_datetime(prev_rng[0]), pd.to_datetime(prev_rng[1])
        cur_s,  cur_e  = pd.to_datetime(cur_rng[0]),  pd.to_datetime(cur_rng[1])

        if prev_s > prev_e:
            prev_s, prev_e = prev_e, prev_s
        if cur_s > cur_e:
            cur_s, cur_e = cur_e, cur_s

        d = d0[(d0["날짜"] >= min(prev_s, cur_s)) & (d0["날짜"] <= max(prev_e, cur_e))]
        if fixed_types is not None:
            d = d[d["키워드유형"].astype(str).isin([str(x) for x in fixed_types])]

        if d.empty:
            st.warning("선택 기간/조건에 데이터가 없습니다.")
            return

        # kw_total 대표 베이스는 한 번만 생성해서 재사용
        base = _kw_total_base(d)

        # (1) 기간 합계
        if is_all_tab:
            cur_sum = (
                base[(base["날짜"] >= cur_s) & (base["날짜"] <= cur_e)]
                .groupby(["키워드유형"], as_index=False)["kw_total"].sum()
                .rename(columns={"kw_total": "비교기간(최근)"})
            )
            prev_sum = (
                base[(base["날짜"] >= prev_s) & (base["날짜"] <= prev_e)]
                .groupby(["키워드유형"], as_index=False)["kw_total"].sum()
                .rename(columns={"kw_total": "이전기간"})
            )
            out = cur_sum.merge(prev_sum, on=["키워드유형"], how="outer")
        else:
            cur_sum = (
                base[(base["날짜"] >= cur_s) & (base["날짜"] <= cur_e)]
                .groupby(["키워드유형", "키워드"], as_index=False)["kw_total"].sum()
                .rename(columns={"kw_total": "비교기간(최근)"})
            )
            prev_sum = (
                base[(base["날짜"] >= prev_s) & (base["날짜"] <= prev_e)]
                .groupby(["키워드유형", "키워드"], as_index=False)["kw_total"].sum()
                .rename(columns={"kw_total": "이전기간"})
            )
            out = cur_sum.merge(prev_sum, on=["키워드유형", "키워드"], how="outer")

        out["비교기간(최근)"] = pd.to_numeric(out["비교기간(최근)"], errors="coerce").fillna(0)
        out["이전기간"]       = pd.to_numeric(out["이전기간"], errors="coerce").fillna(0)
        out["증감량"]     = out["비교기간(최근)"] - out["이전기간"]
        out["증감률"]         = np.where(out["이전기간"] > 0, out["증감량"] / out["이전기간"], np.nan)

        # (2) 노이즈 컷 + 방향 필터
        out = out[out["증감량"].abs() >= float(min_diff)]
        if show == "증가":
            out = out[out["증감량"] > 0]
        elif show == "감소":
            out = out[out["증감량"] < 0]

        if out.empty:
            st.info("조건을 만족하는 항목이 없습니다.")
            return

        # (3) TopN 적용
        out["abs_diff"] = out["증감량"].abs()
        if is_all_tab:
            out = (
                out.sort_values("abs_diff", ascending=False)
                .head(int(topn))
                .reset_index(drop=True)
            )
        else:
            out = (
                out.sort_values(["키워드유형", "abs_diff"], ascending=[True, False])
                .groupby("키워드유형", as_index=False, group_keys=False)
                .apply(lambda g: g.head(int(topn)))
                .reset_index(drop=True)
            )

        # 표 데이터
        if is_all_tab:
            t = out[["키워드유형", "이전기간", "비교기간(최근)", "증감량", "증감률"]].copy()
        else:
            t = out[["키워드유형", "키워드", "이전기간", "비교기간(최근)", "증감량", "증감률"]].copy()

        # (4) Dumbbell Chart
        LINE_UP = "#2E7D32"   # 증가(초록)
        LINE_DN = "#D32F2F"   # 감소(빨강)
        LINE_EQ = "#64748B"   # 변화 없음(슬레이트)
        MK_PREV = "#64748B"   # 이전기간 마커
        MK_CUR  = "#181818"   # 최근기간 마커

        dmb = out.assign(
            prev=lambda x: pd.to_numeric(x["이전기간"], errors="coerce").fillna(0),
            cur =lambda x: pd.to_numeric(x["비교기간(최근)"], errors="coerce").fillna(0),
        )
        dmb["diff"] = dmb["cur"] - dmb["prev"]
        dmb["abs_diff"] = dmb["diff"].abs()

        grp_col = None if is_all_tab else "키워드유형"
        y_col   = "키워드유형" if is_all_tab else "키워드"
        groups = [(None, dmb)] if grp_col is None else list(dmb.groupby(grp_col, dropna=False))

        for gk, g0 in groups:
            g = g0.sort_values("abs_diff", ascending=False).head(int(topn))
            if g.empty:
                continue

            g = g.sort_values("abs_diff", ascending=True).reset_index(drop=True)

            y_lbl = g[y_col].astype(str).tolist()
            n = len(g)
            h = max(200, n * 22 + 90) # 최소높이, 줄간격사이, 상단/하단 여백

            prev = g["prev"].astype(float).to_numpy()
            cur  = g["cur"].astype(float).to_numpy()
            diff = (cur - prev)

            x_min = float(min(prev.min(), cur.min()))
            x_max = float(max(prev.max(), cur.max()))
            span = max(x_max - x_min, 1.0)

            # ✅ 오른쪽 고정 여백(축에 먼저 확보)
            GUTTER_RATIO = 0.22   # 오른쪽 여백 비율(고정)
            TEXT_PAD_RATIO = 0.045
            text_pad = max(span * TEXT_PAD_RATIO, max(abs(x_max), 1.0) * 0.01, 1.0)
            x_gutter_max = x_max + span * GUTTER_RATIO
            x_axis_max   = max(x_gutter_max, x_max + text_pad * 3)

            # 각 행 오른쪽 끝
            x_end = np.maximum(prev, cur)

            # ✅ 텍스트는 "선 끝 옆"에 두되, 항상 gutter 안쪽으로 clamp
            x_text = x_end + text_pad
            x_text = np.minimum(x_text, x_axis_max - text_pad * 0.6)

            diff_txt = np.where(diff > 0, np.char.add("+", np.char.mod("%0.0f", diff)),
                    np.where(diff < 0, np.char.mod("%0.0f", diff), "0"))

            fig = go.Figure()

            # 선
            for i in range(n):
                lc = LINE_UP if diff[i] > 0 else (LINE_DN if diff[i] < 0 else LINE_EQ)
                fig.add_trace(go.Scatter(
                    x=[prev[i], cur[i]],
                    y=[y_lbl[i], y_lbl[i]],
                    mode="lines",
                    line=dict(width=3, color=lc),
                    showlegend=False,
                    hovertemplate=(
                        f"{y_lbl[i]}<br>"
                        f"이전 {prev[i]:,.0f} → 최근 {cur[i]:,.0f}<br>"
                        f"증감 {diff[i]:+,.0f}<extra></extra>"
                    ),
                ))

            # 기준선
            # fig.add_vline(x=np.mean(prev), line_dash="dot", line_color="#64748B",
            #                 annotation_text=f"이전 평균 {np.mean(prev):,.0f}",
            #                 annotation_position="top left",)
            # fig.add_vline(x=np.mean(cur), line_dash="dot", line_color="#181818",
            #                 annotation_text=f"최근 평균 {np.mean(cur):,.0f}",
            #                 annotation_position="bottom right",)

            # 마커
            fig.add_trace(go.Scatter(
                x=prev, y=y_lbl,
                mode="markers",
                name="이전기간",
                marker=dict(size=8, symbol="circle", color=MK_PREV),
            ))
            fig.add_trace(go.Scatter(
                x=cur, y=y_lbl,
                mode="markers",
                name="비교기간(최근)",
                marker=dict(size=8, symbol="diamond", color=MK_CUR),
            ))

            # 증감 텍스트 (겹침 방지: gutter 확보 + clamp)
            fig.add_trace(go.Scatter(
                x=x_text,
                y=y_lbl,
                mode="text",
                text=diff_txt.tolist(),
                textposition="middle left",
                showlegend=False,
                opacity=0.9,
                cliponaxis=False,
                hoverinfo="skip",
            ))

            fig.update_xaxes(range=[x_min, x_axis_max])

            fig.update_layout(
                height=h,
                margin=dict(l=10, r=10, t=60, b=10),
                legend=dict(
                    orientation="h",
                    x=1, xanchor="right",
                    y=1.5, yanchor="top",
                    title=None,
                ),
            )

            fig.update_yaxes(
                categoryorder="array",
                categoryarray=y_lbl,
                automargin=True
            )

            key_tag = "type" if is_all_tab else _clean_key(str(gk))
            st.plotly_chart(fig, use_container_width=True, key=f"sp_dumbbell_{tk}_{key_tag}")

        # (5) Table
        t["이전기간"] = pd.to_numeric(t["이전기간"], errors="coerce").fillna(0).round(0).astype(int)
        t["비교기간(최근)"] = pd.to_numeric(t["비교기간(최근)"], errors="coerce").fillna(0).round(0).astype(int)
        t["증감량"] = pd.to_numeric(t["증감량"], errors="coerce").fillna(0).round(0).astype(int)

        t_disp = t.copy()
        t_disp["증감률(%)"] = (t_disp["증감률"] * 100).round(1)
        t_disp = t_disp.drop(columns=["증감률", "abs_diff"], errors="ignore")

        st.dataframe(
            t_disp,
            use_container_width=True,
            hide_index=True,
            row_height=30
        )

    for ot, maj in zip(outer_tabs, outer_names):
        with ot:
            if maj == "전체":
                _render_spike_kw(df, tab_key="sp_all", fixed_types=None, is_all_tab=True)
                continue

            plain = sorted(list(major_map[maj]["plain"]))
            subs_full = sorted(list(major_map[maj]["subs"]))
            all_types = plain + subs_full

            if len(subs_full) == 0:
                _render_spike_kw(df, tab_key=f"sp_maj_{maj}", fixed_types=all_types, is_all_tab=False)
                continue

            inner_labels = ["전체"] + [t.split("_", 1)[1] for t in subs_full]
            inner_tabs = st.tabs(inner_labels)

            for it, sub_lbl in zip(inner_tabs, inner_labels):
                with it:
                    if sub_lbl == "전체":
                        _render_spike_kw(df, tab_key=f"sp_maj_{maj}_all", fixed_types=all_types, is_all_tab=True)
                    else:
                        full_type = f"{maj}_{sub_lbl}"
                        _render_spike_kw(df, tab_key=f"sp_maj_{maj}_sub_{sub_lbl}", fixed_types=[full_type], is_all_tab=False)


    # ──────────────────────────────────
    # 4) 연령대 급변 탐지
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>연령대 급변 탐지</span></h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ단순 검색량을 넘어, 연령대별 비중 분포의 변화량을 측정하여 타겟층의 구조적 변화가 큰 키워드를 탐지합니다.")

    outer_tabs = st.tabs(outer_names)

    def _render_shift_detail(dd_cur: pd.DataFrame, key_tag: str):
        dd_cur["age_info"] = pd.Categorical(dd_cur["age_info"], categories=AGE_ORDER, ordered=True)
        dd_cur = dd_cur.sort_values("age_info")

        g1, g2 = st.columns([1.2, 1])

        with g1:
            fig2 = go.Figure()
            fig2.add_bar(
                x=dd_cur["age_info"].astype(str),
                y=dd_cur["share_prev"],
                name="이전 비중",
                opacity=0.6,
                hovertemplate="%{x}<br>%{y:.1%}<extra></extra>",
            )
            fig2.add_bar(
                x=dd_cur["age_info"].astype(str),
                y=dd_cur["share_cur"],
                name="최근 비중",
                opacity=0.85,
                hovertemplate="%{x}<br>%{y:.1%}<extra></extra>",
            )
            fig2.update_layout(
                barmode="group",
                height=300,
                margin=dict(l=10, r=10, t=10, b=10),
                yaxis_tickformat=".0%",
                legend=dict(orientation="h", y=1.02, yanchor="bottom", x=1, xanchor="right"),
            )
            st.plotly_chart(fig2, use_container_width=True, key=f"shift_bar_{key_tag}")

        with g2:
            t3 = dd_cur[["age_info", "share_prev", "share_cur", "dshare"]].rename(columns={
                "age_info": "연령대",
                "share_prev": "이전(%)",
                "share_cur": "최근(%)",
                "dshare": "변화(pp)",
            })
            t3["이전(%)"] = (t3["이전(%)"] * 100).round(1)
            t3["최근(%)"] = (t3["최근(%)"] * 100).round(1)
            t3["변화(pp)"] = (t3["변화(pp)"] * 100).round(1)
            st.dataframe(t3, use_container_width=True, hide_index=True, row_height=30)

    def _render_shift_kw(d0: pd.DataFrame, tab_key: str, fixed_types: list[str] | None, is_all_tab: bool):
        tk = _clean_key(tab_key)

        k_mode = f"shift_mode_{tk}"
        k_prev = f"shift_prev_{tk}"
        k_cur  = f"shift_cur_{tk}"

        if k_mode not in st.session_state:
            st.session_state[k_mode] = "최근 7일"

        if k_prev not in st.session_state or k_cur not in st.session_state:
            ps, pe, cs, ce = _calc_ranges(_max_d, 7)
            st.session_state[k_prev] = [ps, pe]
            st.session_state[k_cur]  = [cs, ce]

        def _shift_apply_win():
            m0 = st.session_state[k_mode]
            if m0 == "커스텀":
                return
            n_map = {"최근 7일": 7, "최근 14일": 14, "최근 30일": 30}
            n = n_map.get(m0, 7)
            ps, pe, cs, ce = _calc_ranges(_max_d, n)
            st.session_state[k_prev] = [ps, pe]
            st.session_state[k_cur]  = [cs, ce]

        def _norm_rng(rng, key):
            if not (isinstance(rng, (list, tuple)) and len(rng) == 2):
                rng = st.session_state[key]
            s, e = pd.to_datetime(rng[0]), pd.to_datetime(rng[1])
            return (e, s) if s > e else (s, e)

        def _build_m(cur_x: pd.DataFrame, prev_x: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
            cd = cur_x.groupby(keys + ["age_info"], as_index=False)["abs_age"].sum()
            pd0 = prev_x.groupby(keys + ["age_info"], as_index=False)["abs_age"].sum()

            cd["den"] = cd.groupby(keys)["abs_age"].transform("sum")
            cd["share_cur"] = np.where(cd["den"] > 0, cd["abs_age"] / cd["den"], 0.0)

            pd0["den"] = pd0.groupby(keys)["abs_age"].transform("sum")
            pd0["share_prev"] = np.where(pd0["den"] > 0, pd0["abs_age"] / pd0["den"], 0.0)

            mm = cd[keys + ["age_info", "share_cur"]].merge(
                pd0[keys + ["age_info", "share_prev"]],
                on=keys + ["age_info"],
                how="outer",
            )
            mm["share_cur"] = mm["share_cur"].fillna(0.0)
            mm["share_prev"] = mm["share_prev"].fillna(0.0)
            mm["dshare"] = mm["share_cur"] - mm["share_prev"]
            return mm

        def _build_top(mm: pd.DataFrame, keys: list[str], min_diff: float, topn2: int) -> pd.DataFrame:
            sc = (
                mm.groupby(keys, as_index=False)
                .agg(shift_score=("dshare", lambda x: float(np.abs(x).sum()) * 100.0))
            )
            up = (
                mm.sort_values("dshare", ascending=False)
                .groupby(keys, as_index=False)
                .head(2)
                .groupby(keys)["age_info"]
                .apply(lambda s: ", ".join([str(x) for x in s.tolist()]))
                .reset_index()
                .rename(columns={"age_info": "증가 연령대 Top2"})
            )
            dn = (
                mm.sort_values("dshare", ascending=True)
                .groupby(keys, as_index=False)
                .head(2)
                .groupby(keys)["age_info"]
                .apply(lambda s: ", ".join([str(x) for x in s.tolist()]))
                .reset_index()
                .rename(columns={"age_info": "감소 연령대 Top2"})
            )
            o2 = sc.merge(up, on=keys, how="left").merge(dn, on=keys, how="left")
            o2["shift_score"] = pd.to_numeric(o2["shift_score"], errors="coerce").fillna(0).round(1)
            o2 = o2[o2["shift_score"] >= float(min_diff)]
            return o2.sort_values("shift_score", ascending=False).head(int(topn2)).reset_index(drop=True)

        # ✅ Filter(3영역 구조), c4만 pass
        with st.expander("Filter", expanded=True):
            c1, c2, c3 = st.columns([1.3, 1.2, 1.2], vertical_alignment="bottom")
            c4, c5, c6 = st.columns([1.3, 1.2, 1.2], vertical_alignment="bottom")

            with c1:
                st.radio(
                    "기간 윈도우 설정",
                    ["최근 7일", "최근 14일", "최근 30일", "커스텀"],
                    horizontal=True,
                    key=k_mode,
                    on_change=_shift_apply_win,
                )

            lock = (st.session_state[k_mode] != "커스텀")

            with c2:
                prev_rng = st.date_input("이전기간 선택", key=k_prev, disabled=lock)
            with c3:
                cur_rng = st.date_input("비교기간(최근) 선택", key=k_cur, disabled=lock)

            with c4:
                pass

            with c5:
                min_diff = st.number_input(
                    "최소 변화점수(Shift Score)",
                    min_value=0.0,
                    value=3.0,
                    step=0.5,
                    format="%.1f",
                    key=f"shift_min_{tk}",
                )

            with c6:
                topn2 = st.selectbox(
                    "Top N",
                    options=CFG["TOPK_OPTS"],
                    index=CFG["TOPK_OPTS"].index(10) if 10 in CFG["TOPK_OPTS"] else 0,
                    key=f"shift_topn_{tk}",
                )

        prev_s, prev_e = _norm_rng(prev_rng, k_prev)
        cur_s,  cur_e  = _norm_rng(cur_rng, k_cur)

        d = d0[(d0["날짜"] >= min(prev_s, cur_s)) & (d0["날짜"] <= max(prev_e, cur_e))]
        if fixed_types is not None:
            d = d[d["키워드유형"].astype(str).isin([str(x) for x in fixed_types])]

        if d.empty:
            st.warning("선택 기간/조건에 데이터가 없습니다.")
            return

        cur_a  = d[(d["날짜"] >= cur_s) & (d["날짜"] <= cur_e)]
        prev_a = d[(d["날짜"] >= prev_s) & (d["날짜"] <= prev_e)]

        keys = ["키워드유형"] if is_all_tab else ["키워드유형", "키워드"]
        mm = _build_m(cur_a, prev_a, keys=keys)


        out2_disp = _build_top(mm, keys=keys, min_diff=float(min_diff), topn2=int(topn2))
        if out2_disp.empty:
            st.info("조건을 만족하는 항목이 없습니다.")
            return
        out2_disp = out2_disp.reset_index(drop=True)


        # ✅ 1 : 1
        st.markdown(" ")
        L2, R2 = st.columns([1, 1], gap="large", vertical_alignment="top")

        with L2:
            st.markdown("<h6 style='margin:0;'>🔥 변화점수 TOP</h6>", unsafe_allow_html=True)
            st.markdown(
                "<p style='margin:-10px 0 12px 0; color:#6c757d; font-size:13px;'>"
                "연령대 비중 분포의 변화가 큰 키워드(유형)와, 어떤 연령대에서 증감했는지 확인합니다."
                "</p>",
                unsafe_allow_html=True
            )
            st.dataframe(out2_disp, use_container_width=True, hide_index=True, height=335, row_height=30)

        with R2:
            st.markdown("<h6 style='margin:0;'>🔥 연령대 변화</h6>", unsafe_allow_html=True)
            st.markdown(
                "<p style='margin:-10px 0 12px 0; color:#6c757d; font-size:13px;'>"
                "선택한 키워드(유형)의 이전/최근 연령대 비중을 비교합니다."
                "</p>",
                unsafe_allow_html=True
            )
            if is_all_tab:
                pick_type = st.selectbox(
                    "", # 제목 없이
                    options=out2_disp["키워드유형"].astype(str).tolist(),
                    index=0,
                    key=f"shift_pick_type_{tk}",
                    label_visibility="collapsed",
                )
                dd_cur = mm[mm["키워드유형"].astype(str) == str(pick_type)]
            else:
                sel_opts = out2_disp["키워드"].astype(str).tolist()
                sel_val = st.selectbox(
                    "",
                    options=sel_opts,
                    index=0,
                    key=f"shift_pick_kw_{tk}",
                    label_visibility="collapsed",
                )
                i = int(sel_opts.index(sel_val))
                pick_type2 = str(out2_disp.loc[i, "키워드유형"])
                pick_kw    = str(out2_disp.loc[i, "키워드"])
                dd_cur = mm[
                    (mm["키워드유형"].astype(str) == pick_type2) &
                    (mm["키워드"].astype(str) == pick_kw)
                ]

            _render_shift_detail(dd_cur, key_tag=tk)

        return

    for ot, maj in zip(outer_tabs, outer_names):
        with ot:
            if maj == "전체":
                _render_shift_kw(df, tab_key="shift_all", fixed_types=None, is_all_tab=True)
                continue

            plain = sorted(list(major_map[maj]["plain"]))
            subs_full = sorted(list(major_map[maj]["subs"]))
            all_types = plain + subs_full

            if len(subs_full) == 0:
                _render_shift_kw(df, tab_key=f"shift_maj_{maj}", fixed_types=all_types, is_all_tab=False)
                continue

            inner_labels = ["전체"] + [t.split("_", 1)[1] for t in subs_full]
            inner_tabs = st.tabs(inner_labels)

            for it, sub_lbl in zip(inner_tabs, inner_labels):
                with it:
                    if sub_lbl == "전체":
                        _render_shift_kw(df, tab_key=f"shift_maj_{maj}_all", fixed_types=all_types, is_all_tab=True)
                    else:
                        full_type = f"{maj}_{sub_lbl}"
                        _render_shift_kw(df, tab_key=f"shift_maj_{maj}_sub_{sub_lbl}", fixed_types=[full_type], is_all_tab=False)


if __name__ == "__main__":
    main()