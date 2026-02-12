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
def render_tab(df: pd.DataFrame, df_aw: pd.DataFrame, title: str, conf: dict):
    pie_dim = conf["pie"]
    x = conf["stack_x"]
    c = conf["stack_color"]

    if df is None or df.empty:
        st.warning("선택된 조건에 해당하는 데이터가 없습니다.")
        return

    AW_COLS = {"awareness_type", "awareness_type_a", "awareness_type_b"}
    use_aw = (pie_dim in AW_COLS) or (c in AW_COLS)
    src = df_aw if (use_aw and df_aw is not None and not df_aw.empty) else df

    def _clean_cat(s: pd.Series) -> pd.Series:
        return (
            s.astype(str)
             .replace("nan", "")
             .fillna("")
             .replace("", "기타")
        )

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
                color_discrete_map=(
                    color_map if (color_map is not None and pie_dim == dim_for_map) else None
                ),
            )
            fig1.update_layout(
                height=360,
                margin=dict(l=0, r=0, t=30, b=30),
                showlegend=False,
            )
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
                    agg,
                    x="_period_dt",
                    y="value",
                    color=c,
                    barmode="stack",
                    opacity=0.6,
                    color_discrete_map=color_map if color_map is not None else None,
                )
                fig2.update_layout(
                    height=360,
                    margin=dict(l=10, r=140, t=20, b=10),
                    xaxis_title=None,
                    yaxis_title=None,
                    legend=dict(
                        orientation="v",
                        x=1.02, xanchor="left",
                        y=1, yanchor="top"
                    ),
                )
                fig2.update_traces(
                    hovertemplate="%{x|%Y-%m-%d}<br>%{fullData.name}: %{y:,}<extra></extra>"
                )
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
                    agg,
                    x=x,
                    y="value",
                    color=c,
                    barmode="stack",
                    opacity=0.6,
                    color_discrete_map=color_map if color_map is not None else None,
                )
                fig2.update_layout(
                    height=360,
                    margin=dict(l=10, r=10, t=10, b=10),
                    xaxis_title=None,
                    yaxis_title=None,
                    legend=dict(orientation="h", y=1.02, x=1, xanchor="right"),
                )
                fig2.update_traces(
                    hovertemplate="%{x}<br>%{fullData.name}: %{y:,}<extra></extra>"
                )
                st.plotly_chart(fig2, use_container_width=True, key=f"stack::{title}::{x}::{c}")

                pv = ui.build_pivot_table(agg, index_col=c, col_col=x, val_col="value")
        else:
            st.info("Stack 차원 컬럼이 없습니다.")

    # ── Table ─────────────────────────
    if pv is not None:
        st.dataframe(pv, use_container_width=True, hide_index=True, row_height=30)
    else:
        st.info("표를 만들 수 있는 데이터가 없습니다.")


# ──────────────────────────────────
# main
# ──────────────────────────────────
def main():
    # ──────────────────────────────────
    # A) Layout / CSS
    # ──────────────────────────────────
    st.markdown(CFG["CSS_BLOCK_CONTAINER"], unsafe_allow_html=True)
    st.markdown(CFG["CSS_TABS"], unsafe_allow_html=True)
    px.defaults.color_discrete_sequence = px.colors.qualitative.Pastel2

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

        # 기간 필터
        df = df[(df["event_date"] >= pd.to_datetime(cs)) & (df["event_date"] < pd.to_datetime(ce))]

        # (범주화)
        cat_cols = ["shrm_name", "shrm_type", "demo_gender", "demo_age", "awareness_type", "purchase_purpose", "visit_type"]
        for c in cat_cols:
            if c in df.columns:
                df[c] = df[c].astype("category")

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
    # 1) 영역
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>제목</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명")

    # ✅ 탭 구성/순서 변경
    DIM_MAP = {
        "쇼룸형태": {  # shrm_type
            "pie": "shrm_type",
            "stack_x": "event_date",
            "stack_color": "shrm_type",
            "raw_cols": ["event_date", "shrm_type"]
        },
        "쇼룸구분": {  # shrm_name
            "pie": "shrm_name",
            "stack_x": "event_date",
            "stack_color": "shrm_name",
            "raw_cols": ["event_date", "shrm_name"]
        },
        "방문유형": {  # visit_type
            "pie": "visit_type",
            "stack_x": "event_date",
            "stack_color": "visit_type",
            "raw_cols": ["event_date", "visit_type"]
        },
        "데모그래픽": {  # 동일
            "pie": "demo_gender",
            "stack_x": "demo_age",
            "stack_color": "demo_gender",
            "raw_cols": ["event_date", "demo_gender", "demo_age"]
        },
        "인지단계": {  # awareness_type_a (df_aw)
            "pie": "awareness_type_a",
            "stack_x": "event_date",
            "stack_color": "awareness_type_a",
            "raw_cols": ["event_date", "awareness_type_a"]
        },
        "인지채널": {  # awareness_type_b (df_aw)
            "pie": "awareness_type_b",
            "stack_x": "event_date",
            "stack_color": "awareness_type_b",
            "raw_cols": ["event_date", "awareness_type_b"]
        },
        "구매목적": {  # 동일
            "pie": "purchase_purpose",
            "stack_x": "event_date",
            "stack_color": "purchase_purpose",
            "raw_cols": ["event_date", "purchase_purpose"]
        },
    }

    tabs = st.tabs(list(DIM_MAP.keys()))
    for tab, name in zip(tabs, DIM_MAP.keys()):
        with tab:
            render_tab(df, df_aw, name, DIM_MAP[name])



    # ──────────────────────────────────
    # 2) Cross Insight
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>Cross Insight</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Insight] 유형 간 관계 확인")

    DIM_OPTS = {
        "쇼룸형태": "shrm_type",
        "쇼룸구분": "shrm_name",
        "방문유형": "visit_type",
        "성별": "demo_gender",
        "연령대": "demo_age",
        "인지단계": "awareness_type_a",
        "인지채널": "awareness_type_b",
        "구매 목적": "purchase_purpose",
    }

    c1, c2 = st.columns(2)
    with c1:
        row_label = st.selectbox("행 기준", options=list(DIM_OPTS.keys()), index=0, key="cross_row")
    with c2:
        col_label = st.selectbox(
            "열 기준",
            options=[k for k in DIM_OPTS.keys() if k != row_label],
            index=0,
            key="cross_col"
        )

    row_col = DIM_OPTS[row_label]
    col_col = DIM_OPTS[col_label]

    if (row_col in df.columns) or (df_aw is not None and row_col in df_aw.columns):
        if (col_col in df.columns) or (df_aw is not None and col_col in df_aw.columns):

            AW_COLS = {"awareness_type", "awareness_type_a", "awareness_type_b"}
            use_aw = (row_col in AW_COLS) or (col_col in AW_COLS)

            if use_aw:
                if df_aw is None or df_aw.empty:
                    st.info("awareness 분해 데이터(df_aw)가 없습니다.")
                    return
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

            def _clean_cat(s: pd.Series) -> pd.Series:
                return (
                    s.astype(str)
                     .replace("nan", "")
                     .fillna("")
                     .replace("", "기타")
                )

            agg[row_col] = _clean_cat(agg[row_col])
            agg[col_col] = _clean_cat(agg[col_col])

            # ✅ 행 기준 정렬 규칙
            # - 기본: 행 합(value) 큰 순
            # - demo_age면: 20대 → 30대 → 40대 → 50대 → 60대 이상 우선
            # - 기타 가 있으면 항상 맨 마지막
            row_sum = (
                agg.groupby(row_col, dropna=False)["value"]
                   .sum()
                   .sort_values(ascending=False)
            )
            base_order = row_sum.index.astype(str).tolist()

            etc_keys = ["기타"]
            etc_in = [k for k in etc_keys if k in base_order]

            if row_col == "demo_age":
                age_order = ["20대", "30대", "40대", "50대", "60대 이상"]
                row_order = (
                    [x for x in age_order if x in base_order]
                    + [x for x in base_order if (x not in age_order) and (x not in etc_in)]
                    + etc_in
                )
            else:
                row_order = (
                    [x for x in base_order if x not in etc_in]
                    + etc_in
                )

            # ✅ 행 기준 퍼센트
            agg["_row_sum"] = agg.groupby(row_col, dropna=False)["value"].transform("sum").replace(0, np.nan)
            agg["pct_row"] = (agg["value"] / agg["_row_sum"] * 100).fillna(0)
            agg = agg.drop(columns=["_row_sum"])

            # 피벗 2종
            pv_cnt = ui.build_pivot_table(agg, index_col=row_col, col_col=col_col, val_col="value")
            pv_pct = ui.build_pivot_table(agg, index_col=row_col, col_col=col_col, val_col="pct_row")

            # ✅ 피벗도 행 순서 강제
            if row_col in pv_cnt.columns:
                pv_cnt = (
                    pv_cnt.set_index(row_col)
                          .reindex(row_order)
                          .reset_index()
                )
            if row_col in pv_pct.columns:
                pv_pct = (
                    pv_pct.set_index(row_col)
                          .reindex(row_order)
                          .reset_index()
                )

            # ── 가로 100% 누적막대 (행 기준) ─────────────────
            bar = agg[[row_col, col_col, "pct_row"]].rename(columns={"pct_row": "pct"})
            bar[row_col] = pd.Categorical(bar[row_col].astype(str), categories=row_order, ordered=True)
            bar[col_col] = bar[col_col].astype(str)
            bar = bar.sort_values(row_col)


            fig = px.bar(
                bar,
                y=row_col,
                x="pct",
                color=col_col,
                orientation="h",
                barmode="stack",
                text=bar["pct"].round(0).astype(int).astype(str) + "%",
            )

            # ✅ 표(row_order)와 그래프 순서 완전 동일하게 고정 (역순 방지)
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

            # ── 화면용 합친 표 (막대그래프 row_order와 동일하게 고정) ─────────────────
            pv_show = pv_cnt.copy()

            # ✅ pv_cnt/pv_pct는 위에서 이미 row_order로 reindex 했지만,
            # 혹시라도 중간 로직 변경 시에도 표 순서가 깨지지 않게 한 번 더 고정
            if row_col in pv_show.columns:
                pv_show = (
                    pv_show.set_index(row_col)
                           .reindex(row_order)
                           .reset_index()
                )

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


        else:
            st.info("선택한 차원 컬럼이 없습니다.")
    else:
        st.info("선택한 차원 컬럼이 없습니다.")


if __name__ == "__main__":
    main()
