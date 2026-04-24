import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery

# ──────────────────────────────────
# CONFIG
# ──────────────────────────────────
CFG = {
    # 기본
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
st.markdown("""
<style> 
    .main-title { font-size: 24px !important; font-weight: 700; margin-bottom: 10px; } 
    .sub-title { font-size: 18px !important; font-weight: 600; margin-top: 24px; margin-bottom: 10px; color: #333; border-bottom: 1px solid #eee; padding-bottom: 5px; } 
    .section-title { font-size: 15px !important; font-weight: 600; margin-bottom: 8px; margin-top: 10px; color: #555; } 
    .stMetric { background-color: #f8f9fa; padding: 15px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); } 
    .js-plotly-plot .plotly text { text-shadow: none !important; font-weight: 500 !important; }
    .filter-box { background-color: #f1f3f5; padding: 15px 20px; border-radius: 8px; margin-bottom: 20px; border: 1px solid #e9ecef; }
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────
# 헬퍼 함수
# ──────────────────────────────────
def render_chart_with_data(fig, df_show, height=350):
    tab1, tab2 = st.tabs(["📊 차트", "📋 데이터"])
    with tab1:
        fig.update_layout(height=height, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        st.dataframe(df_show, use_container_width=True, height=height)

def get_brand_base(x):
    x = str(x).strip()
    x = x.split(" (")[0]
    x = x.split("(")[0]
    return x.strip()

brand_color_map = {
    "슬립퍼": "#66C5CC",
    "누어": "#F89C74",
    "토들즈": "#F6CF71",
    "종료/이탈": "#D5DAE5",
    "미확인": "#D5DAE5",
    "기타": "#E2E2E2"
}

def color_to_rgba(c_str, op=0.15):
    c_str = str(c_str).strip().lower()
    if c_str.startswith('rgb'):
        ps = c_str.replace('rgba(', '').replace('rgb(', '').replace(')', '').split(',')
        return f"rgba({ps[0].strip()}, {ps[1].strip()}, {ps[2].strip()}, {op})"
    h_c = c_str.lstrip('#')
    return f"rgba({int(h_c[0:2], 16)}, {int(h_c[2:4], 16)}, {int(h_c[4:6], 16)}, {op})"

def generate_sankey(df_journey_data, top_n=None):
    df_temp = df_journey_data.copy()
    
    if top_n is not None:
        items = df_temp[["Step 1", "Step 2", "Step 3", "Step 4", "Step 5"]].melt().value.value_counts()
        items = items.drop("종료/이탈", errors='ignore')
        top_items = items.nlargest(top_n).index.tolist()
        
        def map_top(val):
            if val == "종료/이탈" or val == "미확인": return val
            if val in top_items: return val
            return "기타"
        
        for c in ["Step 1", "Step 2", "Step 3", "Step 4", "Step 5"]:
            df_temp[c] = df_temp[c].apply(map_top)

    links = []
    for step_i in range(1, 5):
        source_col, target_col = f"Step {step_i}", f"Step {step_i+1}"
        valid_paths = df_temp[df_temp[source_col] != "종료/이탈"]
        if not valid_paths.empty:
            tmp = valid_paths.groupby([source_col, target_col]).size().reset_index(name="value")
            tmp.columns = ["source", "target", "value"]
            tmp["source"] = tmp["source"] + f" ({step_i}차)"
            tmp["target"] = tmp["target"] + f" ({step_i+1}차)"
            links.append(tmp)
    
    if not links:
        return None
        
    all_links = pd.concat(links, axis=0)
    all_nodes = list(dict.fromkeys(all_links["source"].tolist() + all_links["target"].tolist()))
    node_map = {name: i for i, name in enumerate(all_nodes)}
    
    x_vals, y_vals = [], []
    x_map = {"1": 0.01, "2": 0.25, "3": 0.50, "4": 0.75, "5": 0.99}
    
    step_nodes = {"1": [], "2": [], "3": [], "4": [], "5": []}
    for n in all_nodes:
        s_idx = n.rsplit(" (", 1)[1].replace("차)", "")
        if s_idx in step_nodes: step_nodes[s_idx].append(n)
    
    for s_idx in step_nodes:
        step_nodes[s_idx].sort(key=lambda x: (1 if "종료/이탈" in x or "기타" in x or "미확인" in x else 0, x))
        
    for n in all_nodes:
        s_idx = n.rsplit(" (", 1)[1].replace("차)", "")
        x_vals.append(x_map.get(s_idx, 0.99))
        idx_in_step = step_nodes[s_idx].index(n)
        total_in_step = len(step_nodes[s_idx])
        if total_in_step == 1: y_vals.append(0.5)
        else: y_vals.append(0.05 + 0.9 * (idx_in_step / (total_in_step - 1)))

    n_colors = []
    for n in all_nodes:
        base_n = get_brand_base(n)
        n_colors.append(brand_color_map.get(base_n, "#B8C2D1")) 
    
    l_colors = []
    for s in all_links["source"]:
        base_s = get_brand_base(s)
        default_link_color = "rgba(184, 194, 209, 0.2)"
        if base_s in brand_color_map:
            l_colors.append(color_to_rgba(brand_color_map[base_s], 0.2))
        else:
            l_colors.append(default_link_color)

    fig_sankey = go.Figure(data=[go.Sankey(
        node=dict(
            pad=35, thickness=20, 
            line=dict(color="rgba(0,0,0,0.1)", width=0.5), 
            label=all_nodes, color=n_colors, x=x_vals, y=y_vals
        ),
        textfont=dict(size=11, color="black", family="Arial"),
        link=dict(
            source=all_links["source"].map(node_map), 
            target=all_links["target"].map(node_map), 
            value=all_links["value"], color=l_colors
        )
    )])
    fig_sankey.update_layout(height=400, margin=dict(l=10, r=10, t=10, b=10))
    return fig_sankey

# ──────────────────────────────────
# main
# ──────────────────────────────────
def main():
    st.markdown(CFG["CSS_BLOCK_CONTAINER"], unsafe_allow_html=True)
    st.markdown(CFG["CSS_TABS"], unsafe_allow_html=True)

    # ──────────────────────────────────
    # Sidebar (기간)
    # ──────────────────────────────────
    st.sidebar.header("Filter")
    today = datetime.now().date()
    default_end = today - timedelta(days=1)
    default_start = today - timedelta(days=CFG["DEFAULT_LOOKBACK_DAYS"])

    start_date, end_date = st.sidebar.date_input("기간 선택", value=[default_start, default_end], max_value=default_end)
    cs = start_date.strftime("%Y%m%d")
    ce = (end_date + timedelta(days=1)).strftime("%Y%m%d")

    # ──────────────────────────────────
    # Data Load
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def load_data(cs: str, ce: str) -> pd.DataFrame:
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df = bq.get_data("tb_sleeper_f_attribution")
        date_cols = ["event_date", "prev_event_date", "first_event_date"]
        for c in date_cols:
            if c in df.columns: df[c] = pd.to_datetime(df[c], errors="coerce")
        obj_cols = ["_source", "_medium", "_campaign", "_content", "_sourceMedium", "product_cat_a", "first_product_cat_a", "prev_product_cat_a", "source_medium_transition", "source_path_3", "first_product_cat_a", "prev_product_cat_a", "product_cat_a_transition", "return_bucket", "campaign_path_3", "prev_campaign", "prev2_campaign", "campaign_transition"]
        for c in obj_cols:
            if c in df.columns: df[c] = df[c].astype(str).replace({"None": "미확인", "nan": "미확인", "(not set)": "미확인", "": "미확인"}).fillna("미확인")
        if "is_return_user" in df.columns:
            df["is_return_user"] = df["is_return_user"].astype(str).replace({"first": "0"})
            df["is_return_user"] = pd.to_numeric(df["is_return_user"], errors="coerce").fillna(0)
        num_cols = ["visit_order", "days_since_prev", "same_source_medium_yn"]
        for c in num_cols:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    
    with st.spinner("로딩 중입니다..."):
        df_raw = load_data(cs, ce)

    st.subheader("MTJ 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    st.divider()

    # ──────────────────────────────────
    # 브랜드 탭 구성 (최상위 필터 탭)
    # ──────────────────────────────────
    brand_options = sorted([b for b in df_raw["first_product_cat_a"].unique() if b != "미확인"])
    brand_list = ["전체"] + brand_options
    tabs = st.tabs([f" {b} " for b in brand_list])

    for i, tab in enumerate(tabs):
        first_brand_name = brand_list[i]
        with tab:
            df_brand = df_raw if first_brand_name == "전체" else df_raw[df_raw["first_product_cat_a"] == first_brand_name]
            df_target = df_brand.copy()

            if df_target.empty:
                st.info("조건에 맞는 데이터가 없습니다.")
                continue

            # -----------------------------------------------------------------
            # 1. 기초 여정 데이터 전처리
            # -----------------------------------------------------------------
            user_max_visit = df_target.groupby("user_pseudo_id")["visit_order"].max().reset_index(name="max_order")
            steps = [f"Step {idx}" for idx in range(1, 6)]
            lags = [f"Lag {idx}" for idx in range(1, 6)]

            # A. 브랜드 기준 여정
            df_pivot = df_target[df_target["visit_order"] <= 5].pivot(index="user_pseudo_id", columns="visit_order", values="product_cat_a").fillna("종료/이탈")
            df_lag_pivot = df_target[df_target["visit_order"] <= 5].pivot(index="user_pseudo_id", columns="visit_order", values="days_since_prev").fillna(0)
            df_pivot.columns = steps; df_lag_pivot.columns = lags
            df_journey = pd.concat([df_pivot, df_lag_pivot], axis=1).reset_index()
            df_journey = pd.merge(df_journey, user_max_visit, on="user_pseudo_id", how="left")

            def get_final_brand(row):
                for step_idx in range(5, 0, -1):
                    if row[f"Step {step_idx}"] != "종료/이탈": return row[f"Step {step_idx}"]
                return "미확인"
            df_journey["final_brand"] = df_journey.apply(get_final_brand, axis=1)
            df_journey["full_path"] = df_journey["Step 1"] + " ➔ " + df_journey["Step 2"] + " ➔ " + df_journey["Step 3"] + " ➔ " + df_journey["Step 4"] + " ➔ " + df_journey["Step 5"]
            df_journey["valid_step_len"] = sum((df_journey[f"Step {s}"] != "종료/이탈").astype(int) for s in range(1, 6))

            # B. 소스/매체 기준 여정
            df_pivot_sm = df_target[df_target["visit_order"] <= 5].pivot(index="user_pseudo_id", columns="visit_order", values="_sourceMedium").fillna("종료/이탈")
            df_lag_sm = df_target[df_target["visit_order"] <= 5].pivot(index="user_pseudo_id", columns="visit_order", values="days_since_prev").fillna(0)
            df_pivot_sm.columns = steps; df_lag_sm.columns = lags
            df_journey_sm = pd.concat([df_pivot_sm, df_lag_sm], axis=1).reset_index()
            df_journey_sm = pd.merge(df_journey_sm, user_max_visit, on="user_pseudo_id", how="left")

            def get_final_sm(row):
                for step_idx in range(5, 0, -1):
                    if row[f"Step {step_idx}"] != "종료/이탈": return row[f"Step {step_idx}"]
                return "미확인"
            df_journey_sm["final_source_medium"] = df_journey_sm.apply(get_final_sm, axis=1)
            df_journey_sm["full_path"] = df_journey_sm["Step 1"] + " ➔ " + df_journey_sm["Step 2"] + " ➔ " + df_journey_sm["Step 3"] + " ➔ " + df_journey_sm["Step 4"] + " ➔ " + df_journey_sm["Step 5"]
            df_journey_sm["valid_step_len"] = df_journey["valid_step_len"] 

            # C. 캠페인 기준 여정
            df_pivot_camp = df_target[df_target["visit_order"] <= 5].pivot(index="user_pseudo_id", columns="visit_order", values="_campaign").fillna("종료/이탈")
            df_lag_camp = df_target[df_target["visit_order"] <= 5].pivot(index="user_pseudo_id", columns="visit_order", values="days_since_prev").fillna(0)
            df_pivot_camp.columns = steps; df_lag_camp.columns = lags
            df_journey_camp = pd.concat([df_pivot_camp, df_lag_camp], axis=1).reset_index()
            df_journey_camp = pd.merge(df_journey_camp, user_max_visit, on="user_pseudo_id", how="left")

            def get_final_camp(row):
                for step_idx in range(5, 0, -1):
                    if row[f"Step {step_idx}"] != "종료/이탈": return row[f"Step {step_idx}"]
                return "미확인"
            df_journey_camp["final_campaign"] = df_journey_camp.apply(get_final_camp, axis=1)
            df_journey_camp["full_path"] = df_journey_camp["Step 1"] + " ➔ " + df_journey_camp["Step 2"] + " ➔ " + df_journey_camp["Step 3"] + " ➔ " + df_journey_camp["Step 4"] + " ➔ " + df_journey_camp["Step 5"]
            df_journey_camp["valid_step_len"] = df_journey["valid_step_len"] 

            # -----------------------------------------------------------------
            # 2. 통합 공통 필터
            # -----------------------------------------------------------------
            st.markdown(" ")
            with st.expander("🔍 통합 공통 Filter (탭 내 모든 차트 및 지표 연동)", expanded=True):
                # 1열: 기본 조건 필터
                col_f1, col_f2, col_f3 = st.columns(3)
                visit_options = {"1차": 1, "2차": 2, "3차": 3, "4차": 4, "5차+": 5}
                with col_f1:
                    selected_visits = st.multiselect("방문 차수", options=list(visit_options.keys()), default=list(visit_options.keys()), key=f"g_ms_visits_{first_brand_name}")
                with col_f2:
                    step_options = ["전체", "1단계", "2단계", "3단계", "4단계", "5단계"]
                    selected_step_len = st.selectbox("연속 경로 길이", step_options, key=f"g_sel_step_len_{first_brand_name}")
                with col_f3:
                    metric_choice = st.radio("리드타임 집계 기준", ["중간값(Median)", "평균(Mean)"], horizontal=True, key=f"g_metric_{first_brand_name}")
                    is_mean = "평균" in metric_choice

                st.markdown("<div style='margin-bottom:10px;'></div>", unsafe_allow_html=True)

                # 2열: 퍼스트터치 필터
                col_f4, col_f5, col_f6 = st.columns(3)
                with col_f4:
                    first_brand_options = ["전체"] + sorted([x for x in df_journey["Step 1"].dropna().unique() if x != "종료/이탈"])
                    selected_first_brand = st.selectbox("퍼스트터치 브랜드", first_brand_options, key=f"g_sel_first_brand_{first_brand_name}")
                with col_f5:
                    first_sm_options = ["전체"] + sorted([x for x in df_journey_sm["Step 1"].dropna().unique() if x != "종료/이탈"])
                    selected_first_sm = st.selectbox("퍼스트터치 소스/매체", first_sm_options, key=f"g_sel_first_sm_{first_brand_name}")
                with col_f6:
                    first_camp_options = ["전체"] + sorted([x for x in df_journey_camp["Step 1"].dropna().unique() if x != "종료/이탈"])
                    selected_first_camp = st.selectbox("퍼스트터치 캠페인", first_camp_options, key=f"g_sel_first_camp_{first_brand_name}")

                st.markdown("<div style='margin-bottom:10px;'></div>", unsafe_allow_html=True)

                # 3열: 라스트터치 필터
                col_f7, col_f8, col_f9 = st.columns(3)
                with col_f7:
                    final_brand_options = ["전체"] + sorted(df_journey["final_brand"].dropna().unique().tolist())
                    selected_final_brand = st.selectbox("라스트터치 브랜드", final_brand_options, key=f"g_sel_final_brand_{first_brand_name}")
                with col_f8:
                    final_sm_options = ["전체"] + sorted(df_journey_sm["final_source_medium"].dropna().unique().tolist())
                    selected_final_sm = st.selectbox("라스트터치 소스/매체", final_sm_options, key=f"g_sel_final_sm_{first_brand_name}")
                with col_f9:
                    final_camp_options = ["전체"] + sorted(df_journey_camp["final_campaign"].dropna().unique().tolist())
                    selected_final_camp = st.selectbox("라스트터치 캠페인", final_camp_options, key=f"g_sel_final_camp_{first_brand_name}")

            # -----------------------------------------------------------------
            # 3. 필터 적용
            # -----------------------------------------------------------------
            valid_users = set(df_journey["user_pseudo_id"])
            
            # 방문 차수 및 연속 경로 길이 필터
            if selected_visits:
                valid_orders = [visit_options[k] for k in selected_visits if visit_options[k] < 5]
                has_5_plus = "5차+" in "".join(selected_visits)
                cond_order = df_journey["max_order"].isin(valid_orders)
                if has_5_plus: cond_order = cond_order | (df_journey["max_order"] >= 5)
                valid_users = valid_users.intersection(set(df_journey[cond_order]["user_pseudo_id"]))
            else:
                valid_users = set()

            if selected_step_len != "전체":
                step_n = int(selected_step_len.replace("단계", ""))
                valid_users = valid_users.intersection(set(df_journey[df_journey["valid_step_len"] == step_n]["user_pseudo_id"]))
            
            # 퍼스트터치 필터
            if selected_first_brand != "전체":
                valid_users = valid_users.intersection(set(df_journey[df_journey["Step 1"] == selected_first_brand]["user_pseudo_id"]))
            if selected_first_sm != "전체":
                valid_users = valid_users.intersection(set(df_journey_sm[df_journey_sm["Step 1"] == selected_first_sm]["user_pseudo_id"]))
            if selected_first_camp != "전체":
                valid_users = valid_users.intersection(set(df_journey_camp[df_journey_camp["Step 1"] == selected_first_camp]["user_pseudo_id"]))

            # 라스트터치 필터
            if selected_final_brand != "전체":
                valid_users = valid_users.intersection(set(df_journey[df_journey["final_brand"] == selected_final_brand]["user_pseudo_id"]))
            if selected_final_sm != "전체":
                valid_users = valid_users.intersection(set(df_journey_sm[df_journey_sm["final_source_medium"] == selected_final_sm]["user_pseudo_id"]))
            if selected_final_camp != "전체":
                valid_users = valid_users.intersection(set(df_journey_camp[df_journey_camp["final_campaign"] == selected_final_camp]["user_pseudo_id"]))

            df_f_journey = df_journey[df_journey["user_pseudo_id"].isin(valid_users)].copy()
            df_f_journey_sm = df_journey_sm[df_journey_sm["user_pseudo_id"].isin(valid_users)].copy()
            df_f_journey_camp = df_journey_camp[df_journey_camp["user_pseudo_id"].isin(valid_users)].copy()
            df_f_target = df_target[df_target["user_pseudo_id"].isin(valid_users)].copy()

            if df_f_journey.empty:
                st.warning("선택된 필터 조건에 해당하는 데이터가 없습니다.")
                continue

            # -----------------------------------------------------------------
            # 4. 연동된 KPI 영역
            # -----------------------------------------------------------------
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("분석 대상 유저수", f"{len(df_f_journey):,}")
            ret_rate = (len(df_f_journey[df_f_journey["Step 2"] != "종료/이탈"]) / len(df_f_journey)) if len(df_f_journey) > 0 else 0
            c2.metric("재방문 유저수", f"{len(df_f_journey[df_f_journey['Step 2'] != '종료/이탈']):,}")
            c3.metric("재방문율", f"{ret_rate:.1%}")
            avg_lag = df_f_target[df_f_target["visit_order"] > 1]["days_since_prev"].median()
            c4.metric("리드타임(중간값)", f"{avg_lag:.1f}일" if not np.isnan(avg_lag) else "-")


            # -----------------------------------------------------------------
            # 5. [최상단] 4가지 파이차트 1:1:1:1 배치
            # -----------------------------------------------------------------
            st.markdown('<p class="sub-title">1. 전체 요약 분포</p>', unsafe_allow_html=True)
            
            # 파이차트 공통 레이아웃 설정 (작은 숫자 숨기기, 범례 노출, 시계방향 등)
            def apply_pie_layout(fig):
                fig.update_traces(
                    textposition="auto", 
                    textinfo="percent",
                    insidetextorientation="horizontal",
                    direction="clockwise",
                    sort=False
                )
                fig.update_layout(
                    height=350,  # 전체 차트 높이 고정
                    uniformtext_minsize=11, 
                    uniformtext_mode='hide',
                    showlegend=True,
                    # 범례를 차트 하단 외부로 빼서 크기 간섭 방지
                    legend=dict(
                        orientation="h", 
                        yanchor="top", 
                        y=-0.15, 
                        xanchor="center", 
                        x=0.5
                    ),
                    # 하단 여백을 넉넉히 주어 파이차트 렌더링 크기를 모두 동일하게 강제
                    margin=dict(t=20, b=120, l=10, r=10)
                )
                return fig

            # (1) 라스트터치 브랜드 파이차트
            d_final_dist = df_f_journey["final_brand"].value_counts().reset_index(name="유저수").sort_values("유저수", ascending=False)
            pie_color_map = {b: brand_color_map.get(get_brand_base(b), "#D5DAE5") for b in d_final_dist["final_brand"].unique()}
            fig_pie_1 = px.pie(d_final_dist, values="유저수", names="final_brand", hole=0.4, color="final_brand", color_discrete_map=pie_color_map)
            fig_pie_1 = apply_pie_layout(fig_pie_1)

            # (2) 방문차수 파이차트 (5차 이상은 5차+로 묶음)
            df_vo = df_f_target.copy()
            df_vo["visit_order_grp"] = df_vo["visit_order"].apply(lambda x: "5차+" if x >= 5 else f"{int(x)}차")
            vo_counts = df_vo["visit_order_grp"].value_counts().reset_index(name="세션수").sort_values("세션수", ascending=False)
            fig_pie_2 = px.pie(vo_counts, values="세션수", names="visit_order_grp", hole=0.4)
            fig_pie_2 = apply_pie_layout(fig_pie_2)

            # (3) 퍼스트터치 매체 파이차트 (Top 5)
            fm_counts = df_f_journey_sm[df_f_journey_sm["Step 1"] != "종료/이탈"]["Step 1"].value_counts().nlargest(5).reset_index(name="유저수").sort_values("유저수", ascending=False)
            fig_pie_3 = px.pie(fm_counts, values="유저수", names="Step 1", hole=0.4)
            fig_pie_3 = apply_pie_layout(fig_pie_3)

            # (4) 라스트터치 매체 파이차트 (Top 5)
            final_sm_counts = df_f_journey_sm[df_f_journey_sm["final_source_medium"] != "미확인"]["final_source_medium"].value_counts().nlargest(5).reset_index(name="유저수").sort_values("유저수", ascending=False)
            fig_pie_4 = px.pie(final_sm_counts, values="유저수", names="final_source_medium", hole=0.4)
            fig_pie_4 = apply_pie_layout(fig_pie_4)

            # 4열 배치
            col_p1, col_p2, col_p3, col_p4 = st.columns(4)
            with col_p1:
                st.markdown("<div style='text-align: center; font-weight: 600; font-size: 14px; margin-bottom: 5px;'>라스트터치 브랜드</div>", unsafe_allow_html=True)
                st.plotly_chart(fig_pie_1, use_container_width=True)
            with col_p2:
                st.markdown("<div style='text-align: center; font-weight: 600; font-size: 14px; margin-bottom: 5px;'>방문 차수 분포</div>", unsafe_allow_html=True)
                st.plotly_chart(fig_pie_2, use_container_width=True)
            with col_p3:
                st.markdown("<div style='text-align: center; font-weight: 600; font-size: 14px; margin-bottom: 5px;'>퍼스트터치 매체 (Top 5)</div>", unsafe_allow_html=True)
                st.plotly_chart(fig_pie_3, use_container_width=True)
            with col_p4:
                st.markdown("<div style='text-align: center; font-weight: 600; font-size: 14px; margin-bottom: 5px;'>라스트터치 매체 (Top 5)</div>", unsafe_allow_html=True)
                st.plotly_chart(fig_pie_4, use_container_width=True)


            # -----------------------------------------------------------------
            # 6. 신설된 분석 탭 (브랜드 / 매체 / 캠페인)
            # -----------------------------------------------------------------
            st.markdown('<p class="sub-title">2. 심층 여정 및 리드타임 탐색</p>', unsafe_allow_html=True)
            inner_tabs = st.tabs(["📊 브랜드", "🎯 매체", "🔥 캠페인"])

            # 공통 Aggregation 함수
            agg_funcs = {
                "user_pseudo_id": "count",
                "Lag 2": "mean" if is_mean else "median",
                "Lag 3": "mean" if is_mean else "median",
                "Lag 4": "mean" if is_mean else "median",
                "Lag 5": "mean" if is_mean else "median"
            }

            # [탭 1] 브랜드
            with inner_tabs[0]:
                st.markdown('<p class="section-title">🌊 브랜드 간 이동 흐름 (Macro)</p>', unsafe_allow_html=True)
                fig_sk_brand = generate_sankey(df_f_journey, top_n=None)
                if fig_sk_brand: st.plotly_chart(fig_sk_brand, use_container_width=True)
                else: st.info("조건에 맞는 연쇄 여정 데이터가 없습니다.")

                st.markdown('<p class="section-title">📋 브랜드 5-Step 경로별 리드타임</p>', unsafe_allow_html=True)
                d_table = df_f_journey.groupby("full_path").agg(agg_funcs).reset_index()
                d_table.columns = ["경로", "유저수", "1➔2차(일)", "2➔3차(일)", "3➔4차(일)", "4➔5차(일)"]
                d_table["비중(%)"] = (d_table["유저수"] / len(df_f_journey) * 100).round(1)
                d_table["전체 리드타임"] = d_table.iloc[:, 2:6].sum(axis=1)
                d_table = d_table[["경로", "유저수", "비중(%)", "1➔2차(일)", "2➔3차(일)", "3➔4차(일)", "4➔5차(일)", "전체 리드타임"]].sort_values("유저수", ascending=False).head(50)

                st.dataframe(
                    d_table, use_container_width=True, height=300, hide_index=True,
                    column_config={
                        "비중(%)": st.column_config.NumberColumn(format="%.1f%%"),
                        "전체 리드타임": st.column_config.ProgressColumn(
                            label="전체 리드타임", min_value=0,
                            max_value=float(d_table["전체 리드타임"].max()) if not d_table.empty else 0.0, format="%.1f일"
                        )
                    }
                )

            # [탭 2] 매체
            with inner_tabs[1]:
                st.markdown('<p class="section-title">🌊 소스/매체 경로 이동 흐름 (Top 10)</p>', unsafe_allow_html=True)
                fig_sk_sm = generate_sankey(df_f_journey_sm, top_n=10)
                if fig_sk_sm: st.plotly_chart(fig_sk_sm, use_container_width=True)
                else: st.info("조건에 맞는 연쇄 여정 데이터가 없습니다.")

                st.markdown('<p class="section-title">📋 소스/매체 유입 경로 및 리드타임 (Micro)</p>', unsafe_allow_html=True)
                d_table_sm = df_f_journey_sm.groupby("full_path").agg(agg_funcs).reset_index()
                d_table_sm.columns = ["경로", "유저수", "1➔2차(일)", "2➔3차(일)", "3➔4차(일)", "4➔5차(일)"]
                d_table_sm["비중(%)"] = (d_table_sm["유저수"] / len(df_f_journey_sm) * 100).round(1)
                d_table_sm["전체 리드타임"] = d_table_sm.iloc[:, 2:6].sum(axis=1)
                d_table_sm = d_table_sm[["경로", "유저수", "비중(%)", "1➔2차(일)", "2➔3차(일)", "3➔4차(일)", "4➔5차(일)", "전체 리드타임"]].sort_values("유저수", ascending=False).head(50)

                st.dataframe(
                    d_table_sm, use_container_width=True, height=300, hide_index=True,
                    column_config={
                        "비중(%)": st.column_config.NumberColumn(format="%.1f%%"),
                        "전체 리드타임": st.column_config.ProgressColumn(
                            label="전체 리드타임", min_value=0,
                            max_value=float(d_table_sm["전체 리드타임"].max()) if not d_table_sm.empty else 0.0, format="%.1f일"
                        )
                    }
                )

            # [탭 3] 캠페인
            with inner_tabs[2]:
                st.markdown('<p class="section-title">🌊 캠페인 경로 이동 흐름 (Top 10)</p>', unsafe_allow_html=True)
                fig_sk_camp = generate_sankey(df_f_journey_camp, top_n=10)
                if fig_sk_camp: st.plotly_chart(fig_sk_camp, use_container_width=True)
                else: st.info("조건에 맞는 연쇄 여정 데이터가 없습니다.")

                st.markdown('<p class="section-title">📋 캠페인 유입 경로 및 리드타임 (Action/Deepdive)</p>', unsafe_allow_html=True)
                d_table_camp = df_f_journey_camp.groupby("full_path").agg(agg_funcs).reset_index()
                d_table_camp.columns = ["경로", "유저수", "1➔2차(일)", "2➔3차(일)", "3➔4차(일)", "4➔5차(일)"]
                d_table_camp["비중(%)"] = (d_table_camp["유저수"] / len(df_f_journey_camp) * 100).round(1)
                d_table_camp["전체 리드타임"] = d_table_camp.iloc[:, 2:6].sum(axis=1)
                d_table_camp = d_table_camp[["경로", "유저수", "비중(%)", "1➔2차(일)", "2➔3차(일)", "3➔4차(일)", "4➔5차(일)", "전체 리드타임"]].sort_values("유저수", ascending=False).head(50)

                st.dataframe(
                    d_table_camp, use_container_width=True, height=300, hide_index=True,
                    column_config={
                        "비중(%)": st.column_config.NumberColumn(format="%.1f%%"),
                        "전체 리드타임": st.column_config.ProgressColumn(
                            label="전체 리드타임", min_value=0,
                            max_value=float(d_table_camp["전체 리드타임"].max()) if not d_table_camp.empty else 0.0, format="%.1f일"
                        )
                    }
                )

            # -----------------------------------------------------------------
            # [주석 처리된 코드] 8. 방문차수 및 유입 채널 변화
            # -----------------------------------------------------------------
            # st.markdown('<p class="sub-title">4. 방문차수(Visit Order) 및 유입 채널 변화</p>', unsafe_allow_html=True)
            # c_v1, c_v2 = st.columns(2)
            # 
            # with c_v1:
            #     st.markdown('<p class="section-title">방문차수별 유입 채널 분포</p>', unsafe_allow_html=True)
            #     d_v = df_f_target[df_f_target["visit_order"] <= 5].groupby(["visit_order", "_sourceMedium"]).size().reset_index(name="세션수")
            #     if not d_v.empty:
            #         # top_n_ch 대신 임의의 숫자 10 고정 적용 가능
            #         t_v = d_v.groupby("_sourceMedium")["세션수"].sum().nlargest(10).index
            #         d_v = d_v[d_v["_sourceMedium"].isin(t_v)]
            #         fig_v = px.bar(d_v, x="visit_order", y="세션수", color="_sourceMedium", barmode="stack").update_xaxes(type='category')
            #         render_chart_with_data(fig_v, d_v)
            #     else:
            #         st.info("데이터가 충분하지 않습니다.")
            #         
            # with c_v2:
            #     st.markdown('<p class="section-title">최초 유입경로 대비 현재 재방문 유입경로</p>', unsafe_allow_html=True)
            #     # 재방문 필터 적용된 df_f_ret를 사용
            #     df_f_ret = df_f_target[df_f_target["is_return_user"] == 1].copy()
            #     d_f = df_f_ret.groupby(["first_source_medium", "_sourceMedium"]).size().reset_index(name="세션수")
            #     if not d_f.empty:
            #         t_f = d_f.groupby("first_source_medium")["세션수"].sum().nlargest(5).index
            #         d_f = d_f[d_f["first_source_medium"].isin(t_f)]
            #         fig_f = px.bar(d_f, x="first_source_medium", y="세션수", color="_sourceMedium", barmode="stack")
            #         render_chart_with_data(fig_f, d_f)
            #     else:
            #         st.info("재방문 데이터가 없습니다.")


if __name__ == "__main__":
    main()