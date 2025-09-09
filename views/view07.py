# 서희_최신수정일_25-08-20

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import importlib
from datetime import datetime, timedelta
import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import JsCode
import plotly.graph_objects as go
import sys
import modules.style
importlib.reload(sys.modules['modules.style'])
from modules.style import style_format, style_cmap

def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정 (반드시 최상단)
    # ──────────────────────────────────

    st.markdown(
        """
        <style>
            /* 전체 컨테이너의 패딩 조정 */
            .block-container {
                max-width: 100% !important;
                padding-top: 4rem;   /* 위쪽 여백 */
                padding-bottom: 8rem;
                padding-left: 5rem; 
                padding-right: 4rem; 
            }
        </style>
        """,
        unsafe_allow_html=True
    )  

    st.subheader('GA PDP 대시보드')
    st.markdown(
        """
        <div style="
            color:#6c757d;        
            font-size:14px;       
            line-height:1.5;      
        ">
        이 대시보드에서는 <b>브랜드/카테고리/제품</b> 단위의 
        <b>제품 상세 페이지(PDP) 조회량</b>을 확인할 수 있습니다.<br>
        해당 대시보드는 view_item 이벤트를 발생시킨 세션 데이터를 기반으로 구성되어 있습니다.
        </div>
        """,
        unsafe_allow_html=True
    )

    st.divider()


    # ──────────────────────────────────
    # 사이드바 필터 설정
    # ──────────────────────────────────
    st.sidebar.header("Filter")
    today         = datetime.now().date()
    default_end   = today - timedelta(days=1)
    default_start = today - timedelta(days=7)

    start_date, end_date = st.sidebar.date_input(
        "기간 선택",
        value=[default_start, default_end]
    )
    cs = start_date.strftime("%Y%m%d")
    ce = end_date.strftime("%Y%m%d")
    
    @st.cache_data(ttl=3600)
    def load_data(cs: str, ce: str) -> pd.DataFrame:
        
        # tb_sleeper_product
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df = bq.get_data("tb_sleeper_product")
        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d")

        def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
            df["_sourceMedium"] = (df["traffic_source__source"].astype(str) + " / " + df["traffic_source__medium"].astype(str))
            # df["isPaid_4"]    = categorize_paid(df)

            return df
        
        # def categorize_paid(df: pd.DataFrame) -> pd.Series:
        #     paid_sources = ['google','naver','meta','meta_adv','mobon','mobion','naver_gfa','DV360','dv360','fb','sns','IGShopping','criteo']
        #     owned_sources = ['litt.ly','instagram','l.instagram.com','instagram.com','blog.naver.com','m.blog.naver.com','smartstore.naver.com','m.brand.naver.com']
        #     earned_sources = ['youtube','youtube.com','m.youtube.com']
        #     sms_referral = ['m.facebook.com / referral','l.facebook.com / referral','facebook.com / referral']
        #     conds = [
        #         # Organic
        #         df["_sourceMedium"].isin(['google / organic','naver / organic']),
        #         # Paid (exclude sponsored)
        #         (df["collected_traffic_source__manual_source"].isin(paid_sources) & ~df["_sourceMedium"].eq('google / sponsored'))
        #             | df["_sourceMedium"].isin(['youtube / demand_gen','kakako / crm']),
        #         # Owned
        #         df["collected_traffic_source__manual_source"].isin(owned_sources)
        #             | (df["_sourceMedium"] == 'kakao / channel_message'),
        #         # Earned (include sponsored)
        #         df["collected_traffic_source__manual_source"].isin(earned_sources)
        #             | df["_sourceMedium"].isin(sms_referral + ['google / sponsored'])
        #     ]
        #     choices = ['ETC','Paid','Owned','Earned']
        #     return np.select(conds, choices, default='ETC')
        
        
        return preprocess_data(df)


    # ──────────────────────────────────
    # 데이터 불러오기
    # ──────────────────────────────────
    st.toast("GA D-1 데이터는 오전에 예비 처리되고, **15시 이후에 최종 업데이트** 됩니다.", icon="🔔")
    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df = load_data(cs, ce)

    # ──────────────────────────────────
    # 공통 함수
    # ──────────────────────────────────
    def render_line_chart(
        df: pd.DataFrame,
        x: str,
        y: list[str] | str,
        height: int = 400,
        title: str | None = None,
        ) -> None:
        
        # y가 단일 문자열이면 리스트로 감싸기
        y_cols = [y] if isinstance(y, str) else y
        
        fig = px.line(
            df,
            x=x,
            y=y_cols,
            markers=True,
            labels={"variable": ""},
            title=title
        )
        # ────────────────────────────────────
        # 주말 영역 표시 추가
        from datetime import timedelta
        for d in pd.to_datetime(df[x]).dt.date.unique():
            start = datetime.combine(d, datetime.min.time()) + timedelta(hours=12)
            end   = start + timedelta(hours=24)
            if d.weekday() == 4:   # 토요일
                fig.add_vrect(x0=start, x1=end, fillcolor="blue",  opacity=0.05, layer="below", line_width=0)
            elif d.weekday() == 5: # 일요일
                fig.add_vrect(x0=start, x1=end, fillcolor="red",   opacity=0.05, layer="below", line_width=0)
        # ────────────────────────────────────

        fig.update_layout(
            height=height,
            xaxis_title=None,
            yaxis_title=None,
            legend=dict(
                orientation="h",
                y=1.02,
                x=1,
                xanchor="right",
                yanchor="bottom"
            )
        )
        fig.update_xaxes(tickformat="%m월 %d일")
        st.plotly_chart(fig, use_container_width=True)

    def summary_row(df):
        # 숫자형 컬럼만 자동 추출
        num_cols = df.select_dtypes(include="number").columns
        sum_row = df[num_cols].sum().to_frame().T
        sum_row['날짜'] = "합계"
        mean_row = df[num_cols].mean().to_frame().T
        mean_row['날짜'] = "평균"
        df = pd.concat([df, sum_row, mean_row], ignore_index=True)

        return df  

    # 데이터프레임 생성
    # ──────────────────────────────────
    # 1) 브랜드별 추이 (df_brand)
    _df_brand = (
        df
        .groupby([df["event_date"], "product_cat_a"])["pseudo_session_id"]
        .nunique()
        .reset_index(name="session_count")
    )
    df_brand = (
        _df_brand
        .pivot(index="event_date", columns="product_cat_a", values="session_count")
        .fillna(0)
        .reset_index()
        .rename(columns={"event_date": "날짜"})
    )
    df_brand["날짜"] = df_brand["날짜"].dt.strftime("%Y-%m-%d")


    # 2) 카테고리별 추이
    _df_category = (
        df
        .groupby([df["event_date"], "product_cat_a", "product_cat_b"])["pseudo_session_id"]
        .nunique()
        .reset_index(name="session_count")
    )
    df_category = (
        _df_category
        .pivot_table(
            index=["event_date", "product_cat_a"],
            columns="product_cat_b",
            values="session_count",
            fill_value=0
        )
        .reset_index()
        .rename(columns={"event_date": "날짜"})
    )
    df_category["날짜"] = df_category["날짜"].dt.strftime("%Y-%m-%d")


    # ──────────────────────────────────
    # 1) 브랜드별 추이
    # ──────────────────────────────────
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>브랜드별</span> 추이</h5>", unsafe_allow_html=True)      
    st.markdown(":gray-badge[:material/Info: Info]ㅤ**슬립퍼** 및 **누어** 조회 현황을 일자별로 확인할 수 있습니다.")

    # — 시각화
    col1, _p, col2 = st.columns([6.0, 0.2, 3.8])
    with col1:
        y_cols = [c for c in df_brand.columns if c not in "날짜"]
        render_line_chart(df_brand, x="날짜", y=y_cols)
    with _p: pass
    with col2:
        styled = style_format(
            summary_row(df_brand),
            decimals_map={
                ("누어"): 0,
                ("슬립퍼"): 0,
            },
        )
        # styled2 = style_cmap(
        #     styled,
        #     gradient_rules=[
        #         {"col": "슬립퍼", "cmap":"Purples", "low":0.0, "high":0.3},
        #     ]
        # )
        st.dataframe(styled, row_height=30,  hide_index=True)
    
    # ──────────────────────────────────
    # 2) 카테고리별 추이
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown(f"<h5 style='margin:0'><h5 style='margin:0'><span style='color:#FF4B4B;'>카테고리별</span> 추이</h5></h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ**프레임** 및 **매트리스** 조회 현황을 일자별로 확인할 수 있습니다.")
    
    # — 라디오 버튼
    cat_a_options = df_category["product_cat_a"].unique().tolist()
    if "슬립퍼" in cat_a_options: # 항상 "슬립퍼"를 첫번째로 고정
        cat_a_options.remove("슬립퍼")
        cat_a_options.insert(0, "슬립퍼")
    
    selected_cat_a = st.pills(
        "브랜드 선택",
        cat_a_options,
        selection_mode="single",
        default=cat_a_options[0]
    )
    df_temp = df_category[df_category["product_cat_a"] == selected_cat_a].copy()

    # — 시각화
    col1, _p, col2 = st.columns([6.0, 0.2, 3.8])

    with col1:
        fig = go.Figure()
        # — 중분류만 추출: 선택된 cat_a의 값이 0 이상인 컬럼만 표시 —
        mid_cats = [
            c for c in df_temp.columns
            if c not in ["날짜", "product_cat_a"] and df_temp[c].sum() > 0
        ]
        for cat_b in mid_cats:
            fig.add_trace(go.Bar(
                x=df_temp["날짜"],
                y=df_temp[cat_b],
                name=cat_b,
                offsetgroup=selected_cat_a,
                marker_opacity=0.6
            ))

        # 대분류 라인
        df_line = (
            _df_category[_df_category["product_cat_a"] == selected_cat_a]
            .groupby("event_date")["session_count"]
            .sum()
            .reset_index()
        )
        fig.add_trace(go.Scatter(
            x=df_line["event_date"],
            y=df_line["session_count"],
            mode="lines+markers",
            name=selected_cat_a,
            line=dict(width=2),
            marker=dict(size=6)
        ))
        fig.update_layout(
            barmode="stack",
            bargap=0.5, bargroupgap=0.1,
            height=360,
            margin=dict(l=10, r=10, t=60, b=30),
            xaxis=dict(
                tickvals=df_temp["날짜"],
                ticktext=df_temp["날짜"],
                showgrid=False,
                title=None
            ),
            yaxis=dict(showgrid=False, title=None),
            legend=dict(
                orientation="h",
                y=1.02,
                x=1,
                xanchor="right",
                yanchor="bottom",
                title=None
            )
        )
        fig.update_xaxes(tickformat="%m월 %d일")
        st.plotly_chart(fig, use_container_width=True)

    with _p: pass

    with col2:
        mid_cats = [
            c for c in df_temp.columns
            if c not in ["날짜", "product_cat_a"] and df_temp[c].sum() > 0
        ]
        df_table = df_temp[["날짜"] + mid_cats].copy()
        col_sums = {col: df_table[col].sum() for col in mid_cats}
        sorted_cols = sorted(col_sums, key=lambda c: col_sums[c], reverse=True)
        df_table = df_table[["날짜"] + sorted_cols]

        styled = style_format(
            summary_row(df_table),
            decimals_map={
                ("패브릭 침대"): 0,
                ("매트리스"): 0,
                ("원목 침대"): 0,
                ("기타"): 0,
                ("프레임"): 0,
            },
        )
        # styled2 = style_cmap(
        #     styled,
        #     gradient_rules=[
        #         {"col": "매트리스", "cmap":"Purples", "low":0.0, "high":0.3},
        #     ]
        # )
        st.dataframe(styled, row_height=30,  hide_index=True)
        
        
        
    # ──────────────────────────────────
    # 3) 제품별 추이
    # ──────────────────────────────────
    st.header(" ")
    st.markdown(f"<h5 style='margin:0'><h5 style='margin:0'><span style='color:#FF4B4B;'>제품별</span> 추이</h5></h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ**제품별** 조회 현황을 일자별로 확인하고, 선택한 행 필드를 기준으로 지표들을 피벗하여 조회할 수 있습니다.") #

    # ---------- 필터 영역 ----------
    # Pills 필터
    pills01, pills02 = st.columns([2,3])  
    with pills01: # Pills 필터 - 브랜드 선택
        cat_a_opts = sorted([x for x in df["product_cat_a"].unique() if x is not None])
        sel_cat_a = st.pills(
            "브랜드 선택",
            cat_a_opts,
            selection_mode="multi",
            default=cat_a_opts
        ) or cat_a_opts
        df_a = df[df["product_cat_a"].isin(sel_cat_a)]

    with pills02: # Pills 필터 - 카테고리 선택
        cat_b_opts = sorted([x for x in df_a["product_cat_b"].unique() if x is not None])
        sel_cat_b = st.pills(
            "카테고리 선택",
            cat_b_opts,
            selection_mode="multi",
            default=cat_b_opts
        ) or cat_b_opts
        df_ab = df_a[df_a["product_cat_b"].isin(sel_cat_b)]

    # 행필드 선택
    options = {
        "제품":             "product_name",
        "세션 소스":         "traffic_source__source",
        "세션 매체":         "traffic_source__medium",
        "세션 캠페인":       "traffic_source__name"
    }
    sel = st.multiselect(
        "행 필드 선택",
        options=list(options.keys()),
        default=["제품"]
    )
    group_dims = [options[k] for k in sel]

    # 선택 필터
    col_prod, col_src, col_med, col_name = st.columns([2,1,1,1])

    prod_opts = df_ab["product_name"].value_counts().index.tolist()
    with col_prod:
        sel_prod = st.multiselect(
            "제품 선택",
            options=prod_opts,
            default=[],
            placeholder="전체"
        )
        if not sel_prod:
            sel_prod = prod_opts

    src_opts = df_ab["traffic_source__source"].value_counts().index.tolist()
    with col_src:
        sel_src = st.multiselect(
            "세션 소스 선택",
            options=src_opts,
            default=[],
            placeholder="전체"
        )
        if not sel_src:
            sel_src = src_opts

    med_opts = df_ab["traffic_source__medium"].value_counts().index.tolist()
    with col_med:
        sel_med = st.multiselect(
            "세션 매체 선택",
            options=med_opts,
            default=[],
            placeholder="전체"
        )
        if not sel_med:
            sel_med = med_opts

    name_opts = df_ab["traffic_source__name"].value_counts().index.tolist()
    with col_name:
        sel_name = st.multiselect(
            "세션 캠페인 선택",
            options=name_opts,
            default=[],
            placeholder="전체"
        )
        if not sel_name:
            sel_name = name_opts

    # 최종 필터링
    df_ab = df_ab[
        df_ab["product_name"].isin(sel_prod) &
        df_ab["traffic_source__source"].isin(sel_src) &
        df_ab["traffic_source__medium"].isin(sel_med) &
        df_ab["traffic_source__name"].isin(sel_name)
    ]

    # 데이터프레임 생성
    _df_product = (
        df_ab
        .groupby(["event_date"] + group_dims)["pseudo_session_id"]
        .nunique()
        .reset_index(name="유입수")
    )
    ## 여기서 event_date 형식 바꿔줌 -> 나중에 멜팅되므로.
    _df_product["event_date"] = _df_product["event_date"].dt.strftime("%Y-%m-%d")
    
    df_product = _df_product.pivot_table(
        index=group_dims,
        columns="event_date",
        values="유입수",
        fill_value=0
    ).reset_index()

    ## 컬럼명 -> 한글 매핑
    inv_options = {v: k for k, v in options.items()}
    df_product.rename(columns=inv_options, inplace=True)
    st.dataframe(df_product, height=500, hide_index=True)



    # ──────────────────────────────────
    # 4) 제품별 유입경로
    # ──────────────────────────────────
    st.header(" ")
    st.markdown(f"<h5 style='margin:0'><h5 style='margin:0'><span style='color:#FF4B4B;'>제품별</span> 유입경로</h5></h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ특정 제품의 조회수가 특정 일자에 증가했다면, 해당 유입이 **어떤 경로를 통해 발생했는지** 확인할 수 있습니다.")
    
    # 선택 필터
    prod_opts = sorted([p for p in df["product_name"].unique() if p is not None])
    default_prod = "튤리아"
    
    sel_prods = st.multiselect(
        "제품 선택",
        prod_opts,
        default=[default_prod] if default_prod in prod_opts else []
    )
    
    if not sel_prods:
        st.warning("제품을 하나 이상 선택하세요.")
    else:
        # (2) 선택 제품으로만 필터
        df_fs = df[df["product_name"].isin(sel_prods)].copy()
        df_fs["date_str"] = df_fs["event_date"].dt.strftime("%m월 %d일")

        # (3) source / medium 결합 컬럼 추가
        df_fs["source_medium"] = (
            df_fs["traffic_source__source"] + " / " + df_fs["traffic_source__medium"]
        )

        # (4) 상위 7개 source_medium 추출, 나머지는 '기타'
        top7 = (
            df_fs["source_medium"]
            .value_counts()
            .nlargest(7)
            .index
            .tolist()
        )
        df_fs["source_medium"] = df_fs["source_medium"].where(
            df_fs["source_medium"].isin(top7), "기타"
        )

        # (5) 일자·source_medium별 고유 세션 수 집계
        _df_product = (
            df_fs
            .groupby(["date_str", "source_medium"])["pseudo_session_id"]
            .nunique()
            .reset_index(name="sessions")
        )

        # (6) 누적막대 그래프 그리기 (opacity 조정 추가)
        fig9 = px.bar(
            _df_product,
            x="date_str",
            y="sessions",
            color="source_medium",
            barmode="relative",
            labels={
                "date_str": "",
                "sessions": "유입수",
                "source_medium": "소스 / 매체"
            }
        )
        fig9.update_traces(marker_opacity=0.6)
        # 핵심: 진짜로 누적시키기


        fig9.update_layout(
            legend_title_text="",
            xaxis_title=None,
            yaxis_title=None,
            bargap=0.5, bargroupgap=0.2,
            legend=dict(
                orientation="h",
                y=1.02,
                x=1,
                xanchor="right",
                yanchor="bottom"
            ),
            margin=dict(l=10, r=10, t=30, b=10)
        )
        fig9.update_layout(barmode="relative")
        fig9.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))
        st.plotly_chart(fig9, use_container_width=True)
