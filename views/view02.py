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

def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정 (반드시 최상단)
    # ──────────────────────────────────
    st.set_page_config(layout="wide", page_title="SLPR 대시보드")
    st.markdown(
        """
        <style>
        .reportview-container .main .block-container {
            max-width: 100% !important;
            padding-left: 1rem;
            padding-right: 1rem;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )
    st.subheader("제품 대시보드")
    st.markdown("설명")

    # ──────────────────────────────────
    # 1. 캐시된 데이터 로더
    # ──────────────────────────────────
    @st.cache_data(ttl=3600)
    def load_data(cs: str, ce: str) -> pd.DataFrame:
        bq = BigQuery(
            projectCode="sleeper",
            custom_startDate=cs,
            custom_endDate=ce
        )
        df = bq.get_data("tb_sleeper_product")
        # 최소한의 전처리: 날짜 변환, 파생컬럼 준비
        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d")
        df["_sourceMedium"] = df["traffic_source__source"].astype(str) + " / " + df["traffic_source__medium"].astype(str)
        # isPaid_4 벡터화
        paid_sources   = ['google','naver','meta','meta_adv','mobon','mobion','naver_gfa','DV360','dv360','fb','sns','IGShopping','criteo']
        owned_sources  = ['litt.ly','instagram','l.instagram.com','instagram.com','blog.naver.com','m.blog.naver.com','smartstore.naver.com','m.brand.naver.com']
        earned_sources = ['youtube','youtube.com','m.youtube.com']
        sms_referral   = ['m.facebook.com / referral','l.facebook.com / referral','facebook.com / referral']
        conds = [
            df["_sourceMedium"].isin(['google / organic','naver / organic']),
            df["traffic_source__source"].isin(paid_sources)   | df["_sourceMedium"].isin(['youtube / demand_gen','kakako / crm']),
            df["traffic_source__source"].isin(owned_sources)  | (df["_sourceMedium"]=='kakao / channel_message'),
            df["traffic_source__source"].isin(earned_sources) | df["_sourceMedium"].isin(sms_referral),
        ]
        choices = ['ETC','Paid','Owned','Earned']
        df["isPaid_4"] = np.select(conds, choices, default='ETC')
        return df

    # ──────────────────────────────────
    # 2. 사이드바: 기간 선택 (캐시된 df 에만 적용)
    # ──────────────────────────────────
    st.sidebar.header("Filter")
    today         = datetime.now().date()
    default_end   = today - timedelta(days=1)
    default_start = today - timedelta(days=14)

    start_date, end_date = st.sidebar.date_input(
        "기간 선택",
        value=[default_start, default_end]
    )
    cs = start_date.strftime("%Y%m%d")
    ce = end_date.strftime("%Y%m%d")

    # ──────────────────────────────────
    # 3. 데이터 로딩 & 캐시
    # ──────────────────────────────────
    with st.spinner("데이터를 가져오는 중입니다. 잠시만 기다려주세요."):
        df = load_data(cs, ce)

    # ──────────────────────────────────
    # 4. 사이드바: 추가 필터 (캐시된 df 에만 적용)
    # ──────────────────────────────────
    # (초기화 콜백)
    def reset_filters():
        st.session_state.paid_filter   = "전체"

    # 광고유무 선택
    paid_counts = df["isPaid_4"].value_counts()
    paid_opts   = ["전체"] + paid_counts.index.tolist()
    paid_filter = st.sidebar.selectbox(
        "광고유무 선택",
        paid_opts,
        key="paid_filter"
    )

    # 초기화 버튼 (기간 제외, 나머지 필터만 세션리셋)
    st.sidebar.button(
        "♻️ 필터 초기화",
        on_click=reset_filters
    )
    


    # ──────────────────────────────────
    # 5. 필터 적용
    # ──────────────────────────────────
    df = df[
        (df["event_date"] >= pd.to_datetime(start_date)) &
        (df["event_date"] <= pd.to_datetime(end_date))
    ]
    if st.session_state.paid_filter   != "전체":
        df = df[df["isPaid_4"] == st.session_state.paid_filter]


    ### 메인 영역
    # ──────────────────────────────────
    # 7. 일자별 대분류별 세션 추이 및 표
    # ──────────────────────────────────
    # (1) 일자별 · 대분류별 고유 세션 수 집계 및 피벗
    df_date_cat = (
        df
        .groupby([df["event_date"], "product_cat_a"])["pseudo_session_id"]
        .nunique()
        .reset_index(name="session_count")
    )
    pivot_date_cat = (
        df_date_cat
        .pivot(index="event_date", columns="product_cat_a", values="session_count")
        .fillna(0)
        .reset_index()
        .rename(columns={"event_date": "날짜"})
    )
    # (2) x축용 날짜 문자열
    pivot_date_cat["날짜_표시"] = pivot_date_cat["날짜"].dt.strftime("%m월 %d일")

    st.divider()
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>브랜드별</span> 추이</h5>", unsafe_allow_html=True)      
    _col1, _col2 = st.columns([1, 25])
    with _col1:
        # badge() 자체를 호출만 하고, 반환값을 쓰지 마세요
        st.badge("Info", icon=":material/star:", color="green")
    with _col2:
        st.markdown("**슬립퍼** 및 **누어** 조회 현황을 일자별로 확인할 수 있습니다.")

    # 레이아웃: 3분할 (차트 · 공백 · 표)
    col1, col2, col3 = st.columns([6.0, 0.2, 3.8])

    with col1:
        
        
        # (1) 차트용 y축 컬럼
        y_cols = [c for c in pivot_date_cat.columns if c not in ["날짜","날짜_표시"]]

        # ── datetime 축으로 다시 그리기 ──
        fig2 = px.line(
            pivot_date_cat,
            x="날짜",               # 포맷된 문자열 대신 원본 datetime 사용
            y=y_cols,
            markers=True,
            labels={"variable": ""}
        )
        # ── 주말(토·일) 영역 강조 (±12시간) ──
        for d in pivot_date_cat["날짜"]:
            # 절반일(12시간) 오프셋 계산
            start = d - timedelta(hours=12)
            end   = d + timedelta(hours=12)

            if d.weekday() == 5:  # 토요일
                fig2.add_vrect(
                    x0=start,
                    x1=end,
                    fillcolor="blue",
                    opacity=0.2,
                    layer="below",
                    line_width=0
                )
            elif d.weekday() == 6:  # 일요일
                fig2.add_vrect(
                    x0=start,
                    x1=end,
                    fillcolor="red",
                    opacity=0.2,
                    layer="below",
                    line_width=0
                )

        # ── x축 레이블 한글 포맷 유지 ──
        fig2.update_xaxes(
            tickvals=pivot_date_cat["날짜"],
            ticktext=pivot_date_cat["날짜_표시"]
        )
        fig2.update_layout(
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
        st.plotly_chart(fig2, use_container_width=True)
        
    with col2:
        pass

    with col3:
        st.markdown(" ")
        # (3) 테이블용 날짜 포맷
        df_display = pivot_date_cat.copy()
        df_display["날짜"] = df_display["날짜"].dt.strftime("%m월 %d일 (%a)")
        cols = [c for c in df_display.columns if c != "날짜_표시"]
        df_grid = df_display[["날짜"] + [c for c in cols if c != "날짜"]]
        # (4) 합계 행
        bottom = {
            col: ("합계" if col == "날짜" else int(df_grid[col].sum()))
            for col in df_grid.columns
        }

        # (5) AgGrid 설정
        gb2 = GridOptionsBuilder.from_dataframe(df_grid)
        gb2.configure_default_column(flex=1, sortable=True, filter=True)
        for col in df_grid.columns:
            if col != "날짜":
                gb2.configure_column(
                    col,
                    type=["numericColumn","customNumericFormat"],
                    valueFormatter="x.toLocaleString()"
                )
        gb2.configure_grid_options(pinnedBottomRowData=[bottom])
        gb2.configure_grid_options(
            onGridReady=JsCode("""
                function(params) {
                    params.api.sizeColumnsToFit();
                }
            """)
        )
        grid_opts2 = gb2.build()

        # (6) 테마 선택 및 렌더링
        ag_theme = "streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit"
        AgGrid(
            df_grid,
            gridOptions=grid_opts2,
            height=380,
            theme=ag_theme,
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=True
        )


    
    # ──────────────────────────────────
    # 6. 대·중분류별 일자별 세션 집계 (for radio + 차트)
    # ──────────────────────────────────
    st.divider()
    st.markdown(f"<h5 style='margin:0'><h5 style='margin:0'><span style='color:#FF4B4B;'>카테고리별</span> 추이</h5></h5>", unsafe_allow_html=True)

    col1, col2 = st.columns([1, 25])
    with col1:
        # badge() 자체를 호출만 하고, 반환값을 쓰지 마세요
        st.badge("Info", icon=":material/star:", color="green")
    with col2:
        st.markdown("**프레임** 및 **매트리스** 조회 현황을 일자별로 확인할 수 있습니다.")
        st.markdown("")
    
    # (1) 대·중분류별 일자별 고유 세션 수 집계
    df_date_cat_ab = (
        df
        .groupby([df["event_date"], "product_cat_a", "product_cat_b"])["pseudo_session_id"]
        .nunique()
        .reset_index(name="session_count")
    )
    # (2) 피벗: index=event_date, product_cat_a; columns=product_cat_b
    pivot_ab = (
        df_date_cat_ab
        .pivot_table(
            index=["event_date", "product_cat_a"],
            columns="product_cat_b",
            values="session_count",
            fill_value=0
        )
        .reset_index()
        .rename(columns={"event_date": "날짜"})
    )
    pivot_ab["날짜_표시"] = pivot_ab["날짜"].dt.strftime("%m월 %d일")


    # ──────────────────────────────────
    # 7. 라디오 버튼 + 그래프 & 테이블 (날짜별 중분류)
    # ──────────────────────────────────
    # (1) 대·중분류별 일자별 세션 집계 & 피벗 (앞서 정의된 df_date_cat_ab, pivot_ab 이용)
    cat_a_options = pivot_ab["product_cat_a"].unique().tolist()
    # — 토글 순서 항상 "슬립퍼"를 첫번째로 고정 —
    if "슬립퍼" in cat_a_options:
        cat_a_options.remove("슬립퍼")
        cat_a_options.insert(0, "슬립퍼")
    
    # selected_cat_a = st.radio("", cat_a_options, index=0, horizontal=True)
    selected_cat_a = st.pills(
        "브랜드 선택",
        cat_a_options,
        selection_mode="single",
        default=cat_a_options[0]
    )


    
    # — 누적막대그래프에서 라디오 버튼에서 선택한 대분류대로 중분류가 나와야 함 —
    df_temp = pivot_ab[pivot_ab["product_cat_a"] == selected_cat_a].copy()

    # 레이아웃: 3분할 (차트 · 공백 · 표)
    col1, col2, col3 = st.columns([6.0, 0.2, 3.8])

    with col1:
        fig = go.Figure()
        # — 중분류만 추출: 선택된 cat_a의 값이 0 이상인 컬럼만 표시 —
        mid_cats = [
            c for c in df_temp.columns
            if c not in ["날짜", "product_cat_a", "날짜_표시"] and df_temp[c].sum() > 0
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
            df_date_cat_ab[df_date_cat_ab["product_cat_a"] == selected_cat_a]
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
                ticktext=df_temp["날짜_표시"],
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
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        pass

    with col3:
        # 테이블용 데이터 준비: 오직 선택된 cat_a의 중분류만 포함
        mid_cats = [
            c for c in df_temp.columns
            if c not in ["날짜", "product_cat_a", "날짜_표시"] and df_temp[c].sum() > 0
        ]
        df_table = df_temp[["날짜"] + mid_cats].copy()

        df_table["날짜"] = df_table["날짜"].dt.strftime("%m월 %d일 (%a)")

        # 표의 컬럼 순서를 합산(sum)이 큰 순서대로 배치 (날짜 고정)
        col_sums = {col: df_table[col].sum() for col in mid_cats}
        sorted_cols = sorted(col_sums, key=lambda c: col_sums[c], reverse=True)
        df_table = df_table[["날짜"] + sorted_cols]

        # 합계행 계산
        bottom = {"날짜": "합계"}
        for col in sorted_cols:
            bottom[col] = int(df_table[col].sum())

        # AgGrid 설정 (기존 스타일 유지)
        gb = GridOptionsBuilder.from_dataframe(df_table)
        gb.configure_default_column(flex=1, sortable=True, filter=True)
        for col in df_table.columns:
            if col != "날짜":
                gb.configure_column(
                    col,
                    type=["numericColumn","customNumericFormat"],
                    valueFormatter="x.toLocaleString()"
                )
        gb.configure_grid_options(pinnedBottomRowData=[bottom])
        gb.configure_grid_options(onGridReady=JsCode("""
            function(params) {
                params.api.sizeColumnsToFit();
            }
        """))
        grid_opts = gb.build()
        theme = "streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit"
        AgGrid(
            df_table,
            gridOptions=grid_opts,
            height=350,
            theme=theme,
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=True
        )


    st.divider()
    st.markdown(f"<h5 style='margin:0'><h5 style='margin:0'><span style='color:#FF4B4B;'>제품별</span> 추이</h5></h5>", unsafe_allow_html=True)

    _col1, _col2 = st.columns([1, 25])
    with _col1:
        # badge() 자체를 호출만 하고, 반환값을 쓰지 마세요
        st.badge("Info", icon=":material/star:", color="green")
    with _col2:
        st.markdown("**제품별** 조회 현황을 일자별로 확인하고, **유입경로**별로 세분화할 수 있습니다.")
        st.markdown("")


    # ──────────────────────────────────
    # 8. 동적 피벗 테이블 + 대·중분류 Pills 필터 + 행합계 정렬
    # ──────────────────────────────────
    pills01, pills02 = st.columns([2,3])  

    # (0) 대분류 선택 (멀티 Pills)
    with pills01:
        cat_a_opts = sorted([x for x in df["product_cat_a"].unique() if x is not None])
        sel_cat_a = st.pills(
            "브랜드 선택",
            cat_a_opts,
            selection_mode="multi",
            default=cat_a_opts
        ) or cat_a_opts
        df_a = df[df["product_cat_a"].isin(sel_cat_a)]

    # (1) 중분류 선택 (대분류에 따라 동적 변경)
    with pills02:
        cat_b_opts = sorted([x for x in df_a["product_cat_b"].unique() if x is not None])
        sel_cat_b = st.pills(
            "카테고리 선택",
            cat_b_opts,
            selection_mode="multi",
            default=cat_b_opts
        ) or cat_b_opts
        df_ab = df_a[df_a["product_cat_b"].isin(sel_cat_b)]


    # (4) 피벗 기준 컬럼 선택
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


    # (2) 제품 · 세션 소스·매체·캠페인 필터 (빈 선택은 전체로 간주)
    col_prod, col_src, col_med, col_name = st.columns([2,1,1,1])

    # 제품 필터: 등장 빈도 내림차순
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

    # 세션 소스 필터: 등장 빈도 내림차순
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

    # 세션 매체 필터: 등장 빈도 내림차순
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

    # 세션 캠페인 필터: 등장 빈도 내림차순
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

    # (3) 최종 필터링
    df_ab = df_ab[
        df_ab["product_name"].isin(sel_prod) &
        df_ab["traffic_source__source"].isin(sel_src) &
        df_ab["traffic_source__medium"].isin(sel_med) &
        df_ab["traffic_source__name"].isin(sel_name)
    ]


    # (5) 날짜·그룹별 고유 세션수 집계
    df_tmp = (
        df_ab
        .groupby(["event_date"] + group_dims)["pseudo_session_id"]
        .nunique()
        .reset_index(name="유입수")
    )

    # (6) 피벗: index=그룹, columns=날짜
    pivot = df_tmp.pivot_table(
        index=group_dims,
        columns="event_date",
        values="유입수",
        fill_value=0
    ).reset_index()

    # (7) 날짜 컬럼 포맷
    date_cols = [c for c in pivot.columns if isinstance(c, pd.Timestamp)]
    rename_map = {c: c.strftime("%m월 %d일") for c in date_cols}
    pivot.rename(columns=rename_map, inplace=True)
    date_cols = list(rename_map.values())

    # (8) 기준 컬럼명(영문) → 표시명(한글) 변경
    inv_options = {v: k for k, v in options.items()}
    pivot.rename(columns=inv_options, inplace=True)

    # (9) 행합계·열합계 추가 및 행합계 내림차순 정렬
    pivot["합계"] = pivot[date_cols].sum(axis=1)
    pivot = pivot.sort_values("합계", ascending=False).reset_index(drop=True)

    bottom = {col: "합계" for col in inv_options.values() if col in pivot.columns}
    for col in date_cols + ["합계"]:
        bottom[col] = int(pivot[col].sum())
    pinned_bottom = [bottom]

    # (10) AgGrid 설정 & 출력
    gb = GridOptionsBuilder.from_dataframe(pivot)
    gb.configure_default_column(flex=1, sortable=True, filter=True)
    for col in date_cols + ["합계"]:
        gb.configure_column(
            col, 
            type=["numericColumn","customNumericFormat"], 
            valueFormatter="x.toLocaleString()"
        )
    gb.configure_column("합계", pinned="right")
    gb.configure_grid_options(pinnedBottomRowData=pinned_bottom)
    gb.configure_grid_options(onGridReady=JsCode("""
        function(params) {
            params.api.sizeColumnsToFit();
        }
    """))
    grid_opts = gb.build()

    theme = "streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit"
    AgGrid(
        pivot,
        gridOptions=grid_opts,
        height=450,
        theme=theme,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True
)



    st.divider()
    st.markdown(f"<h5 style='margin:0'><h5 style='margin:0'><span style='color:#FF4B4B;'>제품별</span> 유입경로</h5></h5>", unsafe_allow_html=True)
    _col1, _col2 = st.columns([1, 25])
    with _col1:
        # badge() 자체를 호출만 하고, 반환값을 쓰지 마세요
        st.badge("Info", icon=":material/star:", color="green")
    with _col2:
        st.markdown("설명")
        st.markdown("")

    # ──────────────────────────────────
    # 9. 선택 제품별 일자별 유입 추이 (소스 / 매체 결합)
    # ──────────────────────────────────
    # (1) 제품 토글 (8번 영역과 비연동)
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
        df_tmp = (
            df_fs
            .groupby(["date_str", "source_medium"])["pseudo_session_id"]
            .nunique()
            .reset_index(name="sessions")
        )

        # (6) 누적막대 그래프 그리기 (opacity 조정 추가)
        fig9 = px.bar(
            df_tmp,
            x="date_str",
            y="sessions",
            color="source_medium",
            barmode="stack",
            labels={
                "date_str": "",
                "sessions": "유입수",
                "source_medium": "소스 / 매체"
            }
        )
        # ── 막대 투명도 조정 (다른 그래프와 비슷한 톤) ──
        fig9.update_traces(marker_opacity=0.6)
        
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

        st.plotly_chart(fig9, use_container_width=True)
