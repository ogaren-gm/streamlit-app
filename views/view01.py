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
    st.subheader('유입 대시보드')
    st.divider()


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
        df = bq.get_data("tb_sleeper_psi")
        # 최소한의 전처리: 날짜 변환, 파생컬럼 준비
        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y%m%d")
        df["_sourceMedium"] = df["traffic_source__source"].astype(str) + " / " + df["traffic_source__medium"].astype(str)
        df["_isUserNew_y"] = (df["first_visit"] == 1).astype(int)
        df["_isUserNew_n"] = (df["first_visit"] == 0).astype(int)
        df["_engagement_time_sec_sum"] = df["engagement_time_msec_sum"] / 1000
        # 이벤트별 세션 플래그
        events = [
            ("_find_nearby_showroom_sessionCnt", "find_nearby_showroom"),
            ("_product_option_price_sessionCnt", "product_option_price"),
            ("_view_item_sessionCnt", "view_item"),
            ("_add_to_cart_sessionCnt", "add_to_cart"),
            ("_purchase_sessionCnt", "purchase"),
            ("_showroom_10s_sessionCnt", "showroom_10s"),
            ("_product_page_scroll_50_sessionCnt", "product_page_scroll_50"),
            ("_showroom_leads_sessionCnt", "showroom_leads")
        ]
        for nc, sc in events:
            df[nc] = (df[sc] > 0).astype(int)
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
    with st.spinner("데이터 로딩 중…"):
        df = load_data(cs, ce)

    # ──────────────────────────────────
    # 4. 사이드바: 추가 필터 (캐시된 df 에만 적용)
    # ──────────────────────────────────
    # (초기화 콜백)
    def reset_filters():
        st.session_state.paid_filter   = "전체"
        st.session_state.medium_filter = "전체"
        st.session_state.device_filter = "전체"
        st.session_state.geo_filter    = "전체"

    # 광고유무 선택
    paid_counts = df["isPaid_4"].value_counts()
    paid_opts   = ["전체"] + paid_counts.index.tolist()
    paid_filter = st.sidebar.selectbox(
        "광고유무 선택",
        paid_opts,
        key="paid_filter"
    )

    # 소스/매체 선택
    medium_counts = df["_sourceMedium"].value_counts()
    medium_opts   = ["전체"] + medium_counts.index.tolist()
    medium_filter = st.sidebar.selectbox(
        "소스/매체 선택",
        medium_opts,
        key="medium_filter"
    )

    # 디바이스 선택
    device_counts = df["device__category"].value_counts()
    device_opts   = ["전체"] + device_counts.index.tolist()
    device_filter = st.sidebar.selectbox(
        "디바이스 선택",
        device_opts,
        key="device_filter"
    )

    # 접속지역 선택
    geo_counts = df["geo__city"].value_counts()
    geo_opts   = ["전체"] + geo_counts.index.tolist()
    geo_filter = st.sidebar.selectbox(
        "접속지역 선택",
        geo_opts,
        key="geo_filter"
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
    if st.session_state.medium_filter != "전체":
        df = df[df["_sourceMedium"] == st.session_state.medium_filter]
    if st.session_state.device_filter != "전체":
        df = df[df["device__category"] == st.session_state.device_filter]
    if st.session_state.geo_filter    != "전체":
        df = df[df["geo__city"] == st.session_state.geo_filter]


    ### 메인 영역
    # ──────────────────────────────────
    # 6. (1) 유입 추이
    # ──────────────────────────────────
    df_daily = (
        df.groupby("event_date")[["pseudo_session_id", "user_pseudo_id", "_isUserNew_y","_isUserNew_n"]]
          .agg({"pseudo_session_id":"nunique","user_pseudo_id":"nunique",
                "_isUserNew_y":"sum","_isUserNew_n":"sum"})
          .reset_index()
          .rename(columns={
              "event_date":"날짜",
              "pseudo_session_id":"방문수",
              "user_pseudo_id":"유저수",
              "_isUserNew_y":"신규방문수",
              "_isUserNew_n":"재방문수"
          })
    )
    df_daily["날짜_표시"] = df_daily["날짜"].dt.strftime("%m월 %d일") # x축 한글 포맷용 컬럼 추가

    col1, col2, col3 = st.columns([6.0,0.2,3.8])
    
    with col1:
        st.markdown("<h5 style='margin:0'>유입 추이</h5>", unsafe_allow_html=True)
        # st.subheader("유입 추이")
        y_cols = [c for c in df_daily.columns if c not in ["날짜","날짜_표시"]]
        fig = px.line(
            df_daily,
            x="날짜_표시",
            y=y_cols,
            markers=True,
            labels={"variable": ""}  # 레전드 제목 제거
        )
        fig.update_layout(
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
        st.plotly_chart(fig, use_container_width=True)
        df_daily.drop(columns="날짜_표시", inplace=True) # 컬램 개수 맞추기

    with col2:
        pass

    with col3:
        st.markdown("<h5 style='margin:0'> </h5>", unsafe_allow_html=True)
        st.markdown(" ")
        # st.subheader("일자별 유입 수치")

        # (1) 날짜 포맷 변환
        df_display = df_daily.copy()
        df_display["날짜"] = df_display["날짜"].dt.strftime("%m월 %d일 (%a)")

        # (2) 합계 행 계산
        table_cols = [c for c in df_display.columns if c != "날짜"]
        df_grid    = df_display[["날짜"] + table_cols]
        bottom     = {
            col: ("합계" if col == "날짜" else int(df_grid[col].sum()))
            for col in df_grid.columns
        }

        # (3) AgGrid 기본 옵션
        gb = GridOptionsBuilder.from_dataframe(df_grid)
        gb.configure_default_column(flex=1, sortable=True, filter=True)
        for col in table_cols:
            gb.configure_column(
                col,
                type=["numericColumn","customNumericFormat"],
                valueFormatter="x.toLocaleString()"
            )
        gb.configure_grid_options(pinnedBottomRowData=[bottom])
        
        ## 컬럼 길이 조정 
        gb.configure_grid_options(
            onGridReady=JsCode("""
                function(params) {
                    // 테이블 너비에 맞게 확장
                    params.api.sizeColumnsToFit();
                    // 또는 내용에 정확히 맞추려면 아래 주석 해제
                    // params.columnApi.autoSizeAllColumns();
                    // 모든 컬럼을 콘텐츠 너비에 맞춰 자동 조정
                    // params.columnApi.autoSizeAllColumns();
                }
            """)
        )
        
        grid_options = gb.build()

        # (4) 테마 자동 선택: Streamlit 내장 ‘streamlit’ 계열 테마 사용
        base_theme = st.get_option("theme.base")  # 보통 "light" 또는 "dark" 반환
        ag_theme   = "streamlit-dark" if base_theme == "dark" else "streamlit"

        # (5) 그리드 출력 시 theme 인자에 ag_theme 전달
        AgGrid(
            df_grid,
            gridOptions=grid_options,
            height=380,
            theme=ag_theme,                  # "streamlit" 또는 "streamlit-dark"
            fit_columns_on_grid_load=True,  # 사이즈 콜백을 사용하므로 여기선 False 권장
            allow_unsafe_jscode=True
        )

    st.divider()

    # ──────────────────────────────────
    # 7. (2) 액션별 세션수
    # ──────────────────────────────────
    metrics_df = (
        df
        .groupby("event_date")[
            [
                "_view_item_sessionCnt",
                "_product_page_scroll_50_sessionCnt",
                "_product_option_price_sessionCnt",
                "_find_nearby_showroom_sessionCnt",
                "_showroom_10s_sessionCnt",
                "_add_to_cart_sessionCnt",
                "_showroom_leads_sessionCnt"
            ]
        ]
        .sum()
        .reset_index()
    )
    # 날짜를 "MM월 DD일" 포맷으로
    metrics_df["날짜"] = metrics_df["event_date"].dt.strftime("%m월 %d일")

    # 3분할 레이아웃
    col_a, col_b, col_c = st.columns(3)

    # (A) PDP 조회 & 스크롤50
    with col_a:
        st.markdown("<h5 style='margin:0'>액션별 추이</h5>", unsafe_allow_html=True)
        st.markdown("[1] 탐색 액션")
        # st.subheader("PDP 조회 & 스크롤50 세션수")
        m1 = metrics_df.rename(columns={
            "_view_item_sessionCnt": "PDP조회_세션수",
            "_product_page_scroll_50_sessionCnt": "PDPscr50_세션수"
        })
        fig1 = px.line(
            m1,
            x="날짜",
            y=["PDP조회_세션수", "PDPscr50_세션수"],
            markers=True,
            labels={"variable": ""}
        )
        fig1.update_layout(
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
        st.plotly_chart(fig1, use_container_width=True)

    # (B) 가격표시 / 쇼룸찾기 / 쇼룸10초
    with col_b:
        st.markdown("<h5 style='margin:0'> </h5>", unsafe_allow_html=True)
        st.markdown("[2] 관심표현 액션")
        # st.subheader("가격표시 / 쇼룸찾기 / 쇼룸10초 세션수")
        m2 = metrics_df.rename(columns={
            "_product_option_price_sessionCnt": "가격표시_세션수",
            "_find_nearby_showroom_sessionCnt": "쇼룸찾기_세션수",
            "_showroom_10s_sessionCnt": "쇼룸10초_세션수"
        })
        fig2 = px.line(
            m2,
            x="날짜",
            y=["가격표시_세션수", "쇼룸찾기_세션수", "쇼룸10초_세션수"],
            markers=True,
            labels={"variable": ""}
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

    # (C) 장바구니 / 쇼룸예약
    with col_c:
        st.markdown("<h5 style='margin:0'> </h5>", unsafe_allow_html=True)
        st.markdown("[3] 전환의도 액션")
        # st.subheader("장바구니 / 쇼룸예약 세션수")
        m3 = metrics_df.rename(columns={
            "_add_to_cart_sessionCnt": "장바구니_세션수",
            "_showroom_leads_sessionCnt": "쇼룸예약_세션수"
        })
        fig3 = px.line(
            m3,
            x="날짜",
            y=["장바구니_세션수", "쇼룸예약_세션수"],
            markers=True,
            labels={"variable": ""}
        )
        fig3.update_layout(
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
        st.plotly_chart(fig3, use_container_width=True)

    # 표
    # —— 1) 원본 복사 & 날짜 문자열 생성 —— 
    df_metrics = metrics_df.copy()
    df_metrics["날짜"] = df_metrics["event_date"].dt.strftime("%m월 %d일 (%a)")
    df_metrics.drop(columns="event_date", inplace=True)
    
    col_mapping = {
        "_view_item_sessionCnt": "PDP조회_세션수",
        "_product_page_scroll_50_sessionCnt": "PDPscr50_세션수",
        "_product_option_price_sessionCnt": "가격표시_세션수",
        "_find_nearby_showroom_sessionCnt": "쇼룸찾기_세션수",
        "_showroom_10s_sessionCnt": "쇼룸10초_세션수",
        "_add_to_cart_sessionCnt": "장바구니_세션수",
        "_showroom_leads_sessionCnt": "쇼룸예약_세션수"
    }
    df_metrics.rename(columns=col_mapping, inplace=True)
    
    # — 3) 표시할 컬럼 순서 지정 (날짜 먼저) —
    display_cols = ["날짜"] + list(col_mapping.values())

    # — 4) 수치형 컬럼만 골라내기 —
    numeric_cols = [c for c in display_cols if pd.api.types.is_numeric_dtype(df_metrics[c])]

    # — 5) 합계 행 계산 —
    bottom_row = {}
    for col in display_cols:
        if col == "날짜":
            bottom_row[col] = "합계"
        else:
            bottom_row[col] = int(df_metrics[col].sum())

    # — 6) AgGrid 옵션 설정 —
    gb = GridOptionsBuilder.from_dataframe(df_metrics[display_cols])
    gb.configure_default_column(flex=1, sortable=True, filter=True)
    for col in numeric_cols:
        gb.configure_column(
            col,
            type=["numericColumn", "customNumericFormat"],
            valueFormatter="x.toLocaleString()"
        )
    gb.configure_grid_options(pinnedBottomRowData=[bottom_row])
    gb.configure_grid_options(
        onGridReady=JsCode("""
            function(params) {
                params.columnApi.autoSizeAllColumns();
            }
        """)
    )
    grid_options = gb.build()

    # — 7) 테마 자동 선택 —
    base_theme = st.get_option("theme.base")               # "light" or "dark"
    ag_theme   = "streamlit-dark" if base_theme=="dark" else "streamlit"

    # — 8) AgGrid 전체 표 출력 —
    AgGrid(
        df_metrics[display_cols],
        gridOptions=grid_options,
        height=300,
        theme=ag_theme,
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True
    )


    # ──────────────────────────────────
    # 8. 유입 현황
    # ──────────────────────────────────
    st.divider()
    st.markdown("<h5 style='margin:0'>유입 현황</h5>", unsafe_allow_html=True)
    col_paid, col_device, col_geo = st.columns(3)

    with col_paid:
        st.markdown("[1] 광고유무")
        vc = df["isPaid_4"].value_counts()
        top4 = vc.nlargest(4)
        others = vc.iloc[4:].sum()
        pie_data = pd.concat([top4, pd.Series({"기타": others})]).reset_index()
        pie_data.columns = ["isPaid_4", "count"]
        fig_paid = px.pie(pie_data, names="isPaid_4", values="count", hole=0.4)
        fig_paid.update_traces(
            textinfo="percent+label",
            textfont_color="white",
            textposition="inside",            # 내부에만 라벨 표시
            insidetextorientation="horizontal",
            domain=dict(x=[0.2, 0.8], y=[0.2, 0.8])  # ← 여기가 파이 크기 조절
        )
        fig_paid.update_layout(
            legend=dict(orientation="v", y=0.5, x=1.02, xanchor="left", yanchor="middle"),
            uniformtext=dict(mode="hide", minsize=12),
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig_paid, use_container_width=True)

    with col_device:
        st.markdown("[2] 디바이스")
        vc = df["device__category"].value_counts()
        top4 = vc.nlargest(4)
        others = vc.iloc[4:].sum()
        pie_data = pd.concat([top4, pd.Series({"기타": others})]).reset_index()
        pie_data.columns = ["device__category", "count"]
        fig_device = px.pie(pie_data, names="device__category", values="count", hole=0.4)
        fig_device.update_traces(
            textinfo="percent+label",
            textfont_color="white",
            textposition="inside",
            insidetextorientation="horizontal",
            domain=dict(x=[0.2, 0.8], y=[0.2, 0.8])  # ← 여기가 파이 크기 조절
        )
        fig_device.update_layout(
            legend=dict(orientation="v", y=0.5, x=1.02, xanchor="left", yanchor="middle"),
            uniformtext=dict(mode="hide", minsize=12),
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig_device, use_container_width=True)

    with col_geo:
        st.markdown("[3] 접속지역")
        vc = df["geo__city"].value_counts()
        top4 = vc.nlargest(4)
        others = vc.iloc[4:].sum()
        pie_data = pd.concat([top4, pd.Series({"기타": others})]).reset_index()
        pie_data.columns = ["geo__city", "count"]
        fig_geo = px.pie(pie_data, names="geo__city", values="count", hole=0.4)
        fig_geo.update_traces(
            textinfo="percent+label",
            textfont_color="white",
            textposition="inside",
            insidetextorientation="horizontal",
            domain=dict(x=[0.2, 0.8], y=[0.2, 0.8])  # ← 여기가 파이 크기 조절
        )
        fig_geo.update_layout(
            legend=dict(orientation="v", y=0.5, x=1.02, xanchor="left", yanchor="middle"),
            uniformtext=dict(mode="hide", minsize=12),
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig_geo, use_container_width=True)
