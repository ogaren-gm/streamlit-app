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
import io


def main():
        
    # ──────────────────────────────────
    # 스트림릿 페이지 설정 (반드시 최상단)
    # ──────────────────────────────────
    st.set_page_config(layout="wide", page_title="SLPR 대시보드 | 트래픽 대시보드")
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
    st.subheader('트래픽 대시보드')
    st.markdown("설명")
    st.markdown(":primary-badge[:material/Cached: Update]ㅤD-1 데이터는 오전 중 예비 처리된 후, **15:00 이후** 매체 분류가 완료되어 최종 업데이트됩니다.")
    # st.markdown(":green-badge[:material/star: INFO]ㅤ설명")
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
        df["_sourceMedium"] = df["collected_traffic_source__manual_source"].astype(str) + " / " + df["collected_traffic_source__manual_medium"].astype(str)
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
            df["collected_traffic_source__manual_source"].isin(paid_sources)   | df["_sourceMedium"].isin(['youtube / demand_gen','kakako / crm']),
            df["collected_traffic_source__manual_source"].isin(owned_sources)  | (df["_sourceMedium"]=='kakao / channel_message'),
            df["collected_traffic_source__manual_source"].isin(earned_sources) | df["_sourceMedium"].isin(sms_referral),
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
        "🗑️ 필터 초기화",
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
    # st.markdown("<h5 style='margin:0'>유입 추이</h5>", unsafe_allow_html=True)
    # st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>방문 추이</span></h5>", unsafe_allow_html=True)
    st.markdown("<h5 style='margin:0'>방문 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일자별 **방문수, 고유 사용자, 신규 및 재방문수** 현황을 확인할 수 있습니다.")

    
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
        y_cols = [c for c in df_daily.columns if c not in ["날짜","날짜_표시"]]

        # — datetime 축으로 다시 그리기 —
        fig = px.line(
            df_daily,
            x="날짜",
            y=y_cols,
            markers=True,
            labels={"variable": ""}  # 레전드 제목 제거
        )

        # — 주말(토·일) 영역 강조 (±12시간) —
        for d in df_daily["날짜"]:
            start = d - timedelta(hours=12)
            end   = d + timedelta(hours=12)
            if d.weekday() == 5:  # 토요일
                fig.add_vrect(x0=start, x1=end, fillcolor="blue", opacity=0.2, layer="below", line_width=0)
            elif d.weekday() == 6:  # 일요일
                fig.add_vrect(x0=start, x1=end, fillcolor="red",  opacity=0.2, layer="below", line_width=0)

        # — x축 라벨 다시 한글 포맷으로 세팅 —
        fig.update_xaxes(
            tickvals=df_daily["날짜"],
            ticktext=df_daily["날짜_표시"]
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
        df_daily.drop(columns="날짜_표시", inplace=True)

    with col2:
        pass

    with col3:
        # st.markdown("<h5 style='margin:0'> </h5>", unsafe_allow_html=True)
        st.markdown("")
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
        # _x1, _y1 = st.columns([3,2])
        # with _x1 : pass
        # with _y1 : 
        #     to_excel = io.BytesIO()
        #     with pd.ExcelWriter(to_excel, engine="xlsxwriter") as writer:
        #         df_grid.to_excel(writer, index=False, sheet_name="x")
        #     to_excel.seek(0)
        #     excel_bytes = to_excel.read()
        #     st.download_button(
        #         "📊 Download",
        #         data=excel_bytes,
        #         file_name="x.xlsx",
        #         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        #         use_container_width=True
        #     )


    # # ──────────────────────────────────
    # # 8. 유입 현황
    # # ──────────────────────────────────
    # st.divider()

    # st.markdown("<h5 style='margin:0'>유입 현황</h5>", unsafe_allow_html=True)
    # _col1, _col2 = st.columns([1, 25])
    # with _col1:
    #     # badge() 자체를 호출만 하고, 반환값을 쓰지 마세요
    #     st.badge("Info", icon=":material/star:", color="green")
    # with _col2:
    #     st.markdown("설명")
    #     st.markdown("")
    
    
    # col_paid, col_device, col_geo = st.columns(3)

    # with col_paid:
    #     st.badge("광고유무", icon=":material/check:", color="grey")
    #     vc = df["isPaid_4"].value_counts()
    #     top4 = vc.nlargest(4)
    #     others = vc.iloc[4:].sum()
    #     pie_data = pd.concat([top4, pd.Series({"기타": others})]).reset_index()
    #     pie_data.columns = ["isPaid_4", "count"]
    #     fig_paid = px.pie(pie_data, names="isPaid_4", values="count", hole=0.4)
    #     fig_paid.update_traces(
    #         textinfo="percent+label",
    #         textfont_color="white",
    #         textposition="inside",            # 내부에만 라벨 표시
    #         insidetextorientation="horizontal",
    #         domain=dict(x=[0.2, 0.8], y=[0.2, 0.8])  # ← 여기가 파이 크기 조절
    #     )
    #     fig_paid.update_layout(
    #         legend=dict(orientation="v", y=0.5, x=1.02, xanchor="left", yanchor="middle"),
    #         uniformtext=dict(mode="hide", minsize=12),
    #         margin=dict(l=20, r=20, t=20, b=20)
    #     )
    #     st.plotly_chart(fig_paid, use_container_width=True)

    # with col_device:
    #     st.badge("디바이스", icon=":material/check:", color="grey")
    #     vc = df["device__category"].value_counts()
    #     top4 = vc.nlargest(4)
    #     others = vc.iloc[4:].sum()
    #     pie_data = pd.concat([top4, pd.Series({"기타": others})]).reset_index()
    #     pie_data.columns = ["device__category", "count"]
    #     fig_device = px.pie(pie_data, names="device__category", values="count", hole=0.4)
    #     fig_device.update_traces(
    #         textinfo="percent+label",
    #         textfont_color="white",
    #         textposition="inside",
    #         insidetextorientation="horizontal",
    #         domain=dict(x=[0.2, 0.8], y=[0.2, 0.8])  # ← 여기가 파이 크기 조절
    #     )
    #     fig_device.update_layout(
    #         legend=dict(orientation="v", y=0.5, x=1.02, xanchor="left", yanchor="middle"),
    #         uniformtext=dict(mode="hide", minsize=12),
    #         margin=dict(l=20, r=20, t=20, b=20)
    #     )
    #     st.plotly_chart(fig_device, use_container_width=True)

    # with col_geo:
    #     st.badge("접속지역", icon=":material/check:", color="grey")
    #     vc = df["geo__city"].value_counts()
    #     top4 = vc.nlargest(4)
    #     others = vc.iloc[4:].sum()
    #     pie_data = pd.concat([top4, pd.Series({"기타": others})]).reset_index()
    #     pie_data.columns = ["geo__city", "count"]
    #     fig_geo = px.pie(pie_data, names="geo__city", values="count", hole=0.4)
    #     fig_geo.update_traces(
    #         textinfo="percent+label",
    #         textfont_color="white",
    #         textposition="inside",
    #         insidetextorientation="horizontal",
    #         domain=dict(x=[0.2, 0.8], y=[0.2, 0.8])  # ← 여기가 파이 크기 조절
    #     )
    #     fig_geo.update_layout(
    #         legend=dict(orientation="v", y=0.5, x=1.02, xanchor="left", yanchor="middle"),
    #         uniformtext=dict(mode="hide", minsize=12),
    #         margin=dict(l=20, r=20, t=20, b=20)
    #     )
    #     st.plotly_chart(fig_geo, use_container_width=True)


    # ──────────────────────────────────
    # 8. 유입 현황 (상위 4개 + 기타 누적 막대 차트, 함수 사용)
    # ──────────────────────────────────
    st.divider()
    # st.markdown("<h5 style='margin:0'>유입 현황</h5>", unsafe_allow_html=True)
    # st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>방문 현황</span></h5>", unsafe_allow_html=True)
    st.markdown("<h5 style='margin:0'>방문 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ**광고유무, 디바이스, 접속지역**별 추이를 확인하고, 하단에서는 선택한 행 필드를 기준으로 해당 지표들을 피벗하여 조회할 수 있습니다.")

        
    col_paid, col_device, col_geo = st.columns(3)

    # 공통: top4 + 기타 누적 막대 차트 그리기
    def plot_top4_bar(df, group_col, container, title, height=300, top_n=4):
        # 총합 기준 상위 top_n
        total = df.groupby(group_col)["pseudo_session_id"].nunique()
        top_items = total.nlargest(top_n).index.tolist()
        # 기타 처리
        df2 = df.copy()
        df2[group_col] = df2[group_col].where(df2[group_col].isin(top_items), other="기타")
        # 일자·그룹별 집계
        tmp = (
            df2
            .groupby(["event_date", group_col])["pseudo_session_id"]
            .nunique()
            .reset_index(name="count")
        )
        # 피벗 및 날짜 포맷
        pivot = tmp.pivot(index="event_date", columns=group_col, values="count").fillna(0).reset_index()
        pivot["날짜"] = pivot["event_date"].dt.strftime("%m월 %d일")
        # 컬럼 순서: top 순서대로 + 기타
        cols = [c for c in top_items if c in pivot.columns] + (["기타"] if "기타" in pivot.columns else [])
        # 차트 생성
        fig = px.bar(
            pivot,
            x="날짜",
            y=cols,
            labels={"variable": ""},
            title=title,
            opacity=0.6     # 0.0(완전 투명) ~ 1.0(불투명)
        )
        fig.update_layout(
            barmode="stack",
            bargap=0.1,        # 카테고리간 간격 (0~1)
            bargroupgap=0.2,  # 같은 카테고리 내 막대 간 간격 (0~1)
            height=400,
            xaxis_title=None,
            yaxis_title=None,
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom")
            # margin=dict(l=20, r=20, t=90, b=00)
        )
        container.plotly_chart(fig, use_container_width=True, height=height)

    # (A) 광고유무
    with col_paid:
        plot_top4_bar(df, "isPaid_4", col_paid, "💰 광고유무")

    # (B) 디바이스
    with col_device:
        plot_top4_bar(df, "device__category", col_device, "📱 디바이스")

    # (C) 지역
    with col_geo:
        plot_top4_bar(df, "geo__city", col_geo, "🌐 접속지역")

    # ──────────────────────────────────
    # 동적 피벗 테이블 with 멀티셀렉터 & 싱글열 필드
    # ──────────────────────────────────
    # (1) 필터 UI
    col1, col2 = st.columns([2,2])
    with col1:
        sel_rows = st.multiselect(
            "행 필드 선택",
            ["날짜", "세션 소스", "세션 매체", "세션 캠페인"],
            default=["날짜"],
            key="pivot_rows"
        )
        if not sel_rows:
            sel_rows = ["날짜"]
    with col2:
        sel_col = st.selectbox(
            "열 필드 선택",
            ["광고유무", "디바이스", "접속지역"],
            index=2,
            key="pivot_col"
        )

    # (2) 매핑
    row_map = {
        "날짜":       "event_date",
        "세션 소스":   "collected_traffic_source__manual_source",
        "세션 매체":   "collected_traffic_source__manual_medium",
        "세션 캠페인": "collected_traffic_source__manual_campaign_name"
    }
    col_map = {
        "광고유무":    "isPaid_4",
        "디바이스":    "device__category",
        "접속지역":    "geo__city"
    }
    inv_row_map = {v:k for k,v in row_map.items()}

    idx_cols  = [row_map[r] for r in sel_rows]
    pivot_col = col_map[sel_col]

    # (3) 집계
    df_tmp = (
        df
        .groupby(idx_cols + [pivot_col])["pseudo_session_id"]
        .nunique()
        .reset_index(name="유입수")
    )

    # (4) 피벗 후 reset_index
    pivot = df_tmp.pivot_table(
        index=idx_cols,
        columns=pivot_col,
        values="유입수",
        fill_value=0
    ).reset_index()

    # (5) 날짜 문자열 처리 & internal → display 이름 매핑
    if "event_date" in idx_cols:
        pivot["날짜"] = pivot["event_date"].dt.strftime("%m월 %d일")
        pivot.drop(columns="event_date", inplace=True)
    # 이제 모든 idx_cols(영어)들을 한국어로 rename
    pivot.rename(columns={c: inv_row_map[c] for c in idx_cols if c in inv_row_map}, inplace=True)

    # (6) 열 순서 재정의: 숫자형 합계 내림차순
    from pandas.api.types import is_numeric_dtype
    cats = [c for c in pivot.columns if c not in sel_rows and is_numeric_dtype(pivot[c])]
    col_sums = pivot[cats].sum().sort_values(ascending=False)
    pivot = pivot[sel_rows + col_sums.index.tolist()]

    # (7) 행 순서 재정의
    if sel_rows == ["날짜"]:
        pivot.sort_values("날짜", ascending=True, inplace=True)
    else:
        pivot["__row_sum"] = pivot[cats].sum(axis=1)
        pivot.sort_values("__row_sum", ascending=False, inplace=True)
        pivot.drop(columns="__row_sum", inplace=True)
    pivot.reset_index(drop=True, inplace=True)

    # (8) 합계 행·열 추가
    pivot["합계"] = pivot[cats].sum(axis=1)
    col_totals = pivot[cats].sum()
    bottom = {}
    for c in pivot.columns:
        if c in sel_rows:
            bottom[c] = "합계"
        elif is_numeric_dtype(pivot[c]):
            bottom[c] = int(col_totals[c]) if c in col_totals else int(pivot[c].sum())
        else:
            bottom[c] = ""
    
    # (9) AgGrid 옵션 설정
    gb = GridOptionsBuilder.from_dataframe(pivot)
    gb.configure_default_column(flex=1, sortable=True, filter=True)
    # 숫자형 컬럼 (유입수, 합계)에 천단위 콤마 포맷
    for c in cats + ["합계"]:
        gb.configure_column(
            c,
            type=["numericColumn","customNumericFormat"],
            valueFormatter="x.toLocaleString()"
        )
    # 마지막 합계 열 고정
    gb.configure_column("합계", pinned="right")
    gb.configure_grid_options(pinnedBottomRowData=[bottom])
    gb.configure_grid_options(onGridReady=JsCode("function(params){params.api.sizeColumnsToFit();}"))
    grid_opts = gb.build()

    # (10) 결과 출력
    theme = "streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit"
    AgGrid(
        pivot,
        gridOptions=grid_opts,
        height=282,
        theme=theme,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True
    )




    # ──────────────────────────────────
    # 7. (2) 액션별 세션수 + 행/열 필터 + 하단 표
    # ──────────────────────────────────
    st.divider()
    # st.markdown("<h5 style='margin:0'>액션 추이</h5>", unsafe_allow_html=True)
    # st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>액션</span> 추이</h5>", unsafe_allow_html=True)
    st.markdown("<h5 style='margin:0'>주요 이벤트 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ**제품탐색, 관심표현, 전환의도**별 추이를 확인하고, 하단에서는 선택한 행 필드를 기준으로 해당 지표들을 피벗하여 조회할 수 있습니다.")


    # (a) 메트릭 집계
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
    metrics_df["날짜"] = metrics_df["event_date"].dt.strftime("%m월 %d일")

    # (b) 3분할 레이아웃 & 원본 3개 그래프
    col_a, col_b, col_c = st.columns(3)

    # (A) 제품탐색 Action
    with col_a:
        m1 = metrics_df.rename(columns={
            "_view_item_sessionCnt":"PDP조회_세션수",
            "_product_page_scroll_50_sessionCnt":"PDPscr50_세션수"
        })
        fig1 = px.line(
            m1, x="날짜",
            y=["PDP조회_세션수","PDPscr50_세션수"],
            markers=True, labels={"variable":""},
            title="🔍 제품탐색"
        )
        fig1.update_layout(
            height=400,
            xaxis_title=None,
            yaxis_title=None,
            legend=dict(orientation="h", y=1.02, x=1,
                        xanchor="right", yanchor="bottom")
        )
        st.plotly_chart(fig1, use_container_width=True)

    # (B) 관심표현 Action
    with col_b:
        m2 = metrics_df.rename(columns={
            "_product_option_price_sessionCnt":"가격표시_세션수",
            "_find_nearby_showroom_sessionCnt":"쇼룸찾기_세션수",
            "_showroom_10s_sessionCnt":"쇼룸10초_세션수"
        })
        fig2 = px.line(
            m2, x="날짜",
            y=["가격표시_세션수","쇼룸찾기_세션수","쇼룸10초_세션수"],
            markers=True, labels={"variable":""},
            title="❤️ 관심표현"
        )
        fig2.update_layout(
            height=400,
            xaxis_title=None,
            yaxis_title=None,
            legend=dict(orientation="h", y=1.02, x=1,
                        xanchor="right", yanchor="bottom")
        )
        st.plotly_chart(fig2, use_container_width=True)

    # (C) 전환의도 Action
    with col_c:
        m3 = metrics_df.rename(columns={
            "_add_to_cart_sessionCnt":"장바구니_세션수",
            "_showroom_leads_sessionCnt":"쇼룸예약_세션수"
        })
        fig3 = px.line(
            m3, x="날짜",
            y=["장바구니_세션수","쇼룸예약_세션수"],
            markers=True, labels={"variable":""},
            title="🛒 전환의도"
        )
        fig3.update_layout(
            height=400,
            xaxis_title=None,
            yaxis_title=None,
            legend=dict(orientation="h", y=1.02, x=1,
                        xanchor="right", yanchor="bottom")
        )
        st.plotly_chart(fig3, use_container_width=True)

    # (c) 하단 표용 필터 UI (좌우 배치)
    colr, colc = st.columns([2,2])
    with colr:
        sel_rows = st.multiselect(
            "행 필드 선택",
            ["날짜","세션 소스","세션 매체","세션 캠페인"],
            default=["날짜"],
            key="action_rows"
        )
        if not sel_rows:
            sel_rows = ["날짜"]
    with colc:
        sel_cats = st.multiselect(
            "열 필드 선택",
            ["제품탐색","관심표현","전환의도"],
            default=["제품탐색","관심표현","전환의도"],
            key="action_cats"
        )
        if not sel_cats:
            sel_cats = ["제품탐색","관심표현","전환의도"]

    # (d) 매핑 정의
    row_map = {
        "날짜":       "event_date",
        "세션 소스":   "collected_traffic_source__manual_source",
        "세션 매체":   "collected_traffic_source__manual_medium",
        "세션 캠페인": "collected_traffic_source__manual_campaign_name"
    }
    inv_row_map = {v:k for k,v in row_map.items()}
    col_labels = {
        "_view_item_sessionCnt":"PDP조회_세션수",
        "_product_page_scroll_50_sessionCnt":"PDPscr50_세션수",
        "_product_option_price_sessionCnt":"가격표시_세션수",
        "_find_nearby_showroom_sessionCnt":"쇼룸찾기_세션수",
        "_showroom_10s_sessionCnt":"쇼룸10초_세션수",
        "_add_to_cart_sessionCnt":"장바구니_세션수",
        "_showroom_leads_sessionCnt":"쇼룸예약_세션수"
    }
    category_map = {
        "PDP조회_세션수":"제품탐색",
        "PDPscr50_세션수":"제품탐색",
        "가격표시_세션수":"관심표현",
        "쇼룸찾기_세션수":"관심표현",
        "쇼룸10초_세션수":"관심표현",
        "장바구니_세션수":"전환의도",
        "쇼룸예약_세션수":"전환의도"
    }

    # (e) DataFrame 준비
    grp_keys = [row_map[r] for r in sel_rows]
    df_tab = (
        df
        .groupby(grp_keys)[list(col_labels.keys())]
        .sum()
        .reset_index()
    )
    # 날짜 컬럼 처리
    if "event_date" in grp_keys:
        df_tab["날짜"] = df_tab["event_date"].dt.strftime("%m월 %d일")
        df_tab.drop(columns="event_date", inplace=True)
        sel_rows = ["날짜" if r=="날짜" else r for r in sel_rows]
    # 기타 행 필드명 한글화
    df_tab.rename(columns={k:inv_row_map[k] for k in grp_keys if k!="event_date"}, inplace=True)
    # 메트릭 컬럼명 한글화
    df_tab.rename(columns=col_labels, inplace=True)

    # (f) 필터링 & 행합계/열합계 삽입
    value_cols = [lbl for lbl,cat in category_map.items() if cat in sel_cats]
    display_cols = sel_rows + value_cols
    df_display = df_tab[display_cols].copy()
    # 행합계
    df_display["합계"] = df_display[value_cols].sum(axis=1)
    # 정렬
    if sel_rows == ["날짜"]:
        df_display.sort_values("날짜", inplace=True)
    else:
        df_display.sort_values("합계", ascending=False, inplace=True)
    df_display.reset_index(drop=True, inplace=True)

    # 하단 열합계
    from pandas.api.types import is_numeric_dtype
    bottom = {}
    for c in df_display.columns:
        if c in sel_rows:
            bottom[c] = "합계"
        elif is_numeric_dtype(df_display[c]):
            bottom[c] = int(df_display[c].sum())
        else:
            bottom[c] = ""

    # (g) AgGrid 설정 & 출력
    gb = GridOptionsBuilder.from_dataframe(df_display)
    gb.configure_default_column(flex=1, sortable=True, filter=True)
    for c in value_cols + ["합계"]:
        gb.configure_column(
            c,
            type=["numericColumn","customNumericFormat"],
            valueFormatter="x.toLocaleString()"
        )
    gb.configure_column(sel_rows[0])
    gb.configure_column("합계", pinned="right")
    gb.configure_grid_options(pinnedBottomRowData=[bottom])
    gb.configure_grid_options(onGridReady=JsCode("function(params){params.api.sizeColumnsToFit();}"))
    grid_opts = gb.build()

    AgGrid(
        df_display,
        gridOptions=grid_opts,
        height=265,
        theme="streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit",
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True
    )




    # # ──────────────────────────────────
    # # 9. TS 히트맵 (요일×시간대) + 지표 선택 & 다운로드 버튼 & AgGrid 테이블
    # # ──────────────────────────────────
    # st.divider()
    # st.markdown("<h5 style='margin:0'>히트맵</h5>", unsafe_allow_html=True)
    # _col1, _col2 = st.columns([1, 25])
    # with _col1:
    #     # badge() 자체를 호출만 하고, 반환값을 쓰지 마세요
    #     st.badge("Info", icon=":material/star:", color="green")
    # with _col2:
    #     st.markdown("설명")
    #     st.markdown("")

    # # 1) event_ts → datetime, 요일·시간 컬럼 추가
    # df["event_dt"] = pd.to_datetime(df["event_ts"], unit="ms")
    # df["요일"]      = df["event_dt"].dt.day_name(locale="ko_KR")
    # df["hour"]      = df["event_dt"].dt.hour

    # # 2) 지표 선택 & 다운로드 버튼을 같은 행에 배치
    # _empty1, col_select, col_download = st.columns([4, 2, 1])
    # with _empty1:
    #     pass
    # with col_select:
    #     metric = st.selectbox(
    #         "",
    #         ["방문수", "유저수", "신규방문수", "재방문수"],
    #         index=0,
    #         label_visibility="collapsed"
    #     )
    #     # 3) 선택된 지표 기반으로 집계 컬럼과 함수 매핑
    #     agg_map = {
    #         "방문수":     ("pseudo_session_id", "nunique"),
    #         "유저수":     ("user_pseudo_id",    "nunique"),
    #         "신규방문수": ("_isUserNew_y",      "sum"),
    #         "재방문수":   ("_isUserNew_n",      "sum")
    #     }
    #     col_name, aggfunc = agg_map[metric]

    #     # 4) 요일×시간대별 집계 및 피벗
    #     heat = (
    #         df
    #         .groupby(["요일", "hour"])[col_name]
    #         .agg(aggfunc)
    #         .reset_index(name=metric)
    #     )
    #     order = ["월요일","화요일","수요일","목요일","금요일","토요일","일요일"]
    #     pivot = (
    #         heat
    #         .pivot(index="요일", columns="hour", values=metric)
    #         .reindex(order)
    #         .fillna(0)
    #     )
    #     df_grid = pivot.reset_index()
    #     df_grid.columns = df_grid.columns.astype(str)

    #     # 5) "n" → "n시" 컬럼명 변환
    #     rename_map = {c: f"{int(c)}시" for c in df_grid.columns if c != "요일"}
    #     df_grid.rename(columns=rename_map, inplace=True)

    #     # 6) 합계행 계산
    #     bottom = {"요일": "합계"}
    #     for c in df_grid.columns:
    #         if c != "요일":
    #             bottom[c] = int(df_grid[c].sum())

    # with col_download:
    #     # # 7) df_grid CSV 다운로드
    #     # csv = df_grid.to_csv(index=False, encoding="utf-8-sig")
    #     # st.download_button(
    #     #     "CSV 다운로드",
    #     #     data=csv,
    #     #     file_name="ts_heatmap.csv",
    #     #     mime="text/csv",
    #     #     use_container_width=True
    #     # )
    #     to_excel = io.BytesIO()
    #     with pd.ExcelWriter(to_excel, engine="xlsxwriter") as writer:
    #         df_grid.to_excel(writer, index=False, sheet_name="heatmap")
    #     to_excel.seek(0)
    #     excel_bytes = to_excel.read()
    #     st.download_button(
    #         "엑셀로 다운로드",
    #         data=excel_bytes,
    #         file_name="heatmap.xlsx",
    #         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #         use_container_width=True
    #     )
        

    # # 8) AgGrid 설정 & 렌더링
    # gb = GridOptionsBuilder.from_dataframe(df_grid)
    # gb.configure_default_column(
    #     flex=1,
    #     sortable=True,
    #     filter=True,
    #     valueFormatter="x.toLocaleString()"
    # )
    # gb.configure_column("요일", pinned="left")
    # gb.configure_grid_options(pinnedBottomRowData=[bottom])
    # grid_options = gb.build()

    # theme = "streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit"
    # AgGrid(
    #     df_grid,
    #     gridOptions=grid_options,
    #     height=270,
    #     theme=theme,
    #     fit_columns_on_grid_load=True,
    #     allow_unsafe_jscode=True,
    #     # enable_enterprise_modules=True
    # )
