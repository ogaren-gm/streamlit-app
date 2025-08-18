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
from google.oauth2.service_account import Credentials
import gspread

def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정
    # ──────────────────────────────────
    # st.set_page_config(layout="wide", page_title="SLPR | 트래픽 대시보드")
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
    st.markdown("""
    이 대시보드는 **자사몰 트래픽**의 방문 유형, 광고 유무, 접속 지역, 주요 이벤트 세션수 등을 한눈에 보여주는 **GA 대시보드**입니다.  
    여기서는 “**얼마나 방문했는지, 어떤 사용자가 방문했는지, 어떤 이벤트를 발생시켰는지**”의 추이를 직관적으로 확인할 수 있습니다.
    """)
    st.markdown(
        '<a href="https://www.notion.so/SLPR-241521e07c7680df86eecf5c5f8da4af?pvs=97#241521e07c7680439a57cc45c0fba6f2" target="_blank">'
        '지표설명 & 가이드</a>',
        unsafe_allow_html=True
    )
    st.divider()
    
    
    # ────────────────────────────────────────────────────────────────
    # 사이드바 필터 설정
    # ────────────────────────────────────────────────────────────────
    st.sidebar.header("Filter")
    
    today = datetime.now().date()
    default_end = today - timedelta(days=1)
    default_start = today - timedelta(days=14)
    start_date, end_date = st.sidebar.date_input(
        "기간 선택",
        value=[default_start, default_end],
        max_value=default_end
    )
    cs = start_date.strftime("%Y%m%d")
    ce = end_date.strftime("%Y%m%d")


    @st.cache_data(ttl=3600)
    def load_data(cs: str, ce: str) -> pd.DataFrame:
        
        # tb_sleeper_psi
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df_psi = bq.get_data("tb_sleeper_psi")
        df_psi["event_date"] = pd.to_datetime(df_psi["event_date"], format="%Y%m%d")
        
        def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
            """
            1. 파생 컬럼 생성 : _isUserNew_y, _isUserNew_n, _sourceMedium, _engagement_time_sec_sum
            2. 이벤트 플래그 생성
            3. isPaid_4 컬럼 생성 -> categorize_paid 함수로 생성 (추후 확장성 높게 수정하기 위해)
            """
            df["_isUserNew_y"] = (df["first_visit"] == 1).astype(int)
            df["_isUserNew_n"] = (df["first_visit"] == 0).astype(int)
            df["_sourceMedium"] = (df["collected_traffic_source__manual_source"].astype(str) + " / " + df["collected_traffic_source__manual_medium"].astype(str))
            df["_engagement_time_sec_sum"] = df["engagement_time_msec_sum"] / 1000
            events = [
                ("view_item", "_view_item_sessionCnt"),
                ("product_page_scroll_50", "_product_page_scroll_50_sessionCnt"),
                ("product_option_price", "_product_option_price_sessionCnt"),
                ("find_nearby_showroom", "_find_nearby_showroom_sessionCnt"),
                ("showroom_10s", "_showroom_10s_sessionCnt"),
                ("add_to_cart", "_add_to_cart_sessionCnt"),
                ("showroom_leads", "_showroom_leads_sessionCnt"),
                ("purchase", "_purchase_sessionCnt")
            ]
            for event_name, flag_col in events:
                df[flag_col] = (df[event_name] > 0).astype(int)
            df["isPaid_4"] = categorize_paid(df)
            return df

        def categorize_paid(df: pd.DataFrame) -> pd.Series:
            paid_sources = ['google','naver','meta','meta_adv','mobon','mobion','naver_gfa','DV360','dv360','fb','sns','IGShopping','criteo']
            owned_sources = ['litt.ly','instagram','l.instagram.com','instagram.com','blog.naver.com','m.blog.naver.com','smartstore.naver.com','m.brand.naver.com']
            earned_sources = ['youtube','youtube.com','m.youtube.com']
            sms_referral = ['m.facebook.com / referral','l.facebook.com / referral','facebook.com / referral']
            conds = [
                # Organic
                df["_sourceMedium"].isin(['google / organic','naver / organic']),
                # Paid (exclude sponsored)
                (df["collected_traffic_source__manual_source"].isin(paid_sources) & ~df["_sourceMedium"].eq('google / sponsored'))
                    | df["_sourceMedium"].isin(['youtube / demand_gen','kakako / crm']),
                # Owned
                df["collected_traffic_source__manual_source"].isin(owned_sources)
                    | (df["_sourceMedium"] == 'kakao / channel_message'),
                # Earned (include sponsored)
                df["collected_traffic_source__manual_source"].isin(earned_sources)
                    | df["_sourceMedium"].isin(sms_referral + ['google / sponsored'])
            ]
            choices = ['ETC','Paid','Owned','Earned']
            return np.select(conds, choices, default='ETC')
        
        return preprocess_data(df_psi)


    # ────────────────────────────────────────────────────────────────
    # 데이터 불러오기
    # ────────────────────────────────────────────────────────────────
    st.toast("GA D-1 데이터는 오전에 예비 처리되고, **15시 이후에 최종 업데이트** 됩니다.", icon="🔔")
    with st.spinner("데이터를 불러오는 중입니다. ⏳"):
        df_psi = load_data(cs, ce) # 전처리된 df_psi



    # ────────────────────────────────────────────────────────────────
    # 공통 함수
    # ────────────────────────────────────────────────────────────────
    def pivot_daily(
        df: pd.DataFrame,
        group_cols: list[str] | None = None,
        top_n: int | None = None,
        기타_label: str = "기타"
    ) -> pd.DataFrame:

        if group_cols and top_n is not None:
            key = group_cols[0]
            # 1) group_cols[0]별 방문수 집계 후 상위 top_n 값 추출
            top_vals = (
                df
                .groupby(key, as_index=False)
                .agg(방문수_temp = ("pseudo_session_id", "nunique"))
                .nlargest(top_n, "방문수_temp")[key]
                .tolist()
            )
            # 2) 상위 그룹 외 모든 값을 기타_label로 치환
            df[key] = df[key].where(df[key].isin(top_vals), 기타_label)

        # 실제 pivot 집계
        cols = ["event_date"] + (group_cols or [])
        result = (
            df
            .groupby(cols, as_index=False)
            .agg(
                방문수    = ("pseudo_session_id", "nunique"),
                유저수    = ("user_pseudo_id",    "nunique"),
                신규방문수 = ("_isUserNew_y",      "sum"),
                재방문수   = ("_isUserNew_n",      "sum"),
            )
            .rename(columns={"event_date": "날짜"})
        )
        # 날짜 형식 변경
        result["날짜"] = result["날짜"].dt.strftime("%Y-%m-%d")
        return result

    def pivot_bySource(
        df: pd.DataFrame,
        index: str,
        columns: str,
        values: str = "pseudo_session_id",
        aggfunc: str = "nunique"
        ) -> pd.DataFrame:

        wide = (
            df
            .pivot_table(
                index=index,
                columns=columns,
                values=values,
                aggfunc=aggfunc,
                fill_value=0
            )
            .reset_index()
        )
        wide.columns.name = None
        return wide


    def render_stacked_bar(
        df: pd.DataFrame,
        x: str,
        y: str | list[str],
        color: str,
        ) -> None:

        # y가 단일 문자열이면 리스트로 감싸기
        y_cols = [y] if isinstance(y, str) else y

        fig = px.bar(
            df,
            x=x,
            y=y_cols,
            color=color,
            labels={"variable": ""},
            opacity=0.6,
            barmode="stack",
        )
        fig.update_layout(
            bargap=0.1,        # 카테고리 간 간격 (0~1)
            bargroupgap=0.2,   # 같은 카테고리 내 막대 간 간격 (0~1)
            height=400,
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

    def render_aggrid(
        df: pd.DataFrame,
        height: int = 352,
        use_parent: bool = False,
        agg_map: dict[str, str] | None = None,  # {'col_name': 'sum'|'avg'|'mid', ...}
        ):

        # 날짜 컬럼 최좌측으로 재배치
        df2 = df.copy()
        if "날짜" in df2.columns:
            cols = df2.columns.tolist()
            cols.remove("날짜")
            df2 = df2[["날짜"] + cols]
            ## 내림차순 정렬 추가
            df2 = df2.sort_values("날짜", ascending=False)

        # (필수함수) add_summary
        def add_summary(grid_options: dict, df: pd.DataFrame, agg_map: dict[str, str]):
            summary: dict[str, float | str] = {}
            for col, op in agg_map.items():
                val = None
                try:
                    if op == 'sum':
                        val = df[col].sum()
                    elif op == 'avg':
                        val = df[col].mean()
                    elif op == 'mid':
                        val = df[col].median()
                except:
                    val = None

                # NaN / Inf / numpy 타입 → None or native 타입으로 처리
                if val is None or isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    summary[col] = None
                else:
                    # numpy 타입 제거
                    if isinstance(val, (np.integer, np.int64, np.int32)):
                        summary[col] = int(val)
                    elif isinstance(val, (np.floating, np.float64, np.float32)):
                        summary[col] = float(round(val, 2))
                    else:
                        summary[col] = val

            grid_options['pinnedBottomRowData'] = [summary]
            return grid_options

        # AgGrid 옵션 생성
        gb = GridOptionsBuilder.from_dataframe(df2)
        
        # 날짜 컬럼 고정
        if "날짜" in df2.columns:
            gb.configure_column(
                "날짜",
                header_name="날짜",
                pinned="left",
                type=["textColumn"],
                # width=110
            )

        # 이건 다 숫자형식이라, 따로 공통함수를 만들지 않고 바로 적용 (기능: 수치 컬럼 천단위 쉼표 포맷팅)
        num_cols = df2.select_dtypes(include=[np.number]).columns.tolist()
        for col in num_cols:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=JsCode(
                    "function(params) { return params.value != null ? params.value.toLocaleString() : ''; }"
                ),
            )

        # parent/child 레이아웃 사용 시
        if use_parent:
            # 예: children 이나 groupCols가 미리 설정되어 있으면 사용
            pass

        grid_options = gb.build()

        # 탭 전환이나 리사이즈 시에도 컬럼 크기 자동 조정
        auto_size_js = JsCode("function(params) { params.api.sizeColumnsToFit(); }")
        grid_options['onFirstDataRendered'] = auto_size_js
        grid_options['onGridSizeChanged']   = auto_size_js

        # 렌더링
        if agg_map: # agg_map이 주어지면 합계 행을 추가하여 재랜더링
            grid_options = add_summary(grid_options, df2, agg_map)
        
        AgGrid(
            df2,
            gridOptions=grid_options,
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=True,
            enable_enterprise_modules=False,
            height=height
        )


    # 데이터프레임 생성
    df_daily         =  pivot_daily(df_psi)                                       
    df_daily_paid    =  pivot_daily(df_psi, group_cols=["isPaid_4"])                              
    df_daily_device  =  pivot_daily(df_psi, group_cols=["device__category"])
    df_daily_geo     =  pivot_daily(df_psi, group_cols=["geo__city"],          top_n=6,   기타_label="기타")
    df_daily_source  =  pivot_daily(df_psi, group_cols=["_sourceMedium"],      top_n=20,   기타_label="기타")

    # 데이터프레임 별 -> 컬럼명 한글 치환
    df_daily_paid   = df_daily_paid.rename(columns={"isPaid_4":           "광고유무"})
    df_daily_device = df_daily_device.rename(columns={"device__category":   "디바이스"})
    df_daily_geo    = df_daily_geo.rename(columns={"geo__city":           "접속지역"})
    df_daily_source = df_daily_source.rename(columns={"_sourceMedium":       "유입매체"})
    
    # 데이터프레임 공통 -> 합계행을 위한 json 생성
    summary_map__daily = {
        '방문수'   : 'sum',
        '유저수'   : 'sum',
        '신규방문수': 'sum',
        '재방문수' : 'sum',
    }

    # ──────────────────────────────────
    # 1) 방문 추이
    # ──────────────────────────────────
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown("<h5 style='margin:0'>방문 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ날짜별 **방문수**(세션 기준), **유저수**(중복 제거), **신규 및 재방문수** 추이를 확인할 수 있습니다.")
    # — 시각화
    c1, _p, c2 = st.columns([5.0, 0.2, 3.8])
    with c1:
        y_cols = [c for c in df_daily.columns if c not in "날짜"]
        render_line_chart(df_daily, x="날짜", y=y_cols)
    with _p: pass
    with c2:
        # render_aggrid(df_daily)
        render_aggrid(df_daily, agg_map=summary_map__daily)


    # ──────────────────────────────────
    # 2) 주요 방문 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>주요 방문 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ탭을 클릭하여, **광고유무, 디바이스, 접속지역**별 방문 추이를 확인할 수 있습니다.")
    
    tab1, tab2, tab3, tab4 = st.tabs(["광고유무", "디바이스", "접속지역", "유입매체"])
    
    # — 광고유무 탭
    with tab1:
        paid_options = ["전체"] + sorted(df_psi["isPaid_4"].dropna().unique().tolist())
        sel_paid = st.selectbox("광고유무 선택", paid_options, index=0)
        if sel_paid == "전체":
            df_paid_tab = df_daily_paid.copy()
        else:
            df_paid_tab = df_daily_paid[df_daily_paid["광고유무"] == sel_paid]
        c1, _p, c2 = st.columns([5.0, 0.2, 3.8])
        with c1:
            render_stacked_bar(df_paid_tab, x="날짜", y="방문수", color="광고유무")
        with _p: pass
        with c2:
            render_aggrid(df_paid_tab, agg_map=summary_map__daily)
    
    # — 디바이스 탭
    with tab2:
        device_options = ["전체"] + sorted(df_psi["device__category"].dropna().unique().tolist())
        sel_device = st.selectbox("디바이스 선택", device_options, index=0)
        if sel_device == "전체":
            df_dev_tab = df_daily_device.copy()
        else:
            df_dev_tab = df_daily_device[df_daily_device["디바이스"] == sel_device]
        c1, _p, c2 = st.columns([5.0, 0.2, 3.8])
        with c1:
            render_stacked_bar(df_dev_tab, x="날짜", y="방문수", color="디바이스")
        with _p: pass
        with c2:
            render_aggrid(df_dev_tab, agg_map=summary_map__daily)
    
    # — 접속지역 탭
    with tab3:
        geo_options = ["전체"] + sorted(df_psi["geo__city"].dropna().unique().tolist())
        sel_geo = st.selectbox("접속지역 선택", geo_options, index=0)
        if sel_geo == "전체":
            df_geo_tab = df_daily_geo.copy()
        else:
            df_geo_tab = df_daily_geo[df_daily_geo["접속지역"] == sel_geo]
        c1, _p, c2 = st.columns([5.0, 0.2, 3.8])
        with c1:
            render_stacked_bar(df_geo_tab, x="날짜", y="방문수", color="접속지역")
        with _p: pass
        with c2:
            render_aggrid(df_geo_tab, agg_map=summary_map__daily)
            
    # — 유입매체 탭
    with tab4:
        source_options = ["전체"] + sorted(df_psi["_sourceMedium"].dropna().unique().tolist())
        sel_source = st.selectbox("유입매체 선택", source_options, index=0)
        if sel_source == "전체":
            df_source_tab = df_daily_source.copy()
        else:
            df_source_tab = df_daily_source[df_daily_source["유입매체"] == sel_source]
        c1, _p, c2 = st.columns([5.0, 0.2, 3.8])
        with c1:
            render_stacked_bar(df_source_tab, x="날짜", y="방문수", color="유입매체")
        with _p: pass
        with c2:
            render_aggrid(df_source_tab, agg_map=summary_map__daily)


    # ──────────────────────────────────
    # 3) 주요 이벤트 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>주요 이벤트 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ**PDP 조회부터 쇼룸 예약까지**, 날짜별 **GA 주요 이벤트**에 대한 추이를 확인할 수 있습니다.")

    # 매핑 명칭 일괄 선언
    col_map = {
        "_view_item_sessionCnt":             "PDP조회_세션수",
        "_product_page_scroll_50_sessionCnt":"PDPscr50_세션수",
        "_product_option_price_sessionCnt":  "가격표시_세션수",
        "_find_nearby_showroom_sessionCnt":  "쇼룸찾기_세션수",
        "_showroom_10s_sessionCnt":          "쇼룸10초_세션수",
        "_add_to_cart_sessionCnt":           "장바구니_세션수",
        "_showroom_leads_sessionCnt":        "쇼룸예약_세션수",
    }

    # metrics_df
    metrics_df = (
        df_psi
        .groupby("event_date", as_index=False)
        .agg(**{ new_name: (orig_name, "sum")
                for orig_name, new_name in col_map.items() })
    )
    # 날짜 형식 변경, event_date Drop
    metrics_df["날짜"] = metrics_df["event_date"].dt.strftime("%Y-%m-%d")
    metrics_df = metrics_df.drop(columns=["event_date"])

    # metrics_df -> 합계행을 위한 json 생성
    summary_map__metric = {
        'PDP조회_세션수'   : 'sum',
        'PDPscr50_세션수' : 'sum',
        '가격표시_세션수'  : 'sum',
        '쇼룸찾기_세션수'  : 'sum',
        '쇼룸10초_세션수'  : 'sum',
        '장바구니_세션수'  : 'sum',
        '쇼룸예약_세션수'  : 'sum',
    }

    # — 제품탐색
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        y_cols = ["PDP조회_세션수","PDPscr50_세션수"]
        render_line_chart(metrics_df, x="날짜", y=y_cols, title="🔍 제품탐색")
        
    # — 관심표현
    with col_b:
        y_cols = ["가격표시_세션수","쇼룸찾기_세션수","쇼룸10초_세션수"]
        render_line_chart(metrics_df, x="날짜", y=y_cols, title="❤️ 관심표현")

    # — 전환의도
    with col_c:
        y_cols = ["장바구니_세션수","쇼룸예약_세션수"]
        render_line_chart(metrics_df, x="날짜", y=y_cols, title="🛒 전환의도")

    render_aggrid(metrics_df, agg_map=summary_map__metric)


    # ──────────────────────────────────
    # 4) 소스·매체별 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>유입매체별 현황 (기획중)</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ.")

    # tab_paid, tab_device, tab_geo, tab_event = st.tabs(["광고유무", "디바이스", "접속지역", "이벤트별"])

    # # — 광고유무 
    # with tab_paid:
    #     df_paid_wide = pivot_bySource(df_psi, index="_sourceMedium", columns="isPaid_4")
    #     render_aggrid(df_paid_wide)

    # # — 디바이스
    # with tab_device:
    #     df_dev_wide = pivot_bySource(df_psi, index="_sourceMedium", columns="device__category")
    #     render_aggrid(df_dev_wide)
        
    # # — 접속지역
    # with tab_geo:
    #     df_geo_wide = pivot_bySource(df_psi, index="_sourceMedium", columns="geo__city")
    #     render_aggrid(df_geo_wide)
        
    # # — 이벤트별
    # with tab_event:
    #     df_evt = df_psi.melt(
    #         id_vars=['_sourceMedium'],
    #         value_vars=list(col_map.keys()),
    #         var_name='event',
    #         value_name='count'
    #     )
    #     df_evt['count'] = df_evt['count'].astype(int)
    #     df_evt_wide = df_evt.pivot_table(
    #         index="_sourceMedium",
    #         columns="event",
    #         values="count",
    #         aggfunc="sum",
    #         fill_value=0
    #     ).reset_index()
    #     render_aggrid(df_evt_wide)



if __name__ == "__main__":
    main()
