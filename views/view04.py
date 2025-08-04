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
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re


def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정
    # ──────────────────────────────────
    st.set_page_config(layout="wide", page_title="SLPR | 언드 대시보드")
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
    st.subheader('언드 대시보드 (구축중)')
    st.markdown("""
    이 대시보드는 ... 입니다.  
    여기서는 ... 있습니다.
    """)
    st.markdown(
        '<a href="https://www.notion.so/SLPR-241521e07c7680df86eecf5c5f8da4af#241521e07c768094ab81e56cd47e5164" target="_blank">'
        '지표설명 & 가이드</a>',
        unsafe_allow_html=True
    )
    st.divider()
    

    # ────────────────────────────────────────────────────────────────
    # 사이드바 필터 설정
    # ────────────────────────────────────────────────────────────────
    # st.sidebar.header("Filter")
    
    # today = datetime.now().date()
    # default_end = today - timedelta(days=1)
    # default_start = today - timedelta(days=14)
    # start_date, end_date = st.sidebar.date_input(
    #     "기간 선택",
    #     value=[default_start, default_end],
    #     max_value=default_end
    # )
    # cs = start_date.strftime("%Y%m%d")
    # ce = end_date.strftime("%Y%m%d")
    
    @st.cache_data(ttl=3600)
    def load_data():
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(
            "C:/_code/auth/sleeper-461005-c74c5cd91818.json",
            scopes=scope
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1Li4YzwsxI7rB3Q2Z0gkuGIyANTaxFrVzgsKE-RAAdME/edit?gid=2078920702#gid=2078920702')

        df_PplData   = pd.DataFrame(sh.worksheet('ppl_data').get_all_records())
        df_PplAction = pd.DataFrame(sh.worksheet('ppl_action').get_all_records())
        df_query     = pd.DataFrame(sh.worksheet('query').get_all_records())
        
        # # 3) tb_sleeper_psi
        # bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        # df_psi = bq.get_data("tb_sleeper_psi")
        # df_psi["event_date"] = pd.to_datetime(df_psi["event_date"], format="%Y%m%d")
        
        return df_PplData, df_PplAction, df_query

    with st.spinner("데이터를 불러오는 중입니다. ⏳"):
        df_PplData, df_PplAction, df_query = load_data()


    # 날짜 컬럼 타입 변환
    df_PplData['날짜']   = pd.to_datetime(df_PplData['날짜'])
    df_PplAction['날짜'] = pd.to_datetime(df_PplAction['날짜'])
    df_query['date']     = pd.to_datetime(df_query['date'])
    
    df_PplData_f = df_PplData
    df_PplAction_f = df_PplAction
    df_query_f = df_query


    # ────────────────────────────────────────────────────────────────
    # 1번 영역
    # ────────────────────────────────────────────────────────────────
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)


    # 공통함수 render_aggrid 
    def render_aggrid(
        df: pd.DataFrame,
        height: int = 300,
        use_parent: bool = False,
        flat_cols: list = None,
        ) -> None:
        """
        use_parent: False / True
        """
        df2 = df.copy()
        df2.fillna(0, inplace=True)     # 값이 없는 경우 일단 0으로 치환
        
        # (필수함수) make_num_child
        def make_num_child(header, field, fmt_digits=0, suffix=''):
            return {
                "headerName": header, "field": field,
                "type": ["numericColumn","customNumericFormat"],
                "valueFormatter": JsCode(
                    f"function(params){{"
                    f"  return params.value!=null?"
                    f"params.value.toLocaleString(undefined,{{minimumFractionDigits:{fmt_digits},maximumFractionDigits:{fmt_digits}}})+'{suffix}':'';"
                    f"}}"
                ),
                "cellStyle": JsCode("params=>({textAlign:'right'})")
            }

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


        # (use_parent) flat_cols
        if flat_cols is None:
            flat_cols = [
                {"headerName": "시작 날짜",        "field": "날짜"},
                {"headerName": "UTM 캠페인",    "field": "utm_camp"},
                {"headerName": "UTM 컨텐츠",  "field": "utm_content"},
                make_num_child("전체 금액",   "Cost"),
            ]

        # (use_parent) grouped_cols
        # grouped_cols = [
        # ]

        # (use_parent)
        column_defs = grouped_cols if use_parent else flat_cols
        
        # grid_options & 렌더링
        grid_options = {
            "columnDefs": column_defs,
            "defaultColDef": {"sortable": True, "filter": True, "resizable": True},
            "headerHeight": 30,
            "groupHeaderHeight": 30,
        }

        # # (add_summary) grid_options & 렌더링 -> 합계 행 추가하여 재렌더링
        # grid_options = add_summary(
        #     grid_options,
        #     df2,
        #     {
        #         'ord_amount_sum': 'sum',
        #         'ord_count_sum' : 'sum',
        #         'AOV'           : 'avg',
        #         'cost_gross_sum': 'sum',
        #         'ROAS'          : 'avg',
        #         'session_count' : 'sum',
        #         'CVR'           : 'avg',
        #     }
        # )

        AgGrid(
            df2,
            gridOptions=grid_options,
            height=height,
            fit_columns_on_grid_load=True,  # True면 전체넓이에서 균등분배 
            theme="streamlit-dark" if st.get_option("theme.base") == "dark" else "streamlit",
            allow_unsafe_jscode=True
        )

    # 1번 영역
    st.markdown("<h5 style='margin:0'>채널 목록</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)

    min_date = df_PplData_f['날짜'].min()
    df_PplData__min_date = df_PplData_f[df_PplData_f['날짜'] == min_date][['날짜','utm_camp','utm_content','Cost']]
    render_aggrid(df_PplData__min_date, height=100)
    
    
    
    # ────────────────────────────────────────────────────────────────
    # 2번 영역
    # ────────────────────────────────────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>채널 인게이지먼트 및 액션</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)

    df_PplData_drop = df_PplData_f.drop(columns=['Cost'])
    df_merged = pd.merge(df_PplData_drop, df_PplAction_f, on=['날짜', 'utm_camp', 'utm_content'], how='outer')
        
    def make_num_child(header, field, fmt_digits=0, suffix=''):
        from st_aggrid.shared import JsCode
        return {
            "headerName": header, "field": field,
            "type": ["numericColumn","customNumericFormat"],
            "valueFormatter": JsCode(
                f"function(params){{"
                f"  return params.value!=null?"
                f"params.value.toLocaleString(undefined,{{minimumFractionDigits:{fmt_digits},maximumFractionDigits:{fmt_digits}}})+'{suffix}':'';"
                f"}}"
            ),
            "cellStyle": JsCode("params=>({textAlign:'right'})")
        }

    df_merged_col_list = [
        {"headerName": "날짜", "field": "날짜"},
        {"headerName": "UTM 캠페인", "field": "utm_camp"},
        {"headerName": "UTM 컨텐츠", "field": "utm_content"},
        make_num_child("분배 금액", "Cost"),
        make_num_child("컨텐츠 조회수", "컨텐츠 조회수"),
        make_num_child("댓글수", "댓글수"),
        make_num_child("좋아요수", "좋아요수"),
        make_num_child("브랜드 언급량", "브랜드 언급량"),
        make_num_child("UTM 링크 클릭량", "UTM 링크 클릭량"),
        make_num_child("session_count", "session_count"),
        make_num_child("avg_session_duration_sec", "avg_session_duration_sec"),
        make_num_child("view_item_sessions", "view_item_sessions"),
        make_num_child("scroll_50_sessions", "scroll_50_sessions"),
        make_num_child("view_item_list_sessions", "view_item_list_sessions"),
        make_num_child("product_option_price_sessions", "product_option_price_sessions"),
        make_num_child("find_showroom_sessions", "find_showroom_sessions"),
        make_num_child("add_to_cart_sessions", "add_to_cart_sessions"),
        make_num_child("sign_up_sessions", "sign_up_sessions"),
        make_num_child("showroom_10s_sessions", "showroom_10s_sessions"),
        make_num_child("showroom_leads_sessions", "showroom_leads_sessions"),
    ]
    
    # 캠페인 필터
    camp_list = sorted(df_merged['utm_camp'].dropna().unique())
    cont_list = sorted(df_merged['utm_content'].dropna().unique())
    
    ft1, ft2 = st.columns(2)
    with ft1: sel_camp = st.multiselect("UTM 캠페인 선택", options=camp_list, default=[])
    with ft2: sel_cont = st.multiselect("UTM 컨텐츠 선택", options=cont_list, default=[])

    # ① 캠페인 필터
    if sel_camp:
        df_f = df_merged[df_merged['utm_camp'].isin(sel_camp)]
    else:
        df_f = df_merged.copy()

    # ② 컨텐츠 필터
    if sel_cont:
        df_f = df_f[df_f['utm_content'].isin(sel_cont)]

    render_aggrid(df_f, flat_cols=df_merged_col_list)


    # ────────────────────────────────────────────────────────────────
    # 3번 영역
    # ────────────────────────────────────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>네이버 브랜드 쿼리량</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)

    df = df_query_f  

    # 필터 영역
    ft1, ft2, ft3 = st.columns([1, 0.6, 2])
    with ft1: 
        chart_type = st.radio(
            "시각화 유형 선택", 
            ["누적 막대", "누적 영역", "꺾은선"], 
            horizontal=True, 
            index=0
        )
    with ft2:
        date_unit = st.radio(
            "날짜 단위 선택",
            ["일별", "주별"],
            horizontal=True,
            index=0
        )
    with ft3:
        keywords = df['keyword'].unique().tolist()
        sel_keywords = st.multiselect(
            "키워드 선택", 
            keywords, 
            default=['슬립퍼', '슬립퍼매트리스', '슬립퍼프레임', '슬립퍼침대']
        )
        df_f = df[df['keyword'].isin(sel_keywords)]
        
    st.markdown(" ")


    # 탭 영영ㄱ
    tab_labels = ["RSV", "검색량",  "절대화비율", "보정비율"]
    tabs = st.tabs(tab_labels)
    col_map = {
        "RSV": "RSV",
        "검색량": "검색량",
        "절대화비율": "절대화 비율",
        "보정비율": "보정 비율",
    }

    for i, label in enumerate(tab_labels):
        with tabs[i]:
            y_col = col_map[label]

            # --- 단위별 groupby 및 보간 ---
            if date_unit == "일별":
                x_col = "date"
                plot_df = df_f.copy()
                all_x = pd.date_range(plot_df[x_col].min(), plot_df[x_col].max())
            else:  # 주별
                x_col = "week"
                aggfunc = "sum" if label not in ["절대화비율", "보정비율"] else "mean"
                plot_df = (
                    df_f.groupby([x_col, 'keyword'], as_index=False)[y_col].agg(aggfunc)
                )
                all_x = plot_df[x_col].sort_values().unique()

            all_keywords = plot_df['keyword'].unique()
            idx = pd.MultiIndex.from_product([all_x, all_keywords], names=[x_col, "keyword"])
            plot_df = (
                plot_df.set_index([x_col, 'keyword'])[y_col]
                .reindex(idx, fill_value=0)
                .reset_index()
            )

            # --- 차트 유형별 시각화 ---
            if chart_type == "누적 막대":
                fig = px.bar(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="keyword",
                    barmode="relative",
                    labels={x_col: "날짜" if date_unit == "일별" else "주차", y_col: label, "keyword": "키워드"},
                )
                fig.update_traces(opacity=0.6)

            elif chart_type == "누적 영역":
                fig = px.area(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="keyword",
                    groupnorm="",
                    labels={x_col: "날짜" if date_unit == "일별" else "주차", y_col: label, "keyword": "키워드"},
                )
                fig.update_traces(opacity=0.3)

            elif chart_type == "꺾은선":
                fig = px.line(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color="keyword",
                    markers=True,
                    labels={x_col: "날짜" if date_unit == "일별" else "주차", y_col: label, "keyword": "키워드"},
                )
                fig.update_traces(opacity=0.6)
            else:
                fig = None

            if fig:
                st.plotly_chart(fig, use_container_width=True)


    # ────────────────────────────────────────────────────────────────
    # 4번 영역
    # ────────────────────────────────────────────────────────────────
    # 4번 영역
    st.header(" ")
    st.markdown("<h5 style='margin:0'>터치포인트 이력</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-", unsafe_allow_html=True)




if __name__ == '__main__':
    main()
