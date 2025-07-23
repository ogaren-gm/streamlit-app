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
    st.set_page_config(layout="wide", page_title="SLPR 대시보드 | 카테고리별 액션 리포트")
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
    st.subheader('카테고리별 액션 리포트')
    st.markdown("설명")
    st.markdown(":primary-badge[:material/Cached: Update]ㅤ-")
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
    if st.sidebar.button("초기화"):
        st.experimental_rerun()

    cs = start_date.strftime("%Y%m%d")
    ce = end_date.strftime("%Y%m%d")


    # ────────────────────────────────────────────────────────────────
    # 데이터 로드
    # ────────────────────────────────────────────────────────────────
    @st.cache_data(ttl=3600)
    def load_data(cs: str, ce: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        
        # tb_media
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df_bq = bq.get_data("tb_media")
        df_bq["event_date"] = pd.to_datetime(df_bq["event_date"], format="%Y%m%d")
        parts = df_bq['campaign_name'].str.split('_', n=5, expand=True)
        df_bq['campaign_name_short'] = df_bq['campaign_name']
        mask = parts[5].notna()
        df_bq.loc[mask, 'campaign_name_short'] = (
            parts.loc[mask, :4].apply(lambda r: '_'.join(r.dropna().astype(str)), axis=1)
        )
        
        # GS
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/11ov-_o6Lv5HcuZo1QxrKOZnLtnxEKTiV78OFBZzVmWA/edit')
        ws = sh.worksheet('parse')
        df_sheet = pd.DataFrame(ws.get_all_records())
        
        # merged
        merged = df_bq.merge(df_sheet, how='left', on='campaign_name_short')
        merged['cost_gross'] = np.where(
            merged['media_name'].isin(['GOOGLE','META']), merged['cost']*1.1/0.98, merged['cost']
        )
        cond = (
            (merged['media_name']=='NSA') &
            merged['utm_source'].isna() & merged['utm_medium'].isna() &
            merged['media_name_type'].isin(['RSA_AD','TEXT_45'])
        )
        merged.loc[cond, ['utm_source','utm_medium']] = ['naver','search-nonmatch']
        
        # tb_sleeper_psi
        df_psi = bq.get_data("tb_sleeper_psi")
        df_psi["event_date"] = pd.to_datetime(df_psi["event_date"], format="%Y%m%d")
        df_daily_psi = df_psi.groupby("event_date", as_index=False).agg(
            psi_sum=("pseudo_session_id","nunique"),
            PDP조회=("view_item","sum"), PDPscr50=("product_page_scroll_50","sum"),
            가격표시=("product_option_price","sum"), 쇼룸찾기=("find_nearby_showroom","sum"),
            쇼룸10초=("showroom_10s","sum"), 장바구니=("add_to_cart","sum"),
            쇼룸예약=("showroom_leads","sum"), 구매=("purchase","sum")
        ).sort_values("event_date")
        df_cost = merged.groupby("event_date", as_index=False).agg(
            cost_sum=("cost","sum"), cost_gross_sum=("cost_gross","sum")
        ).sort_values("event_date")
        df_daily = df_daily_psi.merge(df_cost, on="event_date", how="left")
        
        # tb_sleeper_product_report
        df_prod_rep = bq.get_data("tb_sleeper_product_report")
        # df_prod_rep["event_date"] = pd.to_datetime(df_prod_rep["event_date"], format="%Y%m%d")
        
        return merged, df_daily, df_prod_rep

    # 데이터 로드
    merged, df_daily, df_prod_rep = load_data(cs, ce)


    # # ────────────────────────────────────────────────────────────────
    # # 1. 종합 액션 CPA/CVR
    # # ────────────────────────────────────────────────────────────────
    # df = df_daily.copy()
    # df["CPA"] = df.apply(lambda r: r["cost_gross_sum"]/r["psi_sum"] if r["psi_sum"]>0 else 0, axis=1)
    # df["날짜_표시"] = df["event_date"].dt.strftime("%m월 %d일")

    # st.markdown("<h5 style='margin:0'>종합 CPA (GA Total)</h5>", unsafe_allow_html=True)
    # st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    # # 이벤트별 CPA 계산 및 테이블 구성
    # events = ["PDP조회","PDPscr50","가격표시","쇼룸찾기","쇼룸10초","장바구니","쇼룸예약","구매"]
    # for ev in events:
    #     df[f"CPA_{ev}"] = df.apply(lambda r: round(r["cost_gross_sum"]/r[ev],2) if r[ev]>0 else 0, axis=1)
    # df_cpa = df[["event_date","psi_sum","cost_sum","cost_gross_sum","CPA"]+events+[f"CPA_{ev}" for ev in events]].copy()
    # df_cpa.rename(columns={"psi_sum":"방문수","cost_sum":"광고비","cost_gross_sum":"광고비(G)","CPA":"유입단가"}, inplace=True)
    # df_cpa["날짜"] = df_cpa["event_date"].dt.strftime("%Y-%m-%d")
    # df_cpa.fillna(0, inplace=True)

    # # 합계행
    # bottom = {"날짜":"합계"}
    # for col in ["방문수","광고비","광고비(G)"]:
    #     bottom[col] = int(df_cpa[col].sum())
    # bottom["유입단가"] = int(round(df_cpa["유입단가"].mean(),0))
    # for ev in events:
    #     bottom[ev] = int(df_cpa[ev].sum())
    #     bottom[f"CPA_{ev}"] = int(round(df_cpa[f"CPA_{ev}"].mean(),0))
    
    # def make_num_child(header, field):
    #     return {"headerName":header,"field":field,"type":["numericColumn","customNumericFormat"],
    #             "valueFormatter":JsCode("function(params){return params.value!=null?params.value.toLocaleString(undefined,{maximumFractionDigits:0}):'';}"),
    #             "cellStyle":JsCode("params=>({textAlign:'right'})")}
    
    # column_defs = [{"headerName":"날짜","field":"날짜","pinned":"left","width":100,
    #                 "cellStyle":JsCode("params=>({textAlign:'left'})")}]
    # for col in ["방문수","광고비","광고비(G)","유입단가"]:
    #     column_defs.append(make_num_child(col,col))
    # for ev in events:
    #     column_defs.append({"headerName":ev,"children":[make_num_child("Actual",ev),make_num_child("CPA",f"CPA_{ev}")]} )
    
    # grid_options = {"columnDefs":column_defs,"defaultColDef":{"sortable":True,"filter":True,"resizable":True},
    #                 "pinnedBottomRowData":[bottom],"headerHeight":30,"groupHeaderHeight":30}
    
    # AgGrid(df_cpa, gridOptions=grid_options, height=450, fit_columns_on_grid_load=False,
    #         theme="streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit",
    #         allow_unsafe_jscode=True)



    # ────────────────────────────────────────────────────────────────
    # 1. 종합 액션 CPA/CVR
    # ────────────────────────────────────────────────────────────────
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>GA 종합</span> 액션 리포트</h5>", unsafe_allow_html=True)   
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)
    df = df_daily.copy()

    # 전체세션수, CPA, CVR 계산
    df["전체세션수"] = df["psi_sum"]
    df["CPA"]         = df.apply(lambda r: r["cost_gross_sum"]/r["psi_sum"] if r["psi_sum"]>0 else 0, axis=1)
    df["CVR"]         = (df["psi_sum"] / df["psi_sum"] * 100).round(2)

    # 이벤트별 CPA/CVR
    events = ["PDP조회","PDPscr50","가격표시","쇼룸찾기","쇼룸10초","장바구니","쇼룸예약","구매"]
    for ev in events:
        df[f"CPA_{ev}"] = df.apply(lambda r: round(r["cost_gross_sum"]/r[ev],2) if r[ev]>0 else 0, axis=1)
        df[f"CVR_{ev}"] = df.apply(lambda r: round(r[ev]/r["psi_sum"]*100,2) if r["psi_sum"]>0 else 0, axis=1)

    # 테이블용 DataFrame 준비
    df_cpa = df[
        ["event_date","전체세션수","cost_sum","cost_gross_sum","CPA","CVR"]
        + events
        + [f"CPA_{ev}" for ev in events]
        + [f"CVR_{ev}" for ev in events]
    ].copy()
    df_cpa.rename(columns={
        "cost_sum":       "광고비",
        "cost_gross_sum": "광고비(G)"
    }, inplace=True)
    df_cpa["날짜"] = df_cpa["event_date"].dt.strftime("%Y-%m-%d")
    df_cpa.fillna(0, inplace=True)

    # 합계행
    bottom = {"날짜": "합계"}
    for col in ["전체세션수","광고비","광고비(G)"]:
        bottom[col] = int(df_cpa[col].sum())
    bottom["CPA"]   = int(round(df_cpa["CPA"].mean(),0))
    bottom["CVR"]   = ""  # 전체 CVR은 비워두기
    for ev in events:
        bottom[ev]            = int(df_cpa[ev].sum())
        bottom[f"CPA_{ev}"]   = int(round(df_cpa[f"CPA_{ev}"].mean(),0))
        bottom[f"CVR_{ev}"]   = ""  # 이벤트별 CVR 합계 생략

    # 숫자 셀 포맷 헬퍼 (CPA 소수 0자리, CVR 소수 2자리 + %)
    def make_num_child(header, field, width=90, fmt_digits=0, suffix=""):
        return {
            "headerName": header,
            "field": field,
            "width": width,
            "type": ["numericColumn","customNumericFormat"],
            "valueFormatter": JsCode(
                f"function(params){{"
                f"  return params.value!=null"
                f"    ? params.value.toLocaleString(undefined,{{minimumFractionDigits:{fmt_digits}, maximumFractionDigits:{fmt_digits}}}) + '{suffix}'"
                f"    : '';"
                f"}}"
            ),
            "cellStyle": JsCode("params=>({textAlign:'right'})")
        }

    # columnDefs 구성
    column_defs = [
        # 날짜
        {"headerName":"날짜","field":"날짜","pinned":"left","width":100,
         "cellStyle":JsCode("params=>({textAlign:'left'})")},

        # 광고비, 광고비(G)
        # make_num_child("광고비",    "광고비",    fmt_digits=0),
        make_num_child("광고비(G)", "광고비(G)", fmt_digits=0),

        # 전체세션수 parent → Actual/CPA/CVR
        {"headerName":"전체세션수","children":[
            make_num_child("Actual","전체세션수", fmt_digits=0),
            make_num_child("CPA",   "CPA",          fmt_digits=0),
            make_num_child("CVR",   "CVR",          fmt_digits=2, suffix="%")
        ]}
    ]
    # 이벤트별 그룹
    for ev in events:
        column_defs.append({
            "headerName": ev,
            "children": [
                make_num_child("Actual", ev,            fmt_digits=0),
                make_num_child("CPA",    f"CPA_{ev}",   fmt_digits=0),
                make_num_child("CVR",    f"CVR_{ev}",   fmt_digits=2, suffix="%")
            ]
        })

    grid_options = {
        "columnDefs": column_defs,
        "defaultColDef": {"sortable":True,"filter":True,"resizable":True},
        "pinnedBottomRowData": [bottom],
        "headerHeight":30, "groupHeaderHeight":30
    }

    AgGrid(
        df_cpa,
        gridOptions=grid_options,
        height=450,
        fit_columns_on_grid_load=False,
        theme="streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit",
        allow_unsafe_jscode=True
    )


    # ────────────────────────────────────────────────────────────────
    # 2. 카테고리별 액션 CPA/CVR (최적화 버전)
    # ────────────────────────────────────────────────────────────────
    st.divider()
    df_prod_rep['event_date'] = df_prod_rep['event_date'].astype(str)
    merged['event_date']      = merged['event_date'].astype(str)

    
    # 1) 피벗 및 메트릭 계산 함수 (벡터화 + 누락 컬럼 채우기)
    def pivot_and_calc(df_rep: pd.DataFrame, df_cost: pd.DataFrame) -> pd.DataFrame:
        # a) 일별 전체 세션 수
        sessions = (
            df_rep
            .groupby("event_date")["pseudo_session_id"]
            .nunique()
            .rename("session_start")
        )

        # b) 이벤트별 세션 피벗
        evt = (
            df_rep
            .groupby(["event_date","event_name"])["pseudo_session_id"]
            .nunique()
            .unstack(fill_value=0)
        )
        # 필요한 컬럼 순서 지정 & 없으면 0 채우기
        cols = [
            "view_item",
            "product_page_scroll_50",
            "product_option_price",
            "find_nearby_showroom",
            "showroom_10s",
            "add_to_cart",
            "showroom_leads",
            "purchase"
        ]
        evt = evt.reindex(columns=cols, fill_value=0)

        # c) 일별 광고비 집계
        cost = (
            df_cost
            .groupby("event_date")["cost_gross"]
            .sum()
            .rename("cost_gross_sum")
        )

        # d) 합치기
        df2 = pd.concat([sessions, evt, cost], axis=1).reset_index()

        # e) 한글 컬럼명 매핑
        df2.rename(columns={
            "view_item": "PDP조회",
            "product_page_scroll_50": "PDPscr50",
            "product_option_price": "가격표시",
            "find_nearby_showroom": "쇼룸찾기",
            "showroom_10s": "쇼룸10초",
            "add_to_cart": "장바구니",
            "showroom_leads": "쇼룸예약",
            "purchase": "구매",
        }, inplace=True)

        # f) CPA, CVR 벡터 계산
        events = ["PDP조회","PDPscr50","가격표시","쇼룸찾기","쇼룸10초","장바구니","쇼룸예약","구매"]
        for ev in events:
            df2[f"CPA_{ev}"] = (
                df2["cost_gross_sum"] / df2[ev]
            ).replace([np.inf, -np.inf, np.nan], 0).round(2)
            df2[f"CVR_{ev}"] = (
                df2[ev] / df2["session_start"] * 100
            ).fillna(0).round(2)
            
        #  세션수 CPA/CVR 계산 추가
        df2["CPA_session_start"] = (df2["cost_gross_sum"] / df2["session_start"]).replace([np.inf, -np.inf, np.nan], 0).round(2)
        df2["CVR_session_start"] = 100.0

        return df2

    # 2) 필터별 DataFrame 생성
    def make_filtered_df(cat_a=None, is_paid=None, cat_b_pattern=None):
        # 1) product_report 필터
        df_f = df_prod_rep
        if cat_a:
            df_f = df_f[df_f['product_cat_a'] == cat_a]
        if is_paid:
            df_f = df_f[df_f['is_paid'] == is_paid]
        if cat_b_pattern:
            df_f = df_f[df_f['product_cat_b'].str.contains(cat_b_pattern)]

        # 2) merged 필터 (같은 cat_a, is_paid, cat_b_pattern) -- 신규 ... 
        df_cost = merged
        if cat_a:
            df_cost = df_cost[df_cost['brand_type'] == cat_a]
        if cat_b_pattern:
            df_cost = df_cost[df_cost['product_type'].str.contains(cat_b_pattern)]

        return pivot_and_calc(df_f, df_cost)

    # 3) 각 탭 데이터 준비
    df_sleeper_paid     = make_filtered_df(cat_a="슬립퍼", is_paid="y")
    df_sleeper_mat      = make_filtered_df(cat_a="슬립퍼", cat_b_pattern="매트리스")
    df_sleeper_paid_mat = make_filtered_df(cat_a="슬립퍼", is_paid="y", cat_b_pattern="매트리스")
    df_sleeper_frm      = make_filtered_df(cat_a="슬립퍼", cat_b_pattern="원목 침대|패브릭 침대|프레임")
    df_sleeper_paid_frm = make_filtered_df(cat_a="슬립퍼", is_paid="y", cat_b_pattern="원목 침대|패브릭 침대|프레임")
    
    df_nooer = make_filtered_df(cat_a="누어")
    df_nooer_mat = make_filtered_df(cat_a="누어", cat_b_pattern="매트리스")
    df_nooer_frm = make_filtered_df(cat_a="누어", cat_b_pattern="프레임")
    

    # 4) AgGrid 렌더링 함수
    def render_performance_aggrid(
        df: pd.DataFrame,
        date_field: str = "event_date",
        cost_field: str = "cost_gross_sum",
        height: int = 400
    ):
        df = df.copy()
        # 날짜 문자열
        if pd.api.types.is_datetime64_any_dtype(df[date_field]):
            df[date_field] = df[date_field].dt.strftime("%Y-%m-%d")

        def make_num_child(header, field, width: int = 90):
            return {
                "headerName": header,
                "field": field,
                "width": width,
                "type": ["numericColumn","customNumericFormat"],
                "valueFormatter": JsCode(
                    "function(params){"
                    "  return params.value != null && params.value !== ''"
                    "    ? params.value.toLocaleString(undefined,{maximumFractionDigits:0})"
                    "    : '';"
                    "}"
                ),
                "cellStyle": JsCode("params => ({ textAlign:'right' })")
            }

        # CVR 전용 포맷터 (소수점 둘째자리까지)
        def make_cvr_child(header, field, width: int = 90):
            return {
                "headerName": header,
                "field": field,
                "width": width,
                "type": ["numericColumn","customNumericFormat"],
                "valueFormatter": JsCode(
                    "function(params){"
                    "  return params.value != null && params.value !== ''"
                    "    ? params.value.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) + '%'"
                    "    : '';"
                    "}"
                ),
                "cellStyle": JsCode("params => ({ textAlign:'right' })")
            }


        column_defs = [{
            "headerName": "날짜",
            "field": date_field,
            "pinned": "left",
            "width": 100,
            "cellStyle": JsCode("params => ({ textAlign:'left' })")
        }, {
            "headerName": "광고비(G)",
            "field": cost_field,
            "width": 100,
            "type": ["numericColumn","customNumericFormat"],
            "valueFormatter": JsCode(
                "function(params){"
                "  return params.value != null"
                "    ? params.value.toLocaleString(undefined,{maximumFractionDigits:0})"
                "    : '';"
                "}"
            ),
            "cellStyle": JsCode("params => ({ textAlign:'right' })")
        }, {
            "headerName": "세션수",
            "children": [
                make_num_child("Actual", "session_start"),
                make_num_child("CPA",     "CPA_session_start"),
                make_cvr_child("CVR",     "CVR_session_start"),
            ]
        }]

        for ev in ["PDP조회","PDPscr50","가격표시","쇼룸찾기","쇼룸10초","장바구니","쇼룸예약","구매"]:
            column_defs.append({
                "headerName": ev,
                "children": [
                    make_num_child("Actual", ev),
                    make_num_child("CPA",    f"CPA_{ev}"),
                    make_cvr_child("CVR",    f"CVR_{ev}")
                ]
            })

        grid_options = {
            "columnDefs": column_defs,
            "defaultColDef": {
                "sortable": True, "filter": True, "resizable": True,
                "wrapHeaderText": True, "autoHeaderHeight": True
            },
            "headerHeight": 30,
            "groupHeaderHeight": 30,
        }

        AgGrid(
            df,
            gridOptions=grid_options,
            height=height,
            fit_columns_on_grid_load=False,
            theme="streamlit-dark" if st.get_option("theme.base")=="dark" else "streamlit",
            allow_unsafe_jscode=True
        )

    # 5) 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>슬립퍼</span> 액션 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    # 6) 탭별 출력
    tabs = st.tabs([
        "Paid",
        "매트리스",
        "매트리스(Paid)",
        "프레임",
        "프레임(Paid)"
    ])
    with tabs[0]:
        render_performance_aggrid(df_sleeper_paid)
    with tabs[1]:
        render_performance_aggrid(df_sleeper_mat)
    with tabs[2]:
        render_performance_aggrid(df_sleeper_paid_mat)
    with tabs[3]:
        render_performance_aggrid(df_sleeper_frm)
    with tabs[4]:
        render_performance_aggrid(df_sleeper_paid_frm)

    st.divider()
    st.markdown("<h5 style='margin:0'><span style='color:#FF4B4B;'>누어</span> 액션 리포트</h5>", unsafe_allow_html=True)  
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    # 6) 탭별 출력
    tabs = st.tabs([
        "누어",
        "매트리스",
        "프레임"
    ])
    with tabs[0]:
        render_performance_aggrid(df_nooer)
    with tabs[1]:
        render_performance_aggrid(df_nooer_mat)
    with tabs[2]:
        render_performance_aggrid(df_nooer_frm)


if __name__ == '__main__':
    main()
