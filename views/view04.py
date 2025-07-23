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

# ────────────────────────────────────────────────────────────────
# 메인 실행 함수
# ────────────────────────────────────────────────────────────────
def main():

        
    # ──────────────────────────────────
    # 스트림릿 페이지 설정 (반드시 최상단)
    # ──────────────────────────────────
    st.set_page_config(layout="wide", page_title="SLPR 대시보드 | 매출 종합 리포트")
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
    st.subheader('매출 종합 리포트')
    st.markdown("설명")
    st.markdown(":primary-badge[:material/Cached: Update]ㅤ설명.")
    # st.markdown(":green-badge[:material/star: INFO]ㅤ설명")
    st.divider()



    st.sidebar.header("Filter")

    # 1) 기간 파라미터 생성
    today         = datetime.now().date()
    default_end   = today - timedelta(days=1)
    default_start = today - timedelta(days=14)

    # 캐시용: YYYYMMDD / 필터용: YYYY-MM-DD
    cs        = default_start.strftime("%Y%m%d")
    ce        = default_end.strftime("%Y%m%d")
    start_str = default_start.strftime("%Y-%m-%d")
    end_str   = default_end.strftime("%Y-%m-%d")


    # ──────────────────────────────────
    # 1. 캐시된 데이터 로더
    # ──────────────────────────────────
    @st.cache_data(ttl=3600)
    def load_all(cs: str, ce: str, start_str: str, end_str: str):
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)


        ##### tb_sleeper_product -> df_sessions
        df_prod = bq.get_data("tb_sleeper_product")        
        # (전처리)
        df_prod["event_date_str"] = (pd.to_datetime(df_prod["event_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d"))
        df_prod = df_prod[(df_prod["event_date_str"] >= start_str) & (df_prod["event_date_str"] <= end_str)]
        df_prod.loc[df_prod["product_cat_b"].isin(["패브릭 침대", "원목 침대"]), "product_cat_b"] = "프레임"
        # (groupby)
        df_sessions_cat = (
            df_prod
            .groupby(["event_date", "product_cat_a", "product_cat_b"])["pseudo_session_id"]
            .nunique()
            .reset_index(name="pseudo_session_id_cnt")
        )
        # (wide melting)
        df_sessions = (
            df_sessions_cat
            .pivot(
                index="event_date",
                columns=["product_cat_a", "product_cat_b"],
                values="pseudo_session_id_cnt"
            )
            .fillna(0)
        )
        # MultiIndex 컬럼을 '<cat_a>_<cat_b>_pseudo_session_id_cnt' 형태로 변경
        df_sessions.columns = [
            f"{cat_a}_{cat_b}_pseudo_session_id_cnt"
            for cat_a, cat_b in df_sessions.columns
        ]
        # 인덱스를 컬럼으로 복원
        df_sessions = df_sessions.reset_index()


        ##### tb_media → cost_gross 계산 & 일자별 합계
        df_media = bq.get_data("tb_media")
        df_media["event_date_str"] = (
            pd.to_datetime(df_media["event_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        )
        df_media = df_media[(df_media["event_date_str"] >= start_str) & (df_media["event_date_str"] <= end_str)]
        df_media["cost_gross"] = np.where(df_media["media_name"].isin(["GOOGLE","META"]), df_media["cost"] * 1.1 / 0.98, df_media["cost"])
        df_cost = df_media.groupby("event_date_str", as_index=False).agg(cost_gross_sum=("cost_gross","sum"))




        ##### tb_sleeper_psi
        df_psi = bq.get_data("tb_sleeper_psi")
        df_psi["event_date_str"] = (
            pd.to_datetime(df_psi["event_date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        )
        df_psi = df_psi[(df_psi["event_date_str"] >= start_str) & (df_psi["event_date_str"] <= end_str)]
        df_all = (
            df_psi.groupby("event_date_str")["pseudo_session_id"].nunique().reset_index(name="pseudo_session_id_cnt")
        )



        ###### GS -> df_wide
        # scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        # creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
        # gc    = gspread.authorize(creds)
        # sh    = gc.open_by_url('https://docs.google.com/spreadsheets/d/1chXeCek1UZPyCr18zLe7lV-8tmv6YPWK6cEqAJcDPqs/edit?gid=695160982')
        # df_sheet = pd.DataFrame(sh.worksheet('raw_매출_테이블').get_all_records())
        # df_total = df_sheet[df_sheet['카테고리']=="합계"].copy()
        # df_wide = (df_total.pivot_table(index='일자', columns=['브랜드','구분'], values='값', aggfunc='sum', fill_value=0))
        # df_wide.columns.name = None
        # df_wide = df_wide.reset_index()
        # df_wide.columns = ['event_date_str'] + [f"{brand}_{metric}" for brand, metric in df_wide.columns.tolist()[1:]]
        # ////// 가라로 맹글어봄
        dates = pd.date_range("2025-07-01","2025-07-21")
        data = {"event_date_str": dates.strftime("%Y-%m-%d")}
        for b in ["누어","슬립퍼"]:
            for p in ["매트리스","프레임"]:
                amt = np.random.randint(10000,50001,len(dates)); cnt = np.random.randint(10,101,len(dates))
                data[f"{b}_{p}_주문금액"]    = amt
                data[f"{b}_{p}_주문수"]      = cnt
                data[f"{b}_{p}_주문당금액"]  = (amt/cnt).round(2)
        df_wide = pd.DataFrame(data)



        return df_sessions, df_cost, df_wide, df_all
        
    # 외부 로드 & 집계
    df_sessions, df_cost, df_wide, df_all = load_all(cs, ce, start_str, end_str)




    # master DataFrame 생성 & 필터 적용
    master = (
        df_sessions
        .merge(df_cost, on="event_date_str", how="outer")
        .merge(df_wide, on="event_date_str", how="outer")
        .merge(df_all, on="event_date_str", how="outer")
        .sort_values("event_date_str")
        .reset_index(drop=True)
    )
    sel_start, sel_end = st.sidebar.date_input("기간 선택", value=[default_start, default_end])
    sel_start_str = sel_start.strftime("%Y-%m-%d")
    sel_end_str   = sel_end.strftime("%Y-%m-%d")
    df_filtered = master[(master["event_date_str"] >= sel_start_str) & (master["event_date_str"] <= sel_end_str)]
    
    st.dataframe(df_filtered)









    # 전처리
    df_filtered["누어_주문당 금액"]    = df_filtered["누어_주문 금액"]    / df_filtered["누어_주문 수"]
    df_filtered["슬립퍼_주문당 금액"] = df_filtered["슬립퍼_주문 금액"] / df_filtered["슬립퍼_주문 수"]

    # 전처리 2
    df_filtered["통합_주문 금액"] = df_filtered["누어_주문 금액"] + df_filtered["슬립퍼_주문 금액"]
    df_filtered["통합_주문 수"] = df_filtered["누어_주문 수"] + df_filtered["슬립퍼_주문 수"]
    df_filtered["통합_주문당 금액"] = df_filtered["통합_주문 금액"] + df_filtered["통합_주문 수"]
    
    # df_filtered['ROAS'] = df_filtered['통합_주문 금액'] / df_filtered['cost_gross_sum'] * 100
    # df_filtered['CVR']  = df_filtered['통합_주문 수'] / df_filtered['pseudo_session_id_cnt'] * 100
    
    
    # 결과 출력

    df_filtered_1 = df_filtered[['event_date_str', '통합_주문 금액', 'cost_gross_sum', '통합_주문당 금액', '통합_주문 수', 'pseudo_session_id_cnt']]
    df_filtered_1['ROAS'] = df_filtered_1['통합_주문 금액'] / df_filtered_1['cost_gross_sum'] * 100
    df_filtered_1['CVR']  = df_filtered_1['통합_주문 수'] / df_filtered_1['pseudo_session_id_cnt'] * 100
    df_filtered_1 = df_filtered_1[['event_date_str', '통합_주문 금액', 'cost_gross_sum', 'ROAS', '통합_주문당 금액', '통합_주문 수', 'pseudo_session_id_cnt', 'CVR' ]]
    # st.dataframe(df_filtered_1)


    df_filtered_2 = df_filtered[['event_date_str', '슬립퍼_주문 금액', 'cost_gross_sum', '슬립퍼_주문당 금액', '슬립퍼_주문 수', 'pseudo_session_id_cnt' ]]
    df_filtered_2['ROAS'] = df_filtered_2['슬립퍼_주문 금액'] / df_filtered_2['cost_gross_sum'] * 100
    df_filtered_2['CVR']  = df_filtered_2['슬립퍼_주문 수'] / df_filtered_2['pseudo_session_id_cnt'] * 100
    df_filtered_2 = df_filtered_2[['event_date_str', '슬립퍼_주문 금액', 'cost_gross_sum', 'ROAS', '슬립퍼_주문당 금액', '슬립퍼_주문 수', 'pseudo_session_id_cnt', 'CVR' ]]
    # st.dataframe(df_filtered_2)
    

    df_filtered_3 = df_filtered[['event_date_str', '누어_주문 금액', 'cost_gross_sum', '누어_주문당 금액', '누어_주문 수', 'pseudo_session_id_cnt']]
    df_filtered_3['ROAS'] = df_filtered_3['누어_주문 금액'] / df_filtered_3['cost_gross_sum'] * 100
    df_filtered_3['CVR']  = df_filtered_3['누어_주문 수'] / df_filtered_3['pseudo_session_id_cnt'] * 100
    df_filtered_3 = df_filtered_3[['event_date_str', '누어_주문 금액', 'cost_gross_sum', 'ROAS', '누어_주문당 금액', '누어_주문 수', 'pseudo_session_id_cnt', 'CVR' ]]
    # st.dataframe(df_filtered_3)


    # 탭 구성
    st.markdown("<h5 style='margin:0'>통합 매출 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-")

    st.dataframe(df_filtered_1)

    
    # 탭 구성
    st.header("")
    st.markdown("<h5 style='margin:0'>슬립퍼 매출 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-")
    tabs = st.tabs(["슬립퍼", "슬립퍼-매트리스", "슬립퍼-프레임"]);
    with tabs[0]:
        st.dataframe(df_filtered_2)
    with tabs[1]:
        pass
    with tabs[2]:
        pass
    
    # 탭 구성
    st.header("")
    st.markdown("<h5 style='margin:0'>누어 매출 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ-")
    tabs = st.tabs(["누어", "누어-매트리스", "누어-프레임"]);
    with tabs[0]:
        st.dataframe(df_filtered_3)
    with tabs[1]:
        pass
    with tabs[2]:
        pass
    
if __name__ == "__main__":
    main()