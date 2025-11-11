# 서희_최신수정일_25-08-19

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
import re
import sys
import modules.style
importlib.reload(sys.modules['modules.style'])
from modules.style import style_format, style_cmap
from zoneinfo import ZoneInfo

def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정
    # ──────────────────────────────────

    st.markdown(
        """
        <style>
            /* 전체 컨테이너의 패딩 조정 */
            .block-container {
                max-width: 100% !important;
                padding-top: 1rem;   /* 위쪽 여백 */
                padding-bottom: 8rem;
                padding-left: 5rem; 
                padding-right: 4rem; 
            }
        </style>
        """,
        unsafe_allow_html=True
    )  
    
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)

    # ────────────────────────────────────────────────────────────────
    # 사이드바 필터 설정
    # ────────────────────────────────────────────────────────────────
    st.sidebar.header("Filter")
    
    today = datetime.now().date()
    default_end = today - timedelta(days=1)
    default_start = today - timedelta(days=7)
    start_date, end_date = st.sidebar.date_input(
        "기간 선택",
        value=[default_start, default_end],
        max_value=default_end
    )
    cs = start_date.strftime("%Y%m%d")
    # ce = end_date.strftime("%Y%m%d")
    ce_exclusive = (end_date + timedelta(days=1)).strftime("%Y%m%d")


    @st.cache_data(ttl=3600)
    def load_data(cs: str, ce: str) -> pd.DataFrame:
        
        # tb_sleeper_psi
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df_psi = bq.get_data("tb_sleeper_psi")
        
        last_updated_time = df_psi["event_date"].max()
        
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
            df["isPaid_4"]    = categorize_paid(df)
            df["_geo_region"] = categorize_region(df)

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

        def categorize_region(
            df: pd.DataFrame,
            city_col: str = "geo__city",
            region_col: str = "geo__region",
            default_region: str = "기타"
        ) -> pd.Series:
            """
            1) geo__city 기준으로 매핑
            2) city 결과가 NaN 또는 '기타'일 때만 geo__region으로 보완
            3) 그래도 없으면 default_region
            """
            # 0) 컬럼 없으면 전부 기본값
            if city_col not in df.columns and region_col not in df.columns:
                return pd.Series([default_region] * len(df), index=df.index, name="_geo_region")

            # 1) 표준화
            def _norm(x):
                if pd.isna(x):
                    return None
                s = str(x).strip()
                s = re.sub(r"\s+", " ", s)
                return s.lower()

            # 2) city 매핑 (세부 시/군 위주)
            norm_map_city = {
                # 수도권(경기 광주시 주의: 'gwangju-si'는 수도권)
                "seoul": "수도권",
                "incheon": "수도권",
                "goyang-si": "수도권",
                "suwon-si": "수도권",
                "yongin-si": "수도권",
                "hwaseong-si": "수도권",
                "seongnam-si": "수도권",
                "gimpo-si": "수도권",
                "paju-si": "수도권",
                "pyeongtaek-si": "수도권",
                "namyangju-si": "수도권",
                "bucheon-si": "수도권",
                "anyang-si": "수도권",
                "ansan-si": "수도권",
                "siheung-si": "수도권",
                "uijeongbu-si": "수도권",
                "gwangmyeong-si": "수도권",
                "icheon-si": "수도권",
                "gunpo-si": "수도권",
                "guri-si": "수도권",
                "anseong-si": "수도권",
                "uiwang-si": "수도권",
                "pocheon-si": "수도권",
                "yeoju-si": "수도권",
                "dongducheon-si": "수도권",
                "gwacheon-si": "수도권",
                "gwangju-si": "수도권",  # 경기 광주시

                # 광주/전라(광주광역시는 'gwangju')
                "gwangju": "광주/전라",
                "jeonju-si": "광주/전라",
                "suncheon-si": "광주/전라",
                "gunsan-si": "광주/전라",
                "iksan-si": "광주/전라",
                "yeosu-si": "광주/전라",
                "mokpo-si": "광주/전라",
                "jeongeup-si": "광주/전라",
                "namwon-si": "광주/전라",
                "naju-si": "광주/전라",

                # 대구/경북
                "daegu": "대구/경북",
                "pohang-si": "대구/경북",
                "gumi-si": "대구/경북",
                "gyeongsan-si": "대구/경북",
                "gyeongju-si": "대구/경북",
                "andong-si": "대구/경북",
                "gimcheon-si": "대구/경북",
                "yeongju-si": "대구/경북",
                "yeongcheon-si": "대구/경북",
                "sangju-si": "대구/경북",
                "mungyeong-si": "대구/경북",

                # 부산/경남(울산 포함)
                "busan": "부산/경남",
                "changwon-si": "부산/경남",
                "ulsan": "부산/경남",
                "gimhae-si": "부산/경남",
                "jinju-si": "부산/경남",
                "yangsan-si": "부산/경남",
                "geoje-si": "부산/경남",
                "tongyeong-si": "부산/경남",
                "sacheon-si": "부산/경남",
                "miryang-si": "부산/경남",

                # 대전/중부(충청권+세종)
                "daejeon": "대전/중부",
                "cheonan-si": "대전/중부",
                "cheongju-si": "대전/중부",
                "sejong-si": "대전/중부",
                "asan-si": "대전/중부",
                "chungju-si": "대전/중부",
                "dangjin-si": "대전/중부",
                "seosan-si": "대전/중부",
                "nonsan-si": "대전/중부",
                "gongju-si": "대전/중부",
                "jecheon-si": "대전/중부",
                "boryeong-si": "대전/중부",
                "gyeryong-si": "대전/중부",

                # 기타(강원/제주 등)
                "jeju-si": "기타",
                "seogwipo-si": "기타",
                "wonju-si": "기타",
                "gangneung-si": "기타",
                "sokcho-si": "기타",
                "donghae-si": "기타",
                "samcheok-si": "기타",
                "chuncheon-si": "기타",
                "taebaek-si": "기타",

                # 예외값
                "(not set)": "기타",
                "not set": "기타",
            }

            # 3) region 매핑 (도/광역시 등 상위뎁스)
            norm_map_region = {
                "seoul": "수도권",
                "gyeonggi-do": "수도권",
                "incheon": "수도권",
                "chungcheongnam-do": "대전/중부",
                "chungcheongbuk-do": "대전/중부",
                "daejeon": "대전/중부",
                "sejong-si": "대전/중부",
                "jeju-do": "기타",
                "gangwon-do": "기타",
                "gwangju": "광주/전라",
                "jeonbuk state": "광주/전라",
                "jeollanam-do": "광주/전라",
                "busan": "부산/경남",
                "gyeongsangnam-do": "부산/경남",
                "ulsan": "부산/경남",
                "daegu": "대구/경북",
                "gyeongsangbuk-do": "대구/경북",
            }

            # 4) 매핑 적용
            city_norm = df[city_col].apply(_norm) if city_col in df.columns else pd.Series(index=df.index, dtype=object)
            city_mapped = city_norm.map(norm_map_city)

            region_norm = df[region_col].apply(_norm) if region_col in df.columns else pd.Series(index=df.index, dtype=object)
            region_mapped = region_norm.map(norm_map_region)

            # 5) city 결과가 비거나 '기타'인 곳만 region으로 보완
            res = city_mapped.copy()
            mask = res.isna() | (res == default_region)
            res.loc[mask] = region_mapped.loc[mask]

            # 6) 최종 기본값 채움
            res = res.fillna(default_region)
            return pd.Series(res, index=df.index, name="_geo_region")

        
        return preprocess_data(df_psi), last_updated_time


    # ────────────────────────────────────────────────────────────────
    # 데이터 불러오기
    # ────────────────────────────────────────────────────────────────
    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df_psi, last_updated_time = load_data(cs, ce_exclusive) # 전처리된 df_psi

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


    def summary_row(df):
        # 숫자형 컬럼만 자동 추출
        num_cols = df.select_dtypes(include="number").columns
        sum_row = df[num_cols].sum().to_frame().T
        sum_row['날짜'] = "합계"
        mean_row = df[num_cols].mean().to_frame().T
        mean_row['날짜'] = "평균"
        df = pd.concat([df, sum_row, mean_row], ignore_index=True)

        return df     


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


    def render_stacked_bar(df: pd.DataFrame, x: str, y: str | list[str], color: str | None) -> None:
        # 숫자형 보정
        def _to_numeric(cols):
            for c in cols:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        if isinstance(y, (list, tuple)):   # wide-form 들어오면
            _to_numeric(list(y))
            if color is not None and color in df.columns:
                # y-list + color가 같이 오면 long으로 변환해 확실히 누적
                long_df = df.melt(id_vars=[x, color], value_vars=list(y),
                                var_name="__series__", value_name="__value__")
                fig = px.bar(long_df, x=x, y="__value__", color="__series__", opacity=0.6)
            else:
                fig = px.bar(df, x=x, y=list(y), opacity=0.6)
        else:                               # y가 단일이면 long-form
            _to_numeric([y])
            fig = px.bar(df, x=x, y=y, color=color, opacity=0.6)

        # 핵심: 진짜로 누적시키기
        fig.update_layout(barmode="relative")
        fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))

        fig.update_layout(
            bargap=0.1,
            bargroupgap=0.2,
            height=450,
            xaxis_title=None,
            yaxis_title=None,
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom", title=None),
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

    # 합계 & 평균 행 추가 (단, num_cols 비정의되어 있을때)



    # 데이터프레임 생성
    df_daily         =  pivot_daily(df_psi)                                       
    df_daily_paid    =  pivot_daily(df_psi, group_cols=["isPaid_4"])                              
    df_daily_device  =  pivot_daily(df_psi, group_cols=["device__category"])
    df_daily_geo     =  pivot_daily(df_psi, group_cols=["geo__city"],          top_n=6,   기타_label="기타")
    df_daily_source  =  pivot_daily(df_psi, group_cols=["_sourceMedium"],      top_n=20,   기타_label="기타")
    df_daily_region  =  pivot_daily(df_psi, group_cols=["_geo_region"])

    # 데이터프레임 별 -> 컬럼명 한글 치환
    df_daily_paid   = df_daily_paid.rename(columns={"isPaid_4":           "광고유무"})
    df_daily_device = df_daily_device.rename(columns={"device__category":   "디바이스"})
    df_daily_geo    = df_daily_geo.rename(columns={"geo__city":           "접속지역"})
    df_daily_source = df_daily_source.rename(columns={"_sourceMedium":       "유입매체"})
    df_daily_region = df_daily_region.rename(columns={"_geo_region":       "접속권역"})
    
    
    
    
    
    # (25.11.10) 제목 + 설명 + 업데이트 시각 + 캐시초기화 
    # 제목
    st.subheader("GA 트래픽 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()   # 파라미터 제거
        st.rerun()
        
    # 설명
    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="      
                font-size:14px;       
                line-height:1.5;      
            ">
            <b>자사몰 트래픽</b>의 방문 유형, 광고 유무, 접속 지역, 주요 이벤트 세션수 등을 
            한눈에 보여주는 <b>GA 대시보드</b>입니다.<br>
            </div>
            <div style="
                color:#6c757d;        
                font-size:14px;       
                line-height:2.0;      
            ">
            ※ GA D-1 데이터의 세션 수치는 <b>오전에 1차</b> 집계되나 , 세션의 유입출처는 <b>오후에 2차</b> 반영됩니다.
            </div>
            """,
            unsafe_allow_html=True
        )
        
    with col2:
        if isinstance(last_updated_time, str):
            latest_dt = datetime.strptime(last_updated_time, "%Y%m%d")
        else:
            latest_dt = last_updated_time  # Timestamp/datetime 가정
        latest_date = latest_dt.date()
        
        now_kst   = datetime.now(ZoneInfo("Asia/Seoul"))
        today_kst = now_kst.date()
        delta_days = (today_kst - latest_date).days
        hm_ref = now_kst.hour * 100 + now_kst.minute  # ex) 1535
        
        msg    = "집계 예정 (AM 08:50 / PM 15:35)"
        sub_bg = "#f8fafc"
        sub_bd = "#e2e8f0"
        sub_fg = "#475569"

        # 1) D-2 이상 지연 → 경고(빨강)
        if delta_days >= 2:
            msg    = f"업데이트가 지연되고 있습니다"
            subtag = "캐시 초기화"
            # 경고 팔레트 (red)
            sub_bg = "#fef2f2"
            sub_bd = "#fee2e2"
            sub_fg = "#b91c1c"

        elif delta_days == 1:
            # last_updated_time 이 datetime/timestamp면 그 시각으로, 아니면 현재 시각으로 판정
            if hm_ref >= 1535:
                msg = "2차 업데이트 완료 (PM 15:35)"
                # 보라톤
                sub_bg = "#fff7ed"
                sub_bd = "#fdba74"
                sub_fg = "#c2410c"
            elif hm_ref >= 850:
                msg = "1차 업데이트 완료 (AM 08:50)"
            else:
                pass

        # 배지 + 캐시초기화(링크) — 높이 동일화
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
            <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 8px;
                font-size:13px;line-height:1;
                color:{sub_fg};background:{sub_bg};border:1px solid {sub_bd};
                border-radius:10px;white-space:nowrap;">
                🔔 {msg}
            </span>
            <a href="?refresh=1" title="캐시 초기화" style="text-decoration:none;vertical-align:middle;">
                <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 8px;
                font-size:13px;line-height:1;
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
    # 1) 방문 추이
    # ──────────────────────────────────
    # 탭 간격 CSS
    st.markdown("""
        <style>
          [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown("<h5 style='margin:0'>방문 추이</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ일자별 **방문수**(세션 기준), **유저수**(중복 제거), **신규 및 재방문수** 추이를 확인할 수 있습니다.")
    # — 시각화
    c1, _p, c2 = st.columns([5.0, 0.2, 3.8])
    with c1:
        y_cols = [c for c in df_daily.columns if c not in "날짜"]
        render_line_chart(df_daily, x="날짜", y=y_cols)
    with _p: pass
    with c2:
        styled = style_format(
            summary_row(df_daily),
            decimals_map={
                ("방문수"): 0,
                ("유저수"): 0,
                ("신규방문수"): 0,
                ("재방문수"): 0,
            },
        )
        styled2 = style_cmap(
            styled,
            gradient_rules=[
                {"col": "방문수", "cmap":"OrRd", "low":0.0, "high":0.3},
            ]
        )
        st.dataframe(styled2, row_height=30,  hide_index=True)


    # ──────────────────────────────────
    # 2) 주요 방문 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>방문 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ**광고유무/디바이스/접속지역/유입매체**별 방문 추이를 확인할 수 있습니다.")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["광고유무", "디바이스", "접속지역", "접속권역", "유입매체"])
    
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
            styled = style_format(
                df_paid_tab,
                decimals_map={
                    ("방문수"): 0,
                    ("유저수"): 0,
                    ("신규방문수"): 0,
                    ("재방문수"): 0,
                },
            )
            styled2 = style_cmap(
                styled,
                gradient_rules=[
                    {"col": "방문수", "cmap":"OrRd",  "low":0.0, "high":0.3},
                ]
            )
            st.dataframe(styled2,  row_height=30,  hide_index=True)
    
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
            styled = style_format(
                df_dev_tab,
                decimals_map={
                    ("방문수"): 0,
                    ("유저수"): 0,
                    ("신규방문수"): 0,
                    ("재방문수"): 0,
                },
            )
            styled2 = style_cmap(
                styled,
                gradient_rules=[
                    {"col": "방문수", "cmap":"OrRd",  "low":0.0, "high":0.3},
                ]
            )
            st.dataframe(styled2,  row_height=30,  hide_index=True)
    
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
            styled = style_format(
                df_geo_tab,
                decimals_map={
                    ("방문수"): 0,
                    ("유저수"): 0,
                    ("신규방문수"): 0,
                    ("재방문수"): 0,
                },
            )
            styled2 = style_cmap(
                styled,
                gradient_rules=[
                    {"col": "방문수", "cmap":"OrRd", "low":0.0, "high":0.3},
                ]
            )
            st.dataframe(styled2,  row_height=30,  hide_index=True)
    
    # - 접속권역 탭
    with tab4: 
        region_options = ["전체"] + sorted(df_psi["_geo_region"].dropna().unique().tolist())
        sel_region = st.selectbox("접속권역 선택", region_options, index=0)
        if sel_region == "전체":
            df_region_tab = df_daily_region.copy()
        else:
            df_region_tab = df_daily_region[df_daily_region["접속권역"] == sel_region]
        c1, _p, c2 = st.columns([5.0, 0.2, 3.8])
        with c1:
            render_stacked_bar(df_region_tab, x="날짜", y="방문수", color="접속권역")
        with _p: pass
        with c2:
            styled = style_format(
                df_region_tab,
                decimals_map={
                    ("방문수"): 0,
                    ("유저수"): 0,
                    ("신규방문수"): 0,
                    ("재방문수"): 0,
                },
            )
            styled2 = style_cmap(
                styled,
                gradient_rules=[
                    {"col": "방문수", "cmap":"OrRd",  "low":0.0, "high":0.3},
                ]
            )
            st.dataframe(styled2,  row_height=30,  hide_index=True)
        # st.dataframe(df_psi[['geo__city', 'geo__region', '_geo_region' ]])
    
            
    # — 유입매체 탭
    with tab5:
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
            styled = style_format(
                df_source_tab,
                decimals_map={
                    ("방문수"): 0,
                    ("유저수"): 0,
                    ("신규방문수"): 0,
                    ("재방문수"): 0,
                },
            )
            styled2 = style_cmap(
                styled,
                gradient_rules=[
                    {"col": "방문수", "cmap":"OrRd", "low":0.0, "high":0.3},
                ]
            )
            st.dataframe(styled2,  row_height=30,  hide_index=True)


    # # ──────────────────────────────────
    # # 3) 주요 이벤트 현황
    # # ──────────────────────────────────
    # st.header(" ")
    # st.markdown("<h5 style='margin:0'>이벤트 현황</h5>", unsafe_allow_html=True)
    # st.markdown(":gray-badge[:material/Info: Info]ㅤ**PDP 조회**부터 **쇼룸 예약**에 대한 세션 추이를 확인할 수 있습니다.")

    # # 매핑 명칭 일괄 선언
    # col_map = {
    #     "_view_item_sessionCnt":             "PDP조회_세션수",
    #     "_product_page_scroll_50_sessionCnt":"PDPscr50_세션수",
    #     "_product_option_price_sessionCnt":  "가격표시_세션수",
    #     "_find_nearby_showroom_sessionCnt":  "쇼룸찾기_세션수",
    #     "_showroom_10s_sessionCnt":          "쇼룸10초_세션수",
    #     "_add_to_cart_sessionCnt":           "장바구니_세션수",
    #     "_showroom_leads_sessionCnt":        "쇼룸예약_세션수",
    # }

    # # metrics_df
    # metrics_df = (
    #     df_psi
    #     .groupby("event_date", as_index=False)
    #     .agg(**{ new_name: (orig_name, "sum")
    #             for orig_name, new_name in col_map.items() })
    # )
    # # 날짜 형식 변경, event_date Drop
    # metrics_df["날짜"] = metrics_df["event_date"].dt.strftime("%Y-%m-%d")
    # metrics_df = metrics_df.drop(columns=["event_date"])
    # metrics_df = metrics_df[['날짜','PDP조회_세션수','PDPscr50_세션수','가격표시_세션수','쇼룸찾기_세션수','쇼룸10초_세션수','장바구니_세션수','쇼룸예약_세션수']]

    # # — 제품탐색
    # col_a, col_b, col_c = st.columns(3)
    # with col_a:
    #     y_cols = ["PDP조회_세션수","PDPscr50_세션수"]
    #     render_line_chart(metrics_df, x="날짜", y=y_cols, title="🔍 제품탐색")
        
    # # — 관심표현
    # with col_b:
    #     y_cols = ["가격표시_세션수","쇼룸찾기_세션수","쇼룸10초_세션수"]
    #     render_line_chart(metrics_df, x="날짜", y=y_cols, title="❤️ 관심표현")

    # # — 전환의도
    # with col_c:
    #     y_cols = ["장바구니_세션수","쇼룸예약_세션수"]
    #     render_line_chart(metrics_df, x="날짜", y=y_cols, title="🛒 전환의도")

    # styled = style_format(
    #     summary_row(metrics_df),
    #     decimals_map={
    #         ("PDP조회_세션수"): 0,
    #         ("PDPscr50_세션수"): 0,
    #         ("가격표시_세션수"): 0,
    #         ("쇼룸찾기_세션수"): 0,
    #         ("쇼룸10초_세션수"): 0,
    #         ("장바구니_세션수"): 0,
    #         ("쇼룸예약_세션수"): 0,
    #     },
    # )
    # # styled2 = style_cmap(
    # #     styled,
    # #     gradient_rules=[
    # #         {"col": "쇼룸찾기_세션수", "cmap":"OrRd",  "low":0.0, "high":0.3},
    # #         {"col": "쇼룸10초_세션수", "cmap":"OrRd",  "low":0.0, "high":0.3},
    # #         {"col": "쇼룸예약_세션수", "cmap":"OrRd",  "low":0.0, "high":0.3},
    # #     ]
    # # )
    # st.dataframe(styled,  row_height=30,  hide_index=True)



    # ──────────────────────────────────
    # 3) 주요 이벤트 현황
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>이벤트 현황</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ**PDP 조회**부터 **쇼룸 예약**에 대한 세션 추이를 확인할 수 있습니다.")

    # 매핑 명칭 일괄 선언
    col_map = {
        "_view_item_sessionCnt":              "PDP조회_세션수",
        "_product_page_scroll_50_sessionCnt": "PDPscr50_세션수",
        "_product_option_price_sessionCnt":   "가격표시_세션수",
        "_find_nearby_showroom_sessionCnt":   "쇼룸찾기_세션수",
        "_showroom_10s_sessionCnt":           "쇼룸10초_세션수",
        "_add_to_cart_sessionCnt":            "장바구니_세션수",
        "_showroom_leads_sessionCnt":         "쇼룸예약_세션수",
    }

    # [추가] ----------------------------------------------------------
    SRC_COL = "collected_traffic_source__manual_source"
    MDM_COL = "collected_traffic_source__manual_medium"
    CMP_COL = "collected_traffic_source__manual_campaign_name"
    CON_COL = "collected_traffic_source__manual_content"

    # 결측 안전 처리
    df_psi[SRC_COL] = df_psi[SRC_COL].fillna("(not set)") if SRC_COL in df_psi.columns else "(not set)"
    df_psi[MDM_COL] = df_psi[MDM_COL].fillna("(not set)") if MDM_COL in df_psi.columns else "(not set)"
    df_psi[CMP_COL] = df_psi[CMP_COL].fillna("(not set)") if CMP_COL in df_psi.columns else "(not set)"
    df_psi[CON_COL] = df_psi[CON_COL].fillna("(not set)") if CON_COL in df_psi.columns else "(not set)"

    # ✅ (이동) 추가 피벗을 '매체 필터'보다 먼저 배치
    pivot_map = {
        "소스": SRC_COL,
        "매체": MDM_COL,
        "캠페인": CMP_COL,
        "컨텐츠": CON_COL,
    }

    # 여기서는 '선택'만 받고, 실제 그룹 계산은 필터 적용 후에 합니다.

    # ── 매체 필터 (그 다음에 위치)
    with st.expander("매체 필터", expanded=False):
        sel_pivots = st.multiselect("행 필드 추가 선택", list(pivot_map.keys()), default=[])
        
        c1, c2, c3, c4 = st.columns([1,1,1,1])

        with c1:
            src_counts = df_psi[SRC_COL].astype(str).value_counts(dropna=False).sort_values(ascending=False)
            src_options = src_counts.index.tolist()
            sel_sources = st.multiselect("소스 선택 (다중)", options=src_options, default=[])

        with c2:
            mdm_counts = df_psi[MDM_COL].astype(str).value_counts(dropna=False).sort_values(ascending=False)
            mdm_options = mdm_counts.index.tolist()
            sel_mediums = st.multiselect("미디엄 선택 (다중)", options=mdm_options, default=[])

        with c3:
            cmp_counts = df_psi[CMP_COL].astype(str).value_counts(dropna=False).sort_values(ascending=False)
            cmp_options = cmp_counts.index.tolist()
            sel_campaigns = st.multiselect("캠페인 선택 (다중)", options=cmp_options, default=[])

        with c4:
            con_counts = df_psi[CON_COL].astype(str).value_counts(dropna=False).sort_values(ascending=False)
            con_options = con_counts.index.tolist()
            sel_contents = st.multiselect("컨텐츠 선택 (다중)", options=con_options, default=[])

    # 필터 적용
    df_psi_f = df_psi.copy()
    if sel_sources:   df_psi_f = df_psi_f[df_psi_f[SRC_COL].isin(sel_sources)]
    if sel_mediums:   df_psi_f = df_psi_f[df_psi_f[MDM_COL].isin(sel_mediums)]
    if sel_campaigns: df_psi_f = df_psi_f[df_psi_f[CMP_COL].isin(sel_campaigns)]
    if sel_contents:  df_psi_f = df_psi_f[df_psi_f[CON_COL].isin(sel_contents)]

    # ✅ (유지) 여기서 선택된 피벗으로 그룹 구성 & 차트 그룹 1개 선택
    grp_cols = [pivot_map[k] for k in sel_pivots] if sel_pivots else []
    
    # 차트는 항상 전체(필터만 반영), 추가 피벗은 '표'에만 반영
    df_for_chart = df_psi_f

    # -------------------------------------------------------------------
    

    # ── 차트용 집계: 날짜 기준(그룹 1개 선택 반영)
    metrics_df = (
        df_for_chart
        .groupby("event_date", as_index=False)
        .agg(**{ new_name: (orig_name, "sum") for orig_name, new_name in col_map.items() })
    )
    metrics_df["날짜"] = metrics_df["event_date"].dt.strftime("%Y-%m-%d")
    metrics_df = metrics_df.drop(columns=["event_date"])
    metrics_df = metrics_df[['날짜','PDP조회_세션수','PDPscr50_세션수','가격표시_세션수','쇼룸찾기_세션수','쇼룸10초_세션수','장바구니_세션수','쇼룸예약_세션수']]

    # — 제품탐색
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        y_cols = ["PDP조회_세션수","PDPscr50_세션수"]
        render_line_chart(metrics_df, x="날짜", y=y_cols, title="🔍 제품탐색")
    with col_b:
        y_cols = ["가격표시_세션수","쇼룸찾기_세션수","쇼룸10초_세션수"]
        render_line_chart(metrics_df, x="날짜", y=y_cols, title="❤️ 관심표현")
    with col_c:
        y_cols = ["장바구니_세션수","쇼룸예약_세션수"]
        render_line_chart(metrics_df, x="날짜", y=y_cols, title="🛒 전환의도")

    # ── 표용 집계: 날짜 + 선택 피벗(grp_cols) 반영
    groupby_cols = ["event_date"] + grp_cols   # ← 날짜 + 추가 피벗
    metrics_tbl = (
        df_psi_f
        .groupby(groupby_cols, as_index=False)
        .agg(**{ new_name: (orig_name, "sum") for orig_name, new_name in col_map.items() })
    )

    # 날짜 포맷
    metrics_tbl["날짜"] = metrics_tbl["event_date"].dt.strftime("%Y-%m-%d")
    metrics_tbl = metrics_tbl.drop(columns=["event_date"])

    # 보기 좋게 한글 컬럼명으로 바꿔서 표에 노출
    rename_map = {
        SRC_COL: "소스",
        MDM_COL: "매체",
        CMP_COL: "캠페인",
        CON_COL: "컨텐츠",
    }
    metrics_tbl = metrics_tbl.rename(columns=rename_map)

    # 표 컬럼 순서: (선택된 피벗들) + 날짜 + 지표들
    pivot_display_cols = [rename_map[c] for c in grp_cols if c in rename_map]  # 선택된 것만
    base_cols = ['날짜','PDP조회_세션수','PDPscr50_세션수','가격표시_세션수','쇼룸찾기_세션수','쇼룸10초_세션수','장바구니_세션수','쇼룸예약_세션수']
    table_cols = pivot_display_cols + base_cols
    metrics_tbl = metrics_tbl[table_cols].sort_values(pivot_display_cols + ['날짜'] if pivot_display_cols else ['날짜'])

    styled = style_format(
        summary_row(metrics_tbl),
        decimals_map={
            "PDP조회_세션수": 0,
            "PDPscr50_세션수": 0,
            "가격표시_세션수": 0,
            "쇼룸찾기_세션수": 0,
            "쇼룸10초_세션수": 0,
            "장바구니_세션수": 0,
            "쇼룸예약_세션수": 0,
        },
    )

    # # 상태 캡션
    # st.caption(
    #     "[선택된 데이터] "
    #     f"source={', '.join(sel_sources) if sel_sources else '전체'} / "
    #     f"medium={', '.join(sel_mediums) if sel_mediums else '전체'} / "
    #     f"campaign={', '.join(sel_campaigns) if sel_campaigns else '전체'} / "
    #     f"content={', '.join(sel_contents) if sel_contents else '전체'}"
    # )

    st.dataframe(styled, row_height=30, hide_index=True)



if __name__ == "__main__":
    main()
