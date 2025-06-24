import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import importlib
from datetime import datetime, timedelta

# — BigQuery 모듈 로드 & 클래스 가져오기 —
import bigquery
importlib.reload(bigquery)
from bigquery import BigQuery

# ──────────────────────────────────
# 스트림릿 페이지 설정 (반드시 최상단)
# ──────────────────────────────────
st.set_page_config(layout="wide", page_title="SLPR 유입 대시보드")
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
st.header("🛌 SLPR - 유입 분석 대시보드")

# ──────────────────────────────────
# 1. 캐시된 데이터 로더 (날짜 구간 바뀔 때만 재실행)
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
    df["_sourceMedium"] = df["psi_source"].astype(str) + " / " + df["psi_medium"].astype(str)
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
        df["psi_source"].isin(paid_sources)   | df["_sourceMedium"].isin(['youtube / demand_gen','kakako / crm']),
        df["psi_source"].isin(owned_sources)  | (df["_sourceMedium"]=='kakao / channel_message'),
        df["psi_source"].isin(earned_sources) | df["_sourceMedium"].isin(sms_referral),
    ]
    choices = ['ETC','Paid','Owned','Earned']
    df["isPaid_4"] = np.select(conds, choices, default='ETC')
    return df

# ──────────────────────────────────
# 2. 사이드바: 날짜 범위 선택
# ──────────────────────────────────
st.sidebar.header("🔎 날짜 범위 선택")
today         = datetime.now().date()
default_end   = today - timedelta(days=1)
default_start = today - timedelta(days=14)

start_date, end_date = st.sidebar.date_input(
    "📅 기간",
    value=[default_start, default_end]
)
cs = start_date.strftime("%Y%m%d")
ce = end_date.strftime("%Y%m%d")

# ──────────────────────────────────
# 3. 데이터 로딩 & 캐시
# ──────────────────────────────────
with st.spinner("데이터 로딩 중…"):
    df = load_data(cs, ce)
# st.success(f"데이터 로드 완료 ({len(df):,} 행)")

# ──────────────────────────────────
# 4. 사이드바: 추가 필터 (캐시된 df 에만 적용)
# ──────────────────────────────────
st.sidebar.header("🔎 추가 필터")
paid_opts   = ["전체"] + sorted(df["isPaid_4"].dropna().unique().tolist())
paid_filter = st.sidebar.selectbox("유입 유형", paid_opts)
medium_opts = ["전체"] + sorted(df["_sourceMedium"].dropna().unique().tolist())
medium_filter = st.sidebar.selectbox("소스/매체", medium_opts)
if st.sidebar.button("🔄 초기화"):
    st.experimental_rerun()

# ──────────────────────────────────
# 5. 캐시된 df 위에 필터 적용
# ──────────────────────────────────
df = df[
    (df["event_date"] >= pd.to_datetime(start_date)) &
    (df["event_date"] <= pd.to_datetime(end_date))
]
if paid_filter != "전체":
    df = df[df["isPaid_4"] == paid_filter]
if medium_filter != "전체":
    df = df[df["_sourceMedium"] == medium_filter]

# ──────────────────────────────────
# 6. 일별 집계
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

# ──────────────────────────────────
# 7. 레이아웃: 그래프(60%) & 테이블(40%)
# ──────────────────────────────────
col1, col2 = st.columns([6,4])

with col1:
    st.subheader("유입 추이")
    # 날짜 컬럼을 제외한 나머지를 y에 자동 할당
    y_cols = [c for c in df_daily.columns if c != "날짜"]
    fig = px.line(
        df_daily,
        x="날짜",
        y=y_cols,
        labels={"value": "수치", "variable": "지표"},
        markers=True
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("일자별 유입 수치")
    # 날짜 문자열 포맷
    df_daily["날짜"] = df_daily["날짜"].dt.strftime("%m월 %d일 (%a)")
    # 날짜 컬럼 외의 모든 컬럼을 자동으로 선택
    table_cols = [c for c in df_daily.columns if c != "날짜"]
    df_table = df_daily[["날짜"] + table_cols]
    # 합계 행 생성
    total = ["합계"] + df_table[table_cols].sum(numeric_only=True).astype(int).tolist()
    df_table = pd.concat(
        [df_table, pd.DataFrame([total], columns=df_table.columns)],
        ignore_index=True
    )
    st.dataframe(df_table, use_container_width=True, height=400)

# ──────────────────────────────────
# 8. 
# ──────────────────────────────────
# 날짜별로 각 세션 플래그 합계 집계
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

# 3분할 레이아웃
col_a, col_b, col_c = st.columns(3)

with col_a:
    st.subheader("PDP 조회 & 스크롤50 세션수")
    m1 = metrics_df.rename(columns={
        "_view_item_sessionCnt": "PDP조회_세션수",
        "_product_page_scroll_50_sessionCnt": "PDPscr50_세션수"
    })
    fig1 = px.line(
        m1,
        x="날짜",
        y=["PDP조회_세션수", "PDPscr50_세션수"],
        labels={"value":"세션수", "variable":"지표"},
        markers=True
    )
    st.plotly_chart(fig1, use_container_width=True)

with col_b:
    st.subheader("가격표시 / 쇼룸찾기 / 쇼룸10초 세션수")
    m2 = metrics_df.rename(columns={
        "_product_option_price_sessionCnt": "가격표시_세션수",
        "_find_nearby_showroom_sessionCnt": "쇼룸찾기_세션수",
        "_showroom_10s_sessionCnt": "쇼룸10초_세션수"
    })
    fig2 = px.line(
        m2,
        x="날짜",
        y=["가격표시_세션수", "쇼룸찾기_세션수", "쇼룸10초_세션수"],
        labels={"value":"세션수", "variable":"지표"},
        markers=True
    )
    st.plotly_chart(fig2, use_container_width=True)

with col_c:
    st.subheader("장바구니 / 쇼룸예약 세션수")
    m3 = metrics_df.rename(columns={
        "_add_to_cart_sessionCnt": "장바구니_세션수",
        "_showroom_leads_sessionCnt": "쇼룸예약_세션수"
    })
    fig3 = px.line(
        m3,
        x="날짜",
        y=["장바구니_세션수", "쇼룸예약_세션수"],
        labels={"value":"세션수", "variable":"지표"},
        markers=True
    )
    st.plotly_chart(fig3, use_container_width=True)
    
    
# metrics_df 는 이미 groupby 후 sum 결과를 가지고 있으므로 그대로 사용
metrics_df_display = metrics_df.copy()
# 날짜 문자열 포맷
metrics_df_display["날짜"] = metrics_df_display["event_date"].dt.strftime("%m월 %d일 (%a)")
# 컬럼 순서: 날짜 먼저, 그다음 세션수 컬럼들
table_cols = ["날짜"] + [c for c in metrics_df_display.columns if c != "event_date" and c != "날짜"]
metrics_df_display = metrics_df_display[table_cols]

st.subheader("📋 일자별 세션수 테이블")
st.dataframe(metrics_df_display, use_container_width=True, height=400)

# ──────────────────────────────────
# 12. 유입 현황: 상위 4개 + 기타로 파이차트 (Series.append → concat 수정)
# ──────────────────────────────────
import pandas as pd  # pie chart 코드 바로 위에 위치

st.subheader("유입 현황")
col_paid, col_device, col_geo = st.columns(3)

# 광고유무
with col_paid:
    st.markdown("### 광고유무")
    vc = df["isPaid_4"].value_counts()
    top4 = vc.nlargest(4)
    others = vc.iloc[4:].sum()
    pie_data = pd.concat([top4, pd.Series({"기타": others})]).reset_index()
    pie_data.columns = ["isPaid_4", "count"]
    fig_paid = px.pie(pie_data, names="isPaid_4", values="count", hole=0.4)
    st.plotly_chart(fig_paid, use_container_width=True)

# 디바이스
with col_device:
    st.markdown("### 디바이스")
    vc = df["device__category"].value_counts()
    top4 = vc.nlargest(4)
    others = vc.iloc[4:].sum()
    pie_data = pd.concat([top4, pd.Series({"기타": others})]).reset_index()
    pie_data.columns = ["device__category", "count"]
    fig_device = px.pie(pie_data, names="device__category", values="count", hole=0.4)
    st.plotly_chart(fig_device, use_container_width=True)

# 접속지역
with col_geo:
    st.markdown("### 접속지역")
    vc = df["geo__city"].value_counts()
    top4 = vc.nlargest(4)
    others = vc.iloc[4:].sum()
    pie_data = pd.concat([top4, pd.Series({"기타": others})]).reset_index()
    pie_data.columns = ["geo__city", "count"]
    fig_geo = px.pie(pie_data, names="geo__city", values="count", hole=0.4)
    st.plotly_chart(fig_geo, use_container_width=True)
