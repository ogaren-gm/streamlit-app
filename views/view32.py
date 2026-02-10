# SEOHEE
# 2026-02-10 ver.

import streamlit as st
import pandas as pd
import numpy as np
import importlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery

import sys
import modules.style
importlib.reload(sys.modules["modules.style"])
from modules.style import style_format, style_cmap

from google.oauth2.service_account import Credentials
import gspread


# ──────────────────────────────────
# CONFIG
# ──────────────────────────────────
CFG = {
    # 기본
    "TZ": "Asia/Seoul",
    "CACHE_TTL": 3600,
    "DEFAULT_LOOKBACK_DAYS": 14,
    "HEADER_UPDATE_AM": 850,
    "HEADER_UPDATE_PM": 1535,

    "TOPK_OPTS": [5, 10, 15, 20],

    "CSS_BLOCK_CONTAINER": """
        <style>
            .block-container {
                max-width: 100% !important;
                padding-top: 1rem;
                padding-bottom: 8rem;
                padding-left: 5rem;
                padding-right: 4rem;
            }
        </style>
    """,
    "CSS_TABS": """
        <style>
            [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """,
}


# ──────────────────────────────────
# CONSTANTS
# ──────────────────────────────────
HEADER_MAP = {
    "event_date": "날짜",
    "media_name": "매체",
    "utm_source": "소스",
    "utm_medium": "미디엄",
    "brand_type": "브랜드",
    "funnel_type": "퍼널",
    "product_type": "품목",
    "campaign_name": "캠페인",
    "adgroup_name": "광고그룹",
    "ad_name": "광고소재",
    "keyword_name": "키워드",
    "utm_content": "컨텐츠",
    "utm_term": "검색어",
}

AGG_MAP = dict(
    cost_sum=("cost", "sum"),
    cost_gross_sum=("cost_gross", "sum"),
    impressions_sum=("impressions", "sum"),
    clicks_sum=("clicks", "sum"),
    view_item_sum=("view_item", "sum"),
    product_page_scroll_50_sum=("product_page_scroll_50", "sum"),
    product_option_price_sum=("product_option_price", "sum"),
    find_nearby_showroom_sum=("find_nearby_showroom", "sum"),
    showroom_10s_sum=("showroom_10s", "sum"),
    add_to_cart_sum=("add_to_cart", "sum"),
    showroom_leads_sum=("showroom_leads", "sum"),
    purchase_sum=("purchase", "sum"),
    session_count=("session_start", "sum"),
    engagement_time_msec_sum=("engagement_time_msec_sum", "sum"),
)

# ──────────────────────────────────
# main
# ──────────────────────────────────
def main():
    # ──────────────────────────────────
    # A) Layout / CSS
    # ──────────────────────────────────
    st.markdown(CFG["CSS_BLOCK_CONTAINER"], unsafe_allow_html=True)
    st.markdown(CFG["CSS_TABS"], unsafe_allow_html=True)

    # ────────────────────────────────────────────────────────────────
    # B) Sidebar (기간/비교기간)
    # ────────────────────────────────────────────────────────────────
    # 기간
    st.sidebar.header("Filter")
    today = datetime.now().date()
    default_end = today - timedelta(days=1)
    default_start = today - timedelta(days=CFG["DEFAULT_LOOKBACK_DAYS"])

    start_date, end_date = st.sidebar.date_input(
        "기간 선택",
        value=[default_start, default_end],
        max_value=default_end,
    )
    cs = start_date.strftime("%Y%m%d")
    ce = end_date.strftime("%Y%m%d")
    
    # 비교기간
    use_compare = st.sidebar.checkbox("비교기간 사용")
    period_len = (end_date - start_date).days + 1
    default_comp_e = start_date - timedelta(days=1)
    default_comp_s = default_comp_e - timedelta(days=period_len - 1)

    if use_compare:
        comp_start, comp_end = st.sidebar.date_input(
            "비교 기간 선택",
            value=[default_comp_s, default_comp_e],
            max_value=default_comp_e,
        )

    show_totals = st.sidebar.checkbox("기간별 합계 보기")

    # 기간 라벨 (표기형식)
    start_date_str = start_date.strftime("%m/%d")
    end_date_str = end_date.strftime("%m/%d")
    default_comp_s_str = default_comp_s.strftime("%m/%d")
    default_comp_e_str = default_comp_e.strftime("%m/%d")

    # ──────────────────────────────────
    # C) Data Load
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def load_data(cs: str, ce: str):
        # 1) tb_media
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)
        df_bq = bq.get_data("tb_media")
        df_bq["event_date"] = pd.to_datetime(df_bq["event_date"], format="%Y%m%d", errors="coerce")

        parts = df_bq["campaign_name"].astype(str).str.split("_", n=5, expand=True)
        df_bq["campaign_name_short"] = df_bq["campaign_name"]
        mask = parts[5].notna()
        df_bq.loc[mask, "campaign_name_short"] = (
            parts.loc[mask, :4].apply(lambda r: "_".join(r.dropna().astype(str)), axis=1)
        )

        # 2) Google Sheet
        # secrets가 dict/string 어떤 형태든 처리(원 코드 동작 유지)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive",] 
        try:
            creds = Credentials.from_service_account_file("C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope)
        except:
            sa_info = st.secrets["sleeper-462701-admin"] 
            if isinstance(sa_info, str):
                import json
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/11ov-_o6Lv5HcuZo1QxrKOZnLtnxEKTiV78OFBZzVmWA/edit")
        df_sheet = pd.DataFrame(sh.worksheet("parse").get_all_records())

        # 3) merge
        merged = df_bq.merge(df_sheet, how="left", on="campaign_name_short")

        # [ 하드코딩 전처리 ] cost_gross(v2) 
        merged["cost_gross"] = np.where(
            merged["event_date"] < pd.to_datetime("2025-11-06"),
            np.where(
                merged["media_name"].isin(["GOOGLE", "META"]),
                merged["cost"] * 1.1 / 0.98,
                merged["cost"],
            ),
            np.where(
                merged["media_name"].isin(["GOOGLE", "META"]),
                merged["cost"] * 1.1 / 0.955,
                merged["cost"],
            ),
        )

        # [ 하드코딩 전처리 ] handle NSA - 기존 로직 유지
        cond = (
            (merged["media_name"] == "NSA")
            & merged["utm_source"].isna()
            & merged["utm_medium"].isna()
            & merged["media_name_type"].isin(["RSA_AD", "TEXT_45"])
        )
        merged.loc[cond, ["utm_source", "utm_medium"]] = ["naver", "search-nonmatch"]

        # ⚠️ 원 코드와 동일: merged.event_date는 문자열 표기로 변환
        merged["event_date"] = merged["event_date"].dt.strftime("%Y-%m-%d")

        return merged

    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        if use_compare:
            cs_cmp = comp_start.strftime("%Y%m%d")
            df_merged = load_data(cs_cmp, ce)

            # 비교/선택 분리용 event_date를 datetime으로 복원(원 코드 유지)
            df_merged["event_date"] = pd.to_datetime(df_merged["event_date"], errors="coerce")

            df_primary = df_merged[
                (df_merged["event_date"] >= pd.to_datetime(start_date))
                & (df_merged["event_date"] <= pd.to_datetime(end_date))
            ]
            df_compare = df_merged[
                (df_merged["event_date"] >= pd.to_datetime(comp_start))
                & (df_merged["event_date"] <= pd.to_datetime(comp_end))
            ]

            df_filtered = df_primary
            df_filtered_cmp = df_compare
        else:
            df_merged = load_data(cs, ce)
            df_merged["event_date"] = pd.to_datetime(df_merged["event_date"], errors="coerce")
            df_filtered = df_merged
            df_filtered_cmp = None

        # apply_filter_pair에서 옵션은 항상 "선택기간 기준"을 쓰므로 원본 유지
        df_primary = df_filtered


    # ──────────────────────────────────
    # Helpers (이 파일 내부에서만)
    # ──────────────────────────────────
    def pivot_perf(df_in: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        return df_in.groupby(keys, as_index=False).agg(**AGG_MAP)

    def render_decor_perf(df: pd.DataFrame, pivot_cols: list[str]) -> pd.DataFrame:
        df2 = df

        # 자료형 워싱 1
        if "event_date" in df2.columns:
            df2 = df2.assign(event_date=pd.to_datetime(df2["event_date"], errors="coerce").dt.strftime("%Y-%m-%d"))

        # 파생 지표 생성(원 로직 유지)
        df2 = df2.assign(
            CPC=((df2["cost_gross_sum"] / df2["clicks_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(0)),
            CTR=((df2["clicks_sum"] / df2["impressions_sum"] * 100).replace([np.inf, -np.inf], 0).fillna(0).round(2)),
            session_count_CPA=(df2["cost_gross_sum"] / df2["session_count"]).replace([np.inf, -np.inf], 0).fillna(0).round(0),

            view_item_CPA=(df2["cost_gross_sum"] / df2["view_item_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(2),
            product_page_scroll_50_CPA=(df2["cost_gross_sum"] / df2["product_page_scroll_50_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(2),
            product_option_price_CPA=(df2["cost_gross_sum"] / df2["product_option_price_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(2),
            find_nearby_showroom_CPA=(df2["cost_gross_sum"] / df2["find_nearby_showroom_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(2),
            showroom_10s_CPA=(df2["cost_gross_sum"] / df2["showroom_10s_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(2),
            showroom_leads_CPA=(df2["cost_gross_sum"] / df2["showroom_leads_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(2),
            add_to_cart_CPA=(df2["cost_gross_sum"] / df2["add_to_cart_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(2),
            purchase_CPA=(df2["cost_gross_sum"] / df2["purchase_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(2),
        )

        # 자료형 워싱 2
        num_cols = df2.select_dtypes(include=["number"]).columns
        if len(num_cols) > 0:
            df2[num_cols] = df2[num_cols].replace([np.inf, -np.inf], np.nan).fillna(0)

        # 컬럼 순서
        base_info = ["period", "event_date"]
        pivot_extra = [c for c in pivot_cols if c not in base_info and c in df2.columns]

        metric_cols = [
            "cost_sum", "cost_gross_sum", "impressions_sum", "clicks_sum", "CPC", "CTR",
            "session_count", "session_count_CPA",
            "view_item_sum", "view_item_CPA",
            "product_page_scroll_50_sum", "product_page_scroll_50_CPA",
            "product_option_price_sum", "product_option_price_CPA",
            "find_nearby_showroom_sum", "find_nearby_showroom_CPA",
            "add_to_cart_sum", "add_to_cart_CPA",
            "showroom_10s_sum", "showroom_10s_CPA",
            "showroom_leads_sum", "showroom_leads_CPA",
            "purchase_sum", "purchase_CPA",
        ]

        ordered_cols = [c for c in (base_info + pivot_extra + metric_cols) if c in df2.columns]
        df2 = df2[ordered_cols]

        # 멀티인덱스 컬럼 맵
        metrics_map_dict = {
            "cost_sum": ("MEDIA", "광고비"),
            "cost_gross_sum": ("MEDIA", "광고비(G)"),
            "impressions_sum": ("MEDIA", "노출수"),
            "clicks_sum": ("MEDIA", "클릭수"),
            "CPC": ("MEDIA", "CPC"),
            "CTR": ("MEDIA", "CTR"),
            "session_count": ("전체 세션수", "Actual"),
            "session_count_CPA": ("전체 세션수", "CPA"),
            "view_item_sum": ("PDP조회", "Actual"),
            "view_item_CPA": ("PDP조회", "CPA"),
            "product_page_scroll_50_sum": ("PDPscr50", "Actual"),
            "product_page_scroll_50_CPA": ("PDPscr50", "CPA"),
            "product_option_price_sum": ("가격표시", "Actual"),
            "product_option_price_CPA": ("가격표시", "CPA"),
            "find_nearby_showroom_sum": ("쇼룸찾기", "Actual"),
            "find_nearby_showroom_CPA": ("쇼룸찾기", "CPA"),
            "add_to_cart_sum": ("장바구니", "Actual"),
            "add_to_cart_CPA": ("장바구니", "CPA"),
            "showroom_10s_sum": ("쇼룸10초", "Actual"),
            "showroom_10s_CPA": ("쇼룸10초", "CPA"),
            "showroom_leads_sum": ("쇼룸예약", "Actual"),
            "showroom_leads_CPA": ("쇼룸예약", "CPA"),
            "purchase_sum": ("구매완료", "Actual"),
            "purchase_CPA": ("구매완료", "CPA"),
        }

        multi_labels: list[tuple[str, str]] = []
        for c in ordered_cols:
            if c == "period":
                multi_labels.append(("기본정보", "기간"))
            elif c == "event_date":
                multi_labels.append(("기본정보", "날짜"))
            elif c in pivot_extra:
                multi_labels.append(("기본정보", c))
            else:
                multi_labels.append(metrics_map_dict.get(c, ("기본정보", c)))

        df2.columns = pd.MultiIndex.from_tuples(multi_labels, names=["그룹", "지표"])
        return df2

    def render_style_perf(target_df: pd.DataFrame, pivot_cols: list[str]) -> None:
        styled = style_format(
            render_decor_perf(target_df, pivot_cols),
            decimals_map={
                ("MEDIA", "광고비"): 0,
                ("MEDIA", "광고비(G)"): 0,
                ("MEDIA", "노출수"): 0,
                ("MEDIA", "클릭수"): 0,
                ("MEDIA", "CPC"): 0,
                ("MEDIA", "CTR"): 2,
                ("전체 세션수", "Actual"): 0,
                ("전체 세션수", "CPA"): 0,
                ("PDP조회", "Actual"): 0,
                ("PDP조회", "CPA"): 0,
                ("PDPscr50", "Actual"): 0,
                ("PDPscr50", "CPA"): 0,
                ("가격표시", "Actual"): 0,
                ("가격표시", "CPA"): 0,
                ("쇼룸찾기", "Actual"): 0,
                ("쇼룸찾기", "CPA"): 0,
                ("장바구니", "Actual"): 0,
                ("장바구니", "CPA"): 0,
                ("쇼룸10초", "Actual"): 0,
                ("쇼룸10초", "CPA"): 0,
                ("쇼룸예약", "Actual"): 0,
                ("쇼룸예약", "CPA"): 0,
                ("구매완료", "Actual"): 0,
                ("구매완료", "CPA"): 0,
            },
            suffix_map={
                ("MEDIA", "CTR"): " %",
            },
        )

        styled2 = style_cmap(
            styled,
            gradient_rules=[
                {"col": ("MEDIA", "노출수"), "cmap": "Blues", "low": 0.0, "high": 0.3},
                {"col": ("MEDIA", "클릭수"), "cmap": "PuBu", "low": 0.0, "high": 0.3},

                {"col": ("전체 세션수", "CPA"), "cmap": "OrRd_r", "low": 0.4, "high": -0.3},
                {"col": ("PDP조회", "CPA"), "cmap": "OrRd_r", "low": 0.4, "high": -0.4},
                {"col": ("PDPscr50", "CPA"), "cmap": "OrRd_r", "low": 0.4, "high": -0.4},
                {"col": ("가격표시", "CPA"), "cmap": "OrRd_r", "low": 0.3, "high": -0.5},
                {"col": ("쇼룸찾기", "CPA"), "cmap": "OrRd_r", "low": 0.3, "high": -0.5},
                {"col": ("장바구니", "CPA"), "cmap": "OrRd_r", "low": 0.3, "high": -0.6},
                {"col": ("쇼룸10초", "CPA"), "cmap": "OrRd_r", "low": 0.3, "high": -0.6},
                {"col": ("쇼룸예약", "CPA"), "cmap": "OrRd_r", "low": 0.3, "high": -0.7},
                {"col": ("구매완료", "CPA"), "cmap": "OrRd_r", "low": 0.3, "high": -0.7},
            ],
        )
        st.dataframe(styled2, use_container_width=True, height=500, row_height=30, hide_index=True)

    # (26.02.10) 포함 필터에서 정규표현식 필터로 변경 
    def apply_regex_filter(
        df: pd.DataFrame,
        df_cmp: pd.DataFrame | None,
        column: str,
        text_filter: bool = False,
    ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        key = f"{column}_{'text' if text_filter else 'multi'}"

        # ── 1) 정규식(텍스트) 필터 ─────────────────
        if text_filter:
            expr = st.text_input(
                f"{HEADER_MAP.get(column, column)} 정규식 검색",
                key=key
            )

            if expr:
                s = df[column].astype(str)

                if "&" in expr:
                    terms = [t.strip() for t in expr.split("&") if t.strip()]
                    mask = pd.Series(True, index=df.index)
                    for t in terms:
                        if t.startswith("!"):
                            mask &= ~s.str.contains(t[1:], regex=True, na=False)
                        else:
                            mask &= s.str.contains(t, regex=True, na=False)

                elif "|" in expr:
                    terms = [t.strip() for t in expr.split("|") if t.strip()]
                    mask = pd.Series(False, index=df.index)
                    for t in terms:
                        if t.startswith("!"):
                            mask |= ~s.str.contains(t[1:], regex=True, na=False)
                        else:
                            mask |= s.str.contains(t, regex=True, na=False)

                else:
                    if expr.startswith("!"):
                        mask = ~s.str.contains(expr[1:], regex=True, na=False)
                    else:
                        mask = s.str.contains(expr, regex=True, na=False)

                df = df[mask]

                if df_cmp is not None:
                    s_cmp = df_cmp[column].astype(str)

                    # df에서 만든 mask를 비교기간에도 "동일 조건"으로 적용
                    if "&" in expr:
                        terms = [t.strip() for t in expr.split("&") if t.strip()]
                        mask_cmp = pd.Series(True, index=df_cmp.index)
                        for t in terms:
                            if t.startswith("!"):
                                mask_cmp &= ~s_cmp.str.contains(t[1:], regex=True, na=False)
                            else:
                                mask_cmp &= s_cmp.str.contains(t, regex=True, na=False)

                    elif "|" in expr:
                        terms = [t.strip() for t in expr.split("|") if t.strip()]
                        mask_cmp = pd.Series(False, index=df_cmp.index)
                        for t in terms:
                            if t.startswith("!"):
                                mask_cmp |= ~s_cmp.str.contains(t[1:], regex=True, na=False)
                            else:
                                mask_cmp |= s_cmp.str.contains(t, regex=True, na=False)

                    else:
                        if expr.startswith("!"):
                            mask_cmp = ~s_cmp.str.contains(expr[1:], regex=True, na=False)
                        else:
                            mask_cmp = s_cmp.str.contains(expr, regex=True, na=False)

                    df_cmp = df_cmp[mask_cmp]

            return df, df_cmp

        # ── 2) 멀티셀렉트 필터(기존 apply_filter_pair 그대로) ─────────
        opts = sorted(df_primary[column].dropna().unique())
        sel = st.multiselect(f"{HEADER_MAP.get(column, column)} 필터", opts, key=key)
        if sel:
            df = df[df[column].isin(sel)]
            if df_cmp is not None:
                df_cmp = df_cmp[df_cmp[column].isin(sel)]
        return df, df_cmp


    # ──────────────────────────────────
    # D) Header
    # ──────────────────────────────────
    st.subheader("퍼포먼스 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()
        st.rerun()

    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="font-size:14px;line-height:1.5;">
            광고 매체 데이터와 GA 행동 데이터를 매칭하여, <b>캠페인·브랜드·품목 등</b>의 기준으로
            <b>퍼포먼스 마케팅 성과</b>를 통합 분석할 수 있는 대시보드입니다.<br>
            </div>
            <div style="color:#6c757d;font-size:14px;line-height:2.0;">
            ※ 매체-GA 통합 D-1 데이터는 매일 15시 ~ 16시 사이에 업데이트됩니다.
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
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
            unsafe_allow_html=True,
        )

    st.divider()

    # ──────────────────────────────────
    # 1) 커스텀 리포트
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>매체-GA 통합 리포트</h5>", unsafe_allow_html=True)
    st.markdown(
        ":gray-badge[:material/Info: Info]ㅤ**행 필드**는 데이터를 어떤 기준으로 구분해 볼지 정하는 기능이며, **필터**로 원하는 조건만 선택해 결과를 확인할 수 있습니다.",    
        unsafe_allow_html=True,
    )
        
    with st.popover("🤔 고급필터 정규식 사용 방법"):
        st.markdown("""
    - **단일 입력**  
    입력한 단어/패턴을 **포함**하는 값을 찾습니다.  
    예) `슬립퍼` : 슬립퍼 캠페인만 조회  
    예) `low` : low 퍼널 캠페인만 조회  

    - **OR (`|`)**  
    여러 패턴 중 **하나라도 포함**하면 매칭됩니다.  
    예) `스테이블|시그니처` : 스테이블 또는 시그니처 포함

    - **AND (`&`)**  
    입력한 **모든 패턴이 포함**되어야 매칭됩니다.  
    예) `low&시그니처` : low도 있고 시그니처도 있는 값

    - **제외 (`!`)**  
    `!패턴`은 해당 패턴을 **제외**합니다.  
    예) `!누어` : 누어 포함된 값 제외  
    예) `슬립퍼&!프로모션` : 슬립퍼 중 프로모션 제외  

    - 여러 기호를 함께 사용하거나, 기본 정규표현식 문법도 사용 가능합니다.  
    예) `^BSA` : BSA로 시작   
    예) `MO$` : MO로 끝  
    예) `슬립퍼&(허쉬|시그)` : 슬립퍼 허쉬 또는 슬립퍼 시그니처  
    예) `누어&low&!매트리스` : 누어 low 중 매트리스만 제외  
    """)


    st.markdown(" ")

    pivot_cols = st.multiselect(
        "행 필드 선택",
        options=list(HEADER_MAP.keys()),
        default=["event_date"],
        format_func=lambda x: HEADER_MAP.get(x, x),
    )

    # 기간별 합계 보기 모드라면 event_date 는 무시
    if show_totals and "event_date" in pivot_cols:
        pivot_cols.remove("event_date")
        st.caption("기간별 합계 보기 선택시, 날짜는 자동으로 제외됩니다.")

    # 필터
    with st.expander("기본 필터", expanded=False):
        ft1, ft2, ft3, ft4, ft5, ft6 = st.columns(6)
        with ft1:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "media_name", text_filter=False)
        with ft2:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "utm_source", text_filter=False)
        with ft3:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "utm_medium", text_filter=False)
        with ft4:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "brand_type", text_filter=False)
        with ft5:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "funnel_type", text_filter=False)
        with ft6:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "product_type", text_filter=False)

    with st.expander("고급 필터", expanded=False):
        ft7, ft8, ft9, ft10 = st.columns([2, 1, 2, 1])
        with ft7:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "campaign_name", text_filter=False)
        with ft8:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "campaign_name", text_filter=True)
        with ft9:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "adgroup_name", text_filter=False)
        with ft10:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "adgroup_name", text_filter=True)

        ft11, ft12, ft13, ft14 = st.columns([2, 1, 2, 1])
        with ft11:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "ad_name", text_filter=False)
        with ft12:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "ad_name", text_filter=True)
        with ft13:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "keyword_name", text_filter=False)
        with ft14:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "keyword_name", text_filter=True)

        ft15, ft16, ft17, ft18 = st.columns([2, 1, 2, 1])
        with ft15:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "utm_content", text_filter=False)
        with ft16:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "utm_content", text_filter=True)
        with ft17:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "utm_term", text_filter=False)
        with ft18:
            df_filtered, df_filtered_cmp = apply_regex_filter(df_filtered, df_filtered_cmp, "utm_term", text_filter=True)


    # ------------------------------
    # 표 (pivot)
    # ------------------------------
    if pivot_cols or show_totals:
        if show_totals:
            df_sel = df_filtered.assign(period=f"{start_date_str} ~ {end_date_str}")

            if use_compare:
                df_cmp = df_filtered_cmp.assign(period=f"{default_comp_s_str} ~ {default_comp_e_str}")
                df_combined = pd.concat([df_sel, df_cmp], ignore_index=True)
            else:
                df_combined = df_sel

            group_keys = ["period"] + pivot_cols
            df_pivot = pivot_perf(df_combined, group_keys)
            render_style_perf(df_pivot, group_keys)


        else:
            df_sel = pivot_perf(df_filtered, pivot_cols).assign(period=f"{start_date_str} ~ {end_date_str}")

            if use_compare:
                df_cmp = pivot_perf(df_filtered_cmp, pivot_cols).assign(period=f"{default_comp_s_str} ~ {default_comp_e_str}")
                df_pivot = pd.concat([df_sel, df_cmp], ignore_index=True)
            else:
                df_pivot = df_sel

            render_style_perf(df_pivot, ["period"] + pivot_cols)


    else:
        st.warning("피벗할 행 필드를 하나 이상 선택해 주세요.")



if __name__ == "__main__":
    main()
