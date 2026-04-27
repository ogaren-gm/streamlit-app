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

import modules.ui_common as ui
importlib.reload(ui)
from modules.ui_common import style_format, style_cmap

from google.oauth2.service_account import Credentials
import gspread
from io import BytesIO # 추가
from openpyxl import Workbook # 추가

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
                padding-top: 0rem;
                padding-bottom: 8rem;
                padding-left: 5rem;
                padding-right: 4.5rem;
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
    "event_date": "날짜(일별)",
    "event_date2": "날짜(주별)",  # (26.04.14) 주별 추가
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
        bq = BigQuery(projectCode="sleeper", custom_startDate=cs, custom_endDate=ce)

        # 1) tb_media
        df_bq = bq.get_data("tb_media")
        df_bq["event_date"] = pd.to_datetime(df_bq["event_date"], format="%Y%m%d", errors="coerce")

        parts = df_bq["campaign_name"].astype(str).str.split("_", n=5, expand=True)
        df_bq["campaign_name_short"] = df_bq["campaign_name"]
        mask = parts[5].notna()
        df_bq.loc[mask, "campaign_name_short"] = (
            parts.loc[mask, :4].apply(lambda r: "_".join(r.dropna().astype(str)), axis=1)
        )

        # 2) tb_media_touchpoint
        df_bq_touchpoint = bq.get_data("tb_media_touchpoint")
        df_bq_touchpoint["event_date"] = pd.to_datetime(df_bq_touchpoint["event_date"], format="%Y%m%d", errors="coerce")

        parts_touchpoint = df_bq_touchpoint["campaign_name"].astype(str).str.split("_", n=5, expand=True)
        df_bq_touchpoint["campaign_name_short"] = df_bq_touchpoint["campaign_name"]
        mask_touchpoint = parts_touchpoint[5].notna()
        df_bq_touchpoint.loc[mask_touchpoint, "campaign_name_short"] = (
            parts_touchpoint.loc[mask_touchpoint, :4].apply(lambda r: "_".join(r.dropna().astype(str)), axis=1)
        )

        # 3) Google Sheet
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
        sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1g2HWpm3Le3t3P3Hb9nm2owoiaxywaXv--L0SHEDx3rQ/edit")
        df_sheet = pd.DataFrame(sh.worksheet("perf_campaign").get_all_records())

        # 4) merge
        merged = df_bq.merge(df_sheet, how="left", on="campaign_name_short")
        merged_touchpoint = df_bq_touchpoint.merge(df_sheet, how="left", on="campaign_name_short")

        # 5) AGG_MAP에서 쓰는 컬럼이 tb_media_touchpoint에 없으면 0으로 생성
        need_cols = [
            "cost", "impressions", "clicks",
            "view_item", "product_page_scroll_50", "product_option_price",
            "find_nearby_showroom", "showroom_10s", "add_to_cart",
            "showroom_leads", "purchase", "session_start", "engagement_time_msec_sum"
        ]
        for c in need_cols:
            if c not in merged.columns:
                merged[c] = 0
            if c not in merged_touchpoint.columns:
                merged_touchpoint[c] = 0

        # 6) cost_gross(v2)
        for x in [merged, merged_touchpoint]:
            x["cost_gross"] = np.where(
                x["event_date"] < pd.to_datetime("2025-11-06"),
                np.where(
                    x["media_name"].isin(["GOOGLE", "META"]),
                    x["cost"] * 1.1 / 0.98,
                    x["cost"],
                ),
                np.where(
                    x["media_name"].isin(["GOOGLE", "META"]),
                    x["cost"] * 1.1 / 0.955,
                    x["cost"],
                ),
            )

        # 7) handle NSA
        for x in [merged, merged_touchpoint]:
            cond = (
                (x["media_name"] == "NSA")
                & x["utm_source"].isna()
                & x["utm_medium"].isna()
                & x["media_name_type"].isin(["RSA_AD", "TEXT_45"])
            )
            x.loc[cond, ["utm_source", "utm_medium"]] = ["naver", "search-nonmatch"]

        # 8) 주별 컬럼
        for x in [merged, merged_touchpoint]:
            week_start = x["event_date"] - pd.to_timedelta(x["event_date"].dt.weekday, unit="D")
            week_end = week_start + pd.Timedelta(days=6)
            x["event_date2"] = week_start.dt.strftime("%Y-%m-%d") + " ~ " + week_end.dt.strftime("%Y-%m-%d")

        # 9) 공통 컬럼 기준으로 union
        common_cols = [c for c in merged.columns if c in merged_touchpoint.columns]
        merged_union = pd.concat(
            [merged[common_cols], merged_touchpoint[common_cols]],
            ignore_index=True
        )

        # 10) 최종 event_date 문자열 변환
        merged["event_date"] = merged["event_date"].dt.strftime("%Y-%m-%d")
        merged_union["event_date"] = pd.to_datetime(merged_union["event_date"], errors="coerce").dt.strftime("%Y-%m-%d")

        return merged, merged_union


    # ──────────────────────────────────
    # C-1) tb_max -> get max date
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def get_max():
        bq  = BigQuery(projectCode="sleeper") # 기간 인자 없이
        date_max = bq.get_max_date("tb_media")
        date_max = pd.to_datetime(date_max).date()

        date_today = datetime.now().date()
        date_diff = (date_today - date_max).days
        
        return date_diff

    # ──────────────────────────────────
    # C-2) Progress Bar
    # ──────────────────────────────────
    import time
    
    spacer_placeholder = st.empty()
    progress_placeholder = st.empty()

    spacer_placeholder.markdown("<br>", unsafe_allow_html=True)
    progress_bar = progress_placeholder.progress(0, text="데이터베이스 연결 확인 중입니다...")
    time.sleep(0.2)
    
    for i in range(1, 80, 5):
        progress_bar.progress(i, text=f"데이터를 불러오고 있습니다...{i}%")
        time.sleep(0.1)

    if use_compare:
        cs_cmp = comp_start.strftime("%Y%m%d")
        df_merged, df_merged_union = load_data(cs_cmp, ce)

        df_merged["event_date"] = pd.to_datetime(df_merged["event_date"], errors="coerce")
        df_merged_union["event_date"] = pd.to_datetime(df_merged_union["event_date"], errors="coerce")

        df_filtered = df_merged[
            (df_merged["event_date"] >= pd.to_datetime(start_date))
            & (df_merged["event_date"] <= pd.to_datetime(end_date))
        ]
        df_filtered_cmp = df_merged[
            (df_merged["event_date"] >= pd.to_datetime(comp_start))
            & (df_merged["event_date"] <= pd.to_datetime(comp_end))
        ]

        df_filtered_union = df_merged_union[
            (df_merged_union["event_date"] >= pd.to_datetime(start_date))
            & (df_merged_union["event_date"] <= pd.to_datetime(end_date))
        ]
        df_filtered_cmp_union = df_merged_union[
            (df_merged_union["event_date"] >= pd.to_datetime(comp_start))
            & (df_merged_union["event_date"] <= pd.to_datetime(comp_end))
        ]
    else:
        df_merged, df_merged_union = load_data(cs, ce)

        df_merged["event_date"] = pd.to_datetime(df_merged["event_date"], errors="coerce")
        df_merged_union["event_date"] = pd.to_datetime(df_merged_union["event_date"], errors="coerce")

        df_filtered = df_merged
        df_filtered_cmp = None

        df_filtered_union = df_merged_union
        df_filtered_cmp_union = None

    progress_bar.progress(95, text="데이터 분석 및 시각화를 구성 중입니다...")
    time.sleep(0.4)
    
    progress_bar.progress(100, text="데이터 로드 완료!")
    time.sleep(0.6)
    
    progress_placeholder.empty()
    spacer_placeholder.empty()
    
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
        base_info = ["period", "event_date", "event_date2"] # (26.04.14) 주별 추가
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
                multi_labels.append(("기본정보", "날짜(일별)"))
            elif c == "event_date2":
                multi_labels.append(("기본정보", "날짜(주별)"))
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
                {"col": ("MEDIA", "노출수"), "cmap": "YlOrBr", "cmap_span": (0.0, 0.3)},
                {"col": ("MEDIA", "클릭수"), "cmap": "YlOrBr", "cmap_span": (0.0, 0.3)},
                {"col": ("전체 세션수", "CPA"), "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)},
                {"col": ("PDP조회", "CPA"), "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)},
                {"col": ("PDPscr50", "CPA"), "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)},
                {"col": ("가격표시", "CPA"), "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)},
                {"col": ("쇼룸찾기", "CPA"), "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)},
                {"col": ("장바구니", "CPA"), "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)},
                {"col": ("쇼룸10초", "CPA"), "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)},
                {"col": ("쇼룸예약", "CPA"), "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)},
                {"col": ("구매완료", "CPA"), "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)},
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


    def apply_saved_filter(
        df: pd.DataFrame,
        df_cmp: pd.DataFrame | None,
        column: str,
        text_filter: bool = False,
    ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        key = f"{column}_{'text' if text_filter else 'multi'}"

        if text_filter:
            expr = st.session_state.get(key, "")
            if not expr:
                return df, df_cmp

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

        sel = st.session_state.get(key, [])
        if sel:
            df = df[df[column].isin(sel)]
            if df_cmp is not None:
                df_cmp = df_cmp[df_cmp[column].isin(sel)]
        return df, df_cmp


    # (26.03.06) 엑셀 다운로드
    def excel_bytes(df: pd.DataFrame) -> bytes:
        wb = Workbook()
        ws = wb.active
        ws.title = "report"

        # MultiIndex 컬럼인 경우: 헤더 2줄 저장
        if isinstance(df.columns, pd.MultiIndex):
            header_top = [str(x[0]) if pd.notna(x[0]) else "" for x in df.columns]
            header_bottom = [str(x[1]) if pd.notna(x[1]) else "" for x in df.columns]

            ws.append(header_top)
            ws.append(header_bottom)

            for row in df.itertuples(index=False, name=None):
                ws.append(list(row))

        # 일반 컬럼인 경우: 헤더 1줄 저장
        else:
            ws.append([str(c) for c in df.columns])
            for row in df.itertuples(index=False, name=None):
                ws.append(list(row))

        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)
        return bio.getvalue()


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
            광고 매체 집행 데이터와 GA4 사용자 행동 데이터를 통합하여, <b>퍼포먼스 마케팅 효율</b>을 통합적으로 관리하고 분석하는 대시보드입니다. <br>
            </div>
            <div style="color:#6c757d;font-size:14px;line-height:2.0;">
            ※ 전일 데이터 업데이트 시점은 15시~17시 입니다.
            </div>
            """,
            unsafe_allow_html=True,
        )
    
    with col2:       
        # tb_max 
        date_diff = get_max()
        if date_diff <= 1:
            status_text = f"업데이트 정상"
            icon_name = "event_available"
            color = "#7FD0C4"
            bg_color = "#F3FBFA"
        else:
            status_text = f"업데이트 이전 (D-{date_diff})"
            icon_name = "event_busy"
            color = "#FF8080"
            bg_color = "#FFF4F4"
        
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:bottom;gap:9px;">
            <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20,400,0,0" rel="stylesheet" />
            
            <span style="
                display:flex;
                align-items:center;
                justify-content:center;
                gap: 4px;
                height:30px;
                padding:10px 10px;
                font-size:13px;
                font-weight:400;
                letter-spacing:-0.3px;
                color:{color};
                background-color:{bg_color};
                border:1.5px solid {color};
                border-radius:14px;
                white-space:nowrap;
                cursor:default;">
                <span class="material-symbols-outlined" style="font-size:15px;">{icon_name}</span>
                {status_text}
            </span>
            
            <a href="?refresh=1" title="사용자 캐시를 초기화하고 서버의 최신 데이터로 갱신합니다." style="text-decoration:none;vertical-align:middle;">
            <span style="
                display:flex;
                align-items:center;
                justify-content:center;
                gap: 4px;
                height:30px;
                padding:10px 10px;
                font-size:13px;
                font-weight:400;
                letter-spacing:-0.3px;
                color:#8B92A0;
                background-color:#FFFFFF;
                border:1.5px solid #D6D6D9;
                border-radius:14px;
                white-space:nowrap;
                cursor:pointer;
                transition:0.1s;">
                <span class="material-symbols-outlined" style="font-size:15px;">sync</span>
                강력 새로고침
            </span>
            </a>
            """,
            unsafe_allow_html=True
            )

    st.divider()


    # ──────────────────────────────────
    # 1) QUICK INSIGHT
    # ──────────────────────────────────
    st.markdown(" ")
    st.markdown("<h5 style='margin:0'>QUICK INSIGHT</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ기간 내 핵심 성과 지표를 요약하고, CPA 기준 성과 상·하위 캠페인을 빠르게 진단합니다.", unsafe_allow_html=True)

    st.markdown(
        """
        <style>
        .kpi-card{
            background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:14px 16px;
        }
        .kpi-title{font-size:15px;color:#64748b;margin:0 0 8px}
        .kpi-row{display:flex;align-items:baseline;justify-content:space-between;gap:10px}
        .kpi-value{font-size:25px;font-weight:500;line-height:1.05;margin:0;white-space:nowrap}
        .kpi-delta{font-size:12px;margin:0;white-space:nowrap}

        /* selectbox 간격(전역) */
        div[data-testid="stSelectbox"]>div{margin-top:-10px}

        /* QUICK INSIGHT 이벤트 카드 컨테이너(카드 테두리/배경) */
        .st-key-ins_kpi_card_evt{
            background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:14px 16px;
        }
        .st-key-ins_kpi_card_evt div[data-testid="stSelectbox"]{margin-bottom:-10px}
        .st-key-ins_kpi_card_evt div[data-testid="stSelectbox"]>div{margin-top:-6px}
        .st-key-ins_kpi_card_evt .kpi-value{margin-bottom:10px}
        </style>
        """,
        unsafe_allow_html=True,
    )

    evt_opts__ins = [
        ("PDP조회", "view_item"),
        ("PDPscr50", "product_page_scroll_50"),
        ("가격표시", "product_option_price"),
        ("쇼룸찾기", "find_nearby_showroom"),
        ("장바구니", "add_to_cart"),
        ("쇼룸10초", "showroom_10s"),
        ("쇼룸예약", "showroom_leads"),
        ("구매완료", "purchase"),
    ]
    evt_map__ins = dict(evt_opts__ins)
    evt_labels__ins = [x[0] for x in evt_opts__ins]

    def _safe_div(a, b):
        return 0.0 if b == 0 else (a / b)

    def _summary(df_in: pd.DataFrame, evt_raw: str) -> dict:
        cost_g = float(df_in["cost_gross"].sum())
        imp = float(df_in["impressions"].sum()) if "impressions" in df_in.columns else 0.0
        clk = float(df_in["clicks"].sum()) if "clicks" in df_in.columns else 0.0
        ses = float(df_in["session_start"].sum())
        evt = float(df_in[evt_raw].sum()) if evt_raw in df_in.columns else 0.0
        return dict(
            cost_g=cost_g,
            clk=clk,
            ses=ses,
            ctr=_safe_div(clk, imp) * 100,
            cpc=_safe_div(cost_g, clk),
            evt=evt,
            cpa=_safe_div(cost_g, evt),
        )

    def _fmt_delta2(cur, prev, good_if_down=False, decimals=1):
        if prev == 0:
            return "", "#64748b"
        d = (cur - prev) / prev * 100
        col = "#16a34a" if ((d <= 0) if good_if_down else (d >= 0)) else "#ef4444"
        return f"{d:+.{decimals}f}%", col

    # ✅ 이벤트 선택값(카드 내부 selectbox) 초기값
    sel_evt_label__ins = st.session_state.get("ins_evt_in_card", evt_labels__ins[0])
    if sel_evt_label__ins not in evt_map__ins:
        sel_evt_label__ins = evt_labels__ins[0]
    sel_evt_raw__ins = evt_map__ins[sel_evt_label__ins]

    cur = _summary(df_filtered, sel_evt_raw__ins)
    prev = _summary(df_filtered_cmp, sel_evt_raw__ins) if (use_compare and df_filtered_cmp is not None) else None

    # 0-1) KPI 6개
    q1, q2, q3, q4, q5, q6 = st.columns(6, vertical_alignment="top")

    with q1:
        t, c = _fmt_delta2(cur["cost_g"], prev["cost_g"], False, 1) if prev else ("", "#64748b")
        st.markdown(f"""
            <div class="kpi-card">
            <div class="kpi-title">광고비(G)</div>
            <div class="kpi-row">
                <div class="kpi-value">{cur["cost_g"]:,.0f}</div>
                <div class="kpi-delta" style="color:{c};">{t}</div>
            </div></div>
        """, unsafe_allow_html=True)

    with q2:
        t, c = _fmt_delta2(cur["ses"], prev["ses"], False, 1) if prev else ("", "#64748b")
        st.markdown(f"""
            <div class="kpi-card">
            <div class="kpi-title">세션</div>
            <div class="kpi-row">
                <div class="kpi-value">{cur["ses"]:,.0f}</div>
                <div class="kpi-delta" style="color:{c};">{t}</div>
            </div></div>
        """, unsafe_allow_html=True)

    with q3:
        t, c = _fmt_delta2(cur["clk"], prev["clk"], False, 1) if prev else ("", "#64748b")
        st.markdown(f"""
            <div class="kpi-card">
            <div class="kpi-title">클릭수</div>
            <div class="kpi-row">
                <div class="kpi-value">{cur["clk"]:,.0f}</div>
                <div class="kpi-delta" style="color:{c};">{t}</div>
            </div></div>
        """, unsafe_allow_html=True)

    with q4:
        t, c = _fmt_delta2(cur["ctr"], prev["ctr"], False, 2) if prev else ("", "#64748b")
        st.markdown(f"""
            <div class="kpi-card">
            <div class="kpi-title">CTR</div>
            <div class="kpi-row">
                <div class="kpi-value">{cur["ctr"]:,.2f}%</div>
                <div class="kpi-delta" style="color:{c};">{t}</div>
            </div></div>
        """, unsafe_allow_html=True)

    # ✅ 5) 이벤트 카드: "컨테이너(카드) + 내부 selectbox" (딥 인사이트 3번째 카드와 동일 구조)
    with q5:
        with st.container(key="ins_kpi_card_evt"):
            sel_evt_label__ins = st.selectbox(
                "",
                evt_labels__ins,
                index=evt_labels__ins.index(sel_evt_label__ins),
                key="ins_evt_in_card",
                label_visibility="collapsed",
            )
            sel_evt_raw__ins = evt_map__ins[sel_evt_label__ins]
            cur = _summary(df_filtered, sel_evt_raw__ins)
            prev = _summary(df_filtered_cmp, sel_evt_raw__ins) if (use_compare and df_filtered_cmp is not None) else None
            t, c = _fmt_delta2(cur["evt"], prev["evt"], False, 1) if prev else ("", "#64748b")

            st.markdown(
                f"""
                <div class="kpi-row" style="margin-top:-2px;">
                    <div class="kpi-value">{cur["evt"]:,.0f}</div>
                    <div class="kpi-delta" style="color:{c};">{t}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with q6:
        t, c = _fmt_delta2(cur["cpa"], prev["cpa"], True, 1) if prev else ("", "#64748b")
        st.markdown(f"""
            <div class="kpi-card">
            <div class="kpi-title">CPA</div>
            <div class="kpi-row">
                <div class="kpi-value">{cur["cpa"]:,.0f}</div>
                <div class="kpi-delta" style="color:{c};">{t}</div>
            </div></div>
        """, unsafe_allow_html=True)

    st.markdown(" ")

    # (Quick Insight - TOP/비효율 TOP) ✅ 광고비(G) 1원 이상만 대상으로 소팅
    topk = 10
    need_cols = ["media_name", "campaign_name", "cost_gross", sel_evt_raw__ins]
    if all(c in df_filtered.columns for c in need_cols):
        g = (
            df_filtered
            .groupby(["media_name", "campaign_name"], dropna=False, as_index=False)
            .agg(cost_gross_sum=("cost_gross", "sum"), evt_sum=(sel_evt_raw__ins, "sum"))
        )

        # ✅ 광고비 1원 이상 + 이벤트 1 이상만 남기고 소팅
        g = g[(g["cost_gross_sum"] >= 1) & (g["evt_sum"] > 0)]

        g["CPA"] = (g["cost_gross_sum"] / g["evt_sum"]).replace([np.inf, -np.inf], 0).fillna(0).round(0)

        def _top(df_in: pd.DataFrame, asc: bool) -> pd.DataFrame:
            return (
                df_in
                .sort_values(["CPA", "evt_sum", "cost_gross_sum"], ascending=[asc, False, False])
                .head(topk)
                .rename(columns={
                    "media_name": "매체",
                    "campaign_name": "캠페인",
                    "cost_gross_sum": "광고비(G)",
                    "evt_sum": sel_evt_label__ins,
                    "CPA": "CPA",
                })
            )

        a, b = st.columns(2, vertical_alignment="top")
        with a:
            st.markdown(f"###### 🙂 Low CPA Top {topk}")
            
            df_LCT = _top(g, True)
            sty = style_format(df_LCT, decimals_map={"광고비(G)": 0, sel_evt_label__ins: 0, "CPA": 0})
            sty = style_cmap(sty, gradient_rules=[{"col": "CPA", "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)}])
            st.dataframe(sty, use_container_width=True, height=250, hide_index=True)
                        
        with b:
            st.markdown(f"###### 🙁 High CPA Top {topk}")
            
            df_HCT = _top(g, False)
            sty = style_format(df_HCT, decimals_map={"광고비(G)": 0, sel_evt_label__ins: 0, "CPA": 0})
            sty = style_cmap(sty, gradient_rules=[{"col": "CPA", "cmap": "PiYG_r", "cmap_span": (0.3, 0.7)}])
            st.dataframe(sty, use_container_width=True, height=250, hide_index=True)            


    # ──────────────────────────────────
    # 2) DEEP INSIGHT
    # ──────────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>DEEP INSIGHT</h5>", unsafe_allow_html=True)
    st.markdown(":gray-badge[:material/Info: Info]ㅤ브랜드·품목·캠페인·소재·키워드 등 목적에 맞는 행 필드를 다중 선택하여 상세 성과 데이터를 자유롭게 재구성(Pivot)합니다.", unsafe_allow_html=True) 
    
    with st.popover("🤔 고급 필터 정규식, 어떻게 쓰나요?"):
        st.markdown("""
        ##### 🛠️ 검색 연산자 가이드
        찾고자 하는 캠페인이나 데이터 단어들 사이에 아래 기호를 넣어 검색 범위를 조절할 수 있습니다.

        | 기호 | 기능 | 예시 |
        | :---: | :--- | :--- |
        | **`\|`** | **OR** | `A\|B` (A 또는 B 포함) |
        | **`&`** | **AND** | `A&B` (A와 B 모두 포함) |
        | **`!`** | **NOT** | `!A` (A가 포함된 항목 제외) |
        | **`^` / `$`** | **시작 / 끝** | `^A` (A로 시작), `B$` (B로 종료) |

        #####  
        ##### 🚀 검색 활용 예시
        * **특정 키워드 제외** : `슬립퍼&!프로모션`
            * '슬립퍼' 캠페인 중 '프로모션'이 붙은 항목만 쏙 빼고 조회
        * **다중 조건 포함** : `누어&low&!매트리스`
            * '누어'이면서 'low' 퍼널이지만 '매트리스'는 제외하고 싶을 때
        * **그룹화 활용** : `슬립퍼&(허쉬|시그)`
            * '슬립퍼 허쉬'와 '슬립퍼 시그니처'를 한 번에 조회
        * **위치 지정** : `^BSA` 또는 `MO$`
            * 'BSA'로 시작하는 캠페인 혹은 'MO'로 끝나는 매체만 필터링
        """)




    st.markdown(" ")

    pivot_cols = st.multiselect(
        "행 필드 선택 ㅤ(*기간별 합계 보기 선택시, 날짜는 자동으로 제외됩니다.)",
        options=list(HEADER_MAP.keys()),
        default=["event_date"],
        format_func=lambda x: HEADER_MAP.get(x, x),
    )

    # 기간별 합계 보기 모드라면 event_date 는 무시 
    if show_totals and "event_date" in pivot_cols:
        pivot_cols.remove("event_date")
    if show_totals and "event_date2" in pivot_cols: # (26.04.14) 주별 추가
        pivot_cols.remove("event_date2")

    # 필터
    with st.expander("기본 Filter", expanded=False):
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

    with st.expander("고급 Filter", expanded=False):
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

        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "media_name", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "utm_source", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "utm_medium", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "brand_type", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "funnel_type", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "product_type", text_filter=False)

        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "campaign_name", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "campaign_name", text_filter=True)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "adgroup_name", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "adgroup_name", text_filter=True)

        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "ad_name", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "ad_name", text_filter=True)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "keyword_name", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "keyword_name", text_filter=True)

        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "utm_content", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "utm_content", text_filter=True)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "utm_term", text_filter=False)
        df_filtered_union, df_filtered_cmp_union = apply_saved_filter(df_filtered_union, df_filtered_cmp_union, "utm_term", text_filter=True)


    # ------------------------------
    # ★ 영역
    # ------------------------------
    # 카드보드 (써머리)
    st.markdown(
        """
        <style>
        /* 카드 */
        .kpi-card, .st-key-kpi_card_evt{
            background:#f8fafc;
            border:1px solid #e2e8f0;
            border-radius:14px;
            padding:14px 16px;
        }

        /* 텍스트 */
        .kpi-title{font-size:15px;color:#64748b;margin:0 0 8px}
        .kpi-row{display:flex;align-items:baseline;justify-content:space-between;gap:10px}
        .kpi-value{font-size:25px;font-weight:500;line-height:1.05;margin:0;white-space:nowrap}
        .kpi-delta{font-size:12px;margin:0;white-space:nowrap}

        /* selectbox 간격 */
        div[data-testid="stSelectbox"]>div{margin-top:-10px}
        .st-key-kpi_card_evt div[data-testid="stSelectbox"]{margin-bottom:-10px}
        .st-key-kpi_card_evt div[data-testid="stSelectbox"]>div{margin-top:-6px}

        /* 3번째 카드 숫자 아래 */
        .st-key-kpi_card_evt .kpi-value{margin-bottom:10px}
        </style>
        """,
        unsafe_allow_html=True,
    )


    # 기준 이벤트(원본 df 컬럼)
    evt_opts = [
        ("PDP조회", "view_item"),
        ("PDPscr50", "product_page_scroll_50"),
        ("가격표시", "product_option_price"),
        ("쇼룸찾기", "find_nearby_showroom"),
        ("장바구니", "add_to_cart"),
        ("쇼룸10초", "showroom_10s"),
        ("쇼룸예약", "showroom_leads"),
        ("구매완료", "purchase"),
    ]
    evt_label_to_raw = dict(evt_opts)

    def _calc_kpi(df, evt_raw: str):
        cost = float(df["cost_gross"].sum())
        sessions = float(df["session_start"].sum())
        clk = float(df["clicks"].sum()) if "clicks" in df.columns else 0.0
        imp = float(df["impressions"].sum()) if "impressions" in df.columns else 0.0
        ctr = (clk / imp * 100) if imp > 0 else 0.0

        evt = float(df[evt_raw].sum()) if evt_raw in df.columns else 0.0
        cpa = (cost / evt) if evt > 0 else 0.0
        return cost, sessions, clk, ctr, evt, cpa

    def _delta(cur, prev):
        return None if prev == 0 else (cur - prev) / prev * 100

    def _fmt_delta(d, good_if_down: bool = False):
        if d is None or (isinstance(d, float) and np.isnan(d)):
            return "", "#64748b"
        col = "#16a34a" if ((d <= 0) if good_if_down else (d >= 0)) else "#ef4444"
        return f"{d:+.1f}%", col

    # 현재 선택값 유지
    sel_evt_label = st.session_state.get("kpi_evt_in_card", evt_opts[0][0])
    if sel_evt_label not in evt_label_to_raw:
        sel_evt_label = evt_opts[0][0]

    # 6개 카드 레이아웃
    st.markdown("###### 📊 Summary (라스트 클릭)")
    c1, c2, c3, c4, c5, c6 = st.columns(6, vertical_alignment="top")

    # (selectbox와 무관한 공통 KPI)
    cost, sessions, clk, ctr, evt, cpa = _calc_kpi(df_filtered, evt_label_to_raw[sel_evt_label])

    if use_compare and df_filtered_cmp is not None:
        cost_c, sessions_c, clk_c, ctr_c, evt_c, cpa_c = _calc_kpi(df_filtered_cmp, evt_label_to_raw[sel_evt_label])

        t_cost, col_cost = _fmt_delta(_delta(cost, cost_c), good_if_down=False)
        t_ses,  col_ses  = _fmt_delta(_delta(sessions, sessions_c), good_if_down=False)
        t_clk,  col_clk  = _fmt_delta(_delta(clk, clk_c), good_if_down=False)
        t_ctr,  col_ctr  = _fmt_delta(_delta(ctr, ctr_c), good_if_down=False)
    else:
        t_cost, col_cost = "", "#64748b"
        t_ses,  col_ses  = "", "#64748b"
        t_clk,  col_clk  = "", "#64748b"
        t_ctr,  col_ctr  = "", "#64748b"

    # 1) 광고비
    with c1:
        st.markdown(
            f"""
            <div class="kpi-card">
            <div class="kpi-title">광고비(G)</div>
            <div class="kpi-row">
                <div class="kpi-value">{cost:,.0f}</div>
                <div class="kpi-delta" style="color:{col_cost};">{t_cost}</div>
            </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 2) 세션
    with c2:
        st.markdown(
            f"""
            <div class="kpi-card">
            <div class="kpi-title">세션</div>
            <div class="kpi-row">
                <div class="kpi-value">{sessions:,.0f}</div>
                <div class="kpi-delta" style="color:{col_ses};">{t_ses}</div>
            </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 3) 클릭수
    with c3:
        st.markdown(
            f"""
            <div class="kpi-card">
            <div class="kpi-title">클릭수</div>
            <div class="kpi-row">
                <div class="kpi-value">{clk:,.0f}</div>
                <div class="kpi-delta" style="color:{col_clk};">{t_clk}</div>
            </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 4) CTR
    with c4:
        st.markdown(
            f"""
            <div class="kpi-card">
            <div class="kpi-title">CTR</div>
            <div class="kpi-row">
                <div class="kpi-value">{ctr:,.2f}%</div>
                <div class="kpi-delta" style="color:{col_ctr};">{t_ctr}</div>
            </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 5) 이벤트(selectbox + 값/증감)
    with c5:
        with st.container(key="kpi_card_evt"):
            sel_evt_label = st.selectbox(
                "",
                [x[0] for x in evt_opts],
                index=[x[0] for x in evt_opts].index(sel_evt_label),
                key="kpi_evt_in_card",
                label_visibility="collapsed",
            )
            sel_evt_raw = evt_label_to_raw[sel_evt_label]

            cost, sessions, clk, ctr, evt, cpa = _calc_kpi(df_filtered, sel_evt_raw)

            if use_compare and df_filtered_cmp is not None:
                cost_c, sessions_c, clk_c, ctr_c, evt_c, cpa_c = _calc_kpi(df_filtered_cmp, sel_evt_raw)
                t_evt, col_evt = _fmt_delta(_delta(evt, evt_c), good_if_down=False)
            else:
                t_evt, col_evt = "", "#64748b"

            st.markdown(
                f"""
                <div class="kpi-row" style="margin-top:-2px;">
                <div class="kpi-value">{evt:,.0f}</div>
                <div class="kpi-delta" style="color:{col_evt};">{t_evt}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # 6) CPA (선택 이벤트 기준)
    with c6:
        sel_evt_raw = evt_label_to_raw[sel_evt_label]
        cost, sessions, clk, ctr, evt, cpa = _calc_kpi(df_filtered, sel_evt_raw)

        if use_compare and df_filtered_cmp is not None:
            cost_c, sessions_c, clk_c, ctr_c, evt_c, cpa_c = _calc_kpi(df_filtered_cmp, sel_evt_raw)
            t_cpa, col_cpa = _fmt_delta(_delta(cpa, cpa_c), good_if_down=True)
        else:
            t_cpa, col_cpa = "", "#64748b"

        st.markdown(
            f"""
            <div class="kpi-card">
            <div class="kpi-title">CPA</div>
            <div class="kpi-row">
                <div class="kpi-value">{cpa:,.0f}</div>
                <div class="kpi-delta" style="color:{col_cpa};">{t_cpa}</div>
            </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


    # 표 (데이터프레임)
    # if pivot_cols or show_totals:
    #     if show_totals:
    #         df_sel = df_filtered.assign(period=f"{start_date_str} ~ {end_date_str}")

    #         if use_compare:
    #             df_cmp = df_filtered_cmp.assign(period=f"{default_comp_s_str} ~ {default_comp_e_str}")
    #             df_combined = pd.concat([df_sel, df_cmp], ignore_index=True)
    #         else:
    #             df_combined = df_sel

    #         group_keys = ["period"] + pivot_cols
    #         df_pivot = pivot_perf(df_combined, group_keys)
    #         render_style_perf(df_pivot, group_keys)

    #     else:
    #         df_sel = pivot_perf(df_filtered, pivot_cols).assign(period=f"{start_date_str} ~ {end_date_str}")

    #         if use_compare:
    #             df_cmp = pivot_perf(df_filtered_cmp, pivot_cols).assign(period=f"{default_comp_s_str} ~ {default_comp_e_str}")
    #             df_pivot = pd.concat([df_sel, df_cmp], ignore_index=True)
    #         else:
    #             df_pivot = df_sel

    #         render_style_perf(df_pivot, ["period"] + pivot_cols)

    # else:
    #     st.warning("피벗할 행 필드를 하나 이상 선택해 주세요.")

    # 표 (데이터프레임)
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

        else:
            df_sel = pivot_perf(df_filtered, pivot_cols).assign(period=f"{start_date_str} ~ {end_date_str}")

            if use_compare:
                df_cmp = pivot_perf(df_filtered_cmp, pivot_cols).assign(period=f"{default_comp_s_str} ~ {default_comp_e_str}")
                df_pivot = pd.concat([df_sel, df_cmp], ignore_index=True)
            else:
                df_pivot = df_sel

            group_keys = ["period"] + pivot_cols

        df_export = render_decor_perf(df_pivot, group_keys)

        # 1) 기존 표
        render_style_perf(df_pivot, group_keys)

        st.download_button(
            label="상기 표 엑셀 다운로드",
            data=excel_bytes(df_export),
            file_name=f"ORANGE_Perf_Report_export_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            icon="💾",
        )

        st.subheader(" ")
        st.markdown("###### 📊 Summary (누적 기여 D+7)")

        # 2) 합본 표
        if show_totals:
            df_sel_u = df_filtered_union.assign(period=f"{start_date_str} ~ {end_date_str}")

            if use_compare:
                df_cmp_u = df_filtered_cmp_union.assign(period=f"{default_comp_s_str} ~ {default_comp_e_str}")
                df_combined_u = pd.concat([df_sel_u, df_cmp_u], ignore_index=True)
            else:
                df_combined_u = df_sel_u

            df_pivot_u = pivot_perf(df_combined_u, group_keys)

        else:
            df_sel_u = pivot_perf(df_filtered_union, pivot_cols).assign(period=f"{start_date_str} ~ {end_date_str}")

            if use_compare:
                df_cmp_u = pivot_perf(df_filtered_cmp_union, pivot_cols).assign(period=f"{default_comp_s_str} ~ {default_comp_e_str}")
                df_pivot_u = pd.concat([df_sel_u, df_cmp_u], ignore_index=True)
            else:
                df_pivot_u = df_sel_u

        df_export_u = render_decor_perf(df_pivot_u, group_keys)
        render_style_perf(df_pivot_u, group_keys)

        st.download_button(
            label="상기 표 엑셀 다운로드",
            data=excel_bytes(df_export_u),
            file_name=f"ORANGE_Perf_Report_union_export_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            icon="💾",
        )

    else:
        st.warning("피벗할 행 필드를 하나 이상 선택해 주세요.")

if __name__ == "__main__":
    main()
