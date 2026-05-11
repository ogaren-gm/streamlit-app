import streamlit as st
import pandas as pd
import importlib
from datetime import datetime, timedelta
from html import escape
from urllib.parse import urlparse
import plotly.graph_objects as go

import modules.bigquery
importlib.reload(modules.bigquery)
from modules.bigquery import BigQuery


# ──────────────────────────────────
# CONFIG
# ──────────────────────────────────
CFG = {
    "TZ": "Asia/Seoul",
    "CACHE_TTL": 3600,
    "DEFAULT_LOOKBACK_DAYS": 14,
    "TABLE_NAME": "tb_naver_smartblock",
    "TOP_ITEM_ORDER": 3,

    "BLOCK_TYPE_ORDER": {
        "brandcontent": 1,
        "intentblock": 2,
        "influencer": 3,
    },
    "BLOCK_TYPE_LABEL": {
        "brandcontent": "브랜드 콘텐츠",
        "intentblock": "스마트블록",
        "influencer": "인플루언서",
    },

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


st.markdown(
    """
    <style>
        .main-title {
            font-size: 24px !important;
            font-weight: 700;
            margin-bottom: 10px;
        }

        .sub-title {
            font-size: 18px !important;
            font-weight: 600;
            margin-top: 24px;
            margin-bottom: 10px;
            color: #333;
            border-bottom: 1px solid #eee;
            padding-bottom: 5px;
        }

        .desc {
            font-size: 13px;
            color: #666;
            margin-bottom: 14px;
        }

        .search-wrap {
            background: #f6f8fa;
            border: 1px solid #e9ecef;
            border-radius: 22px;
            padding: 22px;
            margin-top: 12px;
            overflow: hidden;
        }

        .search-bar {
            display: flex;
            align-items: center;
            gap: 12px;
            background: #fff;
            border: 2px solid #03c75a;
            border-radius: 999px;
            padding: 13px 18px;
            margin-bottom: 20px;
        }

        .search-logo {
            font-size: 23px;
            font-weight: 900;
            color: #03c75a;
            line-height: 1;
        }

        .search-keyword {
            font-size: 17px;
            font-weight: 800;
            color: #111;
        }

        .search-date {
            margin-left: auto;
            font-size: 12px;
            color: #868e96;
            font-weight: 600;
        }

        .sb-grid {
            display: grid;
            gap: 16px;
            align-items: stretch;
            overflow: visible;
        }

        .sb-box {
            background: #fff;
            border: 1px solid #e5e8eb;
            border-radius: 18px;
            padding: 16px 14px 8px 14px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.035);
            min-width: 0;
            height: 100%;
        }

        .sb-head {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 10px;
            margin-bottom: 10px;
        }

        .sb-no {
            font-size: 12px;
            font-weight: 800;
            color: #03c75a;
            margin-bottom: 4px;
        }

        .sb-title {
            font-size: 16px;
            font-weight: 850;
            color: #111;
            line-height: 1.35;
            word-break: keep-all;
        }

        .sb-type {
            font-size: 12px;
            color: #868e96;
            margin-top: 4px;
        }

        .sb-badges {
            display: flex;
            gap: 6px;
            flex-wrap: wrap;
            justify-content: flex-end;
        }

        .sb-badge {
            display: inline-block;
            font-size: 11px;
            font-weight: 800;
            padding: 5px 8px;
            border-radius: 999px;
            background: #f1f3f5;
            color: #495057;
            white-space: nowrap;
        }

        .sb-badge.own {
            background: #fff3bf;
            color: #8a5a00;
            border: 1px solid #ffd43b;
        }

        .result-item {
            display: grid;
            grid-template-columns: 28px 1fr;
            gap: 10px;
            padding: 13px 0;
            border-top: 1px solid #f1f3f5;
            position: relative;
        }

        .result-item.own {
            background: #fff9db;
            border: 1px solid #ffd43b;
            border-radius: 14px;
            padding: 13px 10px;
            margin: 10px 0;
        }

        .rank {
            width: 26px;
            height: 26px;
            border-radius: 8px;
            background: #f1f3f5;
            color: #343a40;
            font-size: 12px;
            font-weight: 850;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        .result-item.own .rank {
            background: #ffd43b;
            color: #5c3b00;
        }

        .item-title {
            font-size: 14px;
            font-weight: 800;
            color: #1a0dab;
            line-height: 1.35;
            margin-bottom: 5px;
            word-break: keep-all;
            padding-right: 38px;
        }

        .item-title a {
            color: #1a0dab;
            text-decoration: none;
        }

        .item-title a:hover {
            text-decoration: underline;
        }

        .item-author {
            font-size: 12px;
            color: #03a64a;
            margin-bottom: 5px;
            overflow: hidden;
            white-space: nowrap;
            text-overflow: ellipsis;
        }

        .item-desc {
            font-size: 12.5px;
            color: #495057;
            line-height: 1.45;
            overflow: hidden;
            white-space: nowrap;
            text-overflow: ellipsis;
        }

        .chip {
            position: absolute;
            top: 12px;
            right: 10px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 11px;
            font-weight: 800;
            padding: 4px 7px;
            border-radius: 999px;
            white-space: nowrap;
        }

        .chip.own {
            background: #ffd43b;
            color: #5c3b00;
        }

        .chip.normal {
            display: none;
        }

        .empty-box {
            background: #fff;
            border: 1px dashed #ced4da;
            color: #868e96;
            border-radius: 14px;
            padding: 20px;
            text-align: center;
            font-size: 13px;
            width: 100%;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ──────────────────────────────────
# 헬퍼
# ──────────────────────────────────
def clean_text(v) -> str:
    if v is None:
        return ""

    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass

    s = str(v).strip()

    if s.lower() in ["none", "nan", "<na>"]:
        return ""

    return s


def html_text(v) -> str:
    return escape(clean_text(v), quote=True)


def safe_href(v) -> str:
    url = clean_text(v)

    if url.startswith("http://") or url.startswith("https://"):
        return escape(url, quote=True)

    return ""


def url_domain(v) -> str:
    url = clean_text(v)

    if not url:
        return ""

    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""


def short_text(v, max_len: int = 20) -> str:
    txt = clean_text(v)

    if len(txt) <= max_len:
        return txt

    return txt[:max_len].rstrip() + "..."


def first_nonempty(s: pd.Series) -> str:
    for v in s.tolist():
        txt = clean_text(v)
        if txt:
            return txt

    return ""


def block_type_label(v) -> str:
    block_type = clean_text(v)
    return CFG["BLOCK_TYPE_LABEL"].get(block_type, block_type or "-")


def block_type_order(v) -> int:
    block_type = clean_text(v)
    return CFG["BLOCK_TYPE_ORDER"].get(block_type, 99)


def build_search_result(df: pd.DataFrame, keyword: str, target_date) -> pd.DataFrame:
    target_dt = pd.to_datetime(target_date).normalize()

    d = df[
        (df["query"].eq(keyword)) &
        (df["start_date"].eq(target_dt)) &
        (df["block_type"].isin(CFG["BLOCK_TYPE_ORDER"].keys())) &
        (df["item_order_in_block"].between(1, CFG["TOP_ITEM_ORDER"]))
    ]

    d = d.sort_values(
        [
            "block_order",
            "item_order_in_block",
            "global_item_order",
        ],
        ascending=[True, True, True],
    )

    return d


def make_item_html(r) -> str:
    rank = int(r["item_order_in_block"])
    is_own = int(r["is_our_brand"]) > 0

    item_title = (
        clean_text(r.get("item_title")) or
        clean_text(r.get("source_name")) or
        "(제목 없음)"
    )

    item_url = clean_text(r.get("item_url")) or clean_text(r.get("source_url"))
    href = safe_href(item_url)

    author = clean_text(r.get("source_name")) or url_domain(item_url) or "-"
    desc = short_text(r.get("item_desc"), 20)

    title_html = (
        f'<a href="{href}" target="_blank" rel="noopener noreferrer">{html_text(item_title)}</a>'
        if href else
        html_text(item_title)
    )

    item_class = "result-item own" if is_own else "result-item"
    chip_class = "chip own" if is_own else "chip normal"
    chip_text = "자사" if is_own else "일반"

    return (
        f'<div class="{item_class}">'
        f'<div class="rank">{rank}</div>'
        f'<div>'
        f'<div class="item-title">{title_html}</div>'
        f'<div class="item-author">{html_text(author)}</div>'
        f'<div class="item-desc">{html_text(desc)}</div>'
        f'</div>'
        f'<div class="{chip_class}">{chip_text}</div>'
        f'</div>'
    )


def make_block_html(block_key, bd: pd.DataFrame) -> str:
    block_type = first_nonempty(bd["block_type"])
    block_label = block_type_label(block_type)
    block_order = int(first_nonempty(bd["block_order"]) or 0)

    block_title = first_nonempty(bd["block_title"]) or f"{block_label} {block_order}"

    own_cnt = int(bd["is_our_brand"].sum())
    total_cnt = len(bd)

    own_badge_class = "sb-badge own" if own_cnt > 0 else "sb-badge"

    item_html = "".join([make_item_html(r) for _, r in bd.iterrows()])

    return (
        f'<div class="sb-box">'
        f'<div class="sb-head">'
        f'<div>'
        f'<div class="sb-no">{html_text(block_label)} {block_order}</div>'
        f'<div class="sb-title">{html_text(block_title)}</div>'
        f'<div class="sb-type">{html_text(block_type)}</div>'
        f'</div>'
        f'<div class="sb-badges">'
        f'<span class="{own_badge_class}">자사 {own_cnt:,}/{total_cnt:,}</span>'
        f'</div>'
        f'</div>'
        f'{item_html}'
        f'</div>'
    )


def render_search_cards(df_search: pd.DataFrame, keyword: str, target_date):
    date_label = pd.to_datetime(target_date).strftime("%Y.%m.%d")

    html = [
        '<div class="search-wrap">',
        '<div class="search-bar">',
        '<div class="search-logo">N</div>',
        f'<div class="search-keyword">{html_text(keyword)}</div>',
        f'<div class="search-date">{html_text(date_label)} 수집 기준</div>',
        '</div>',
    ]

    if df_search.empty:
        html.extend([
            '<div class="empty-box">',
            '선택한 날짜에 표시할 검색결과 데이터가 없습니다.',
            '</div>',
            '</div>',
        ])
        st.markdown("".join(html), unsafe_allow_html=True)
        return

    block_keys = (
        df_search[
            ["block_type", "block_order"]
        ]
        .drop_duplicates()
        .sort_values(["block_order"])
        .to_dict("records")
    )

    block_cnt = max(len(block_keys), 1)
    html.append(
        f'<div class="sb-grid" style="grid-template-columns: repeat({block_cnt}, minmax(0, 1fr));">'
    )

    for b in block_keys:
        bd = df_search[
            (df_search["block_type"].eq(b["block_type"])) &
            (df_search["block_order"].eq(b["block_order"]))
        ]

        html.append(make_block_html(b, bd))

    html.append('</div>')
    html.append('</div>')

    st.markdown("".join(html), unsafe_allow_html=True)


def build_area_trend(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    area_order = ["스블1", "스블2", "스블3", "인플", "브콘"]

    mask = (
        df["block_type"].isin(["brandcontent", "intentblock", "influencer"])
        & df["item_order_in_block"].between(1, CFG["TOP_ITEM_ORDER"])
    )

    d = df.loc[
        mask,
        [
            "query",
            "start_date",
            "block_type",
            "block_order",
            "item_order_in_block",
            "global_item_order",
            "is_our_brand",
        ],
    ]

    if d.empty:
        return pd.DataFrame(), pd.DataFrame()

    blk = (
        d[["query", "start_date", "block_type", "block_order"]]
        .drop_duplicates()
        .sort_values(["query", "start_date", "block_order"], ascending=[True, True, True])
    )

    blk["intent_rank"] = pd.NA

    m_int = blk["block_type"].eq("intentblock")
    blk.loc[m_int, "intent_rank"] = (
        blk.loc[m_int]
        .groupby(["query", "start_date"])
        .cumcount() + 1
    )

    blk["area"] = ""
    blk.loc[blk["block_type"].eq("brandcontent"), "area"] = "브콘"
    blk.loc[blk["block_type"].eq("influencer"), "area"] = "인플"
    blk.loc[m_int, "area"] = "스" + blk.loc[m_int, "intent_rank"].astype(int).astype(str)

    blk = blk[blk["area"].isin(area_order)]

    d = d.merge(
        blk[["query", "start_date", "block_type", "block_order", "area"]],
        on=["query", "start_date", "block_type", "block_order"],
        how="inner",
    )

    daily = (
        d.groupby(["start_date", "area"], as_index=False)
        .agg(cnt=("is_our_brand", "sum"))
    )

    dt_list = sorted(d["start_date"].dropna().unique().tolist())

    base = pd.MultiIndex.from_product(
        [dt_list, area_order],
        names=["start_date", "area"],
    ).to_frame(index=False)

    daily = base.merge(daily, on=["start_date", "area"], how="left")
    daily["cnt"] = daily["cnt"].fillna(0).astype(int)
    daily["area"] = pd.Categorical(daily["area"], categories=area_order, ordered=True)
    daily = daily.sort_values(["area", "start_date"], ascending=[True, True])

    piv = (
        daily
        .pivot_table(
            index="area",
            columns="start_date",
            values="cnt",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(area_order)
    )

    return daily, piv

# ──────────────────────────────────
# main
# ──────────────────────────────────
def main():
    st.markdown(CFG["CSS_BLOCK_CONTAINER"], unsafe_allow_html=True)
    st.markdown(CFG["CSS_TABS"], unsafe_allow_html=True)

    st.markdown(
        '<p class="main-title">네이버 스마트블록 노출 모니터링</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="desc">선택한 키워드의 브랜드 콘텐츠, 스마트블록, 인플루언서 영역을 한 줄로 확인합니다. 자사 브랜드 결과는 노란색으로 표시합니다.</div>',
        unsafe_allow_html=True,
    )

    # ──────────────────────────────────
    # Sidebar
    # ──────────────────────────────────
    st.sidebar.header("Filter")

    today = datetime.now().date()
    default_end = today
    default_start = today - timedelta(days=CFG["DEFAULT_LOOKBACK_DAYS"])

    start_date, end_date = st.sidebar.date_input(
        "기간 선택",
        value=[default_start, default_end],
        max_value=today,
    )

    cs = start_date.strftime("%Y%m%d")
    ce = (end_date + timedelta(days=1)).strftime("%Y%m%d")

    # ──────────────────────────────────
    # Data Load
    # ──────────────────────────────────
    @st.cache_data(ttl=CFG["CACHE_TTL"])
    def load_data(cs: str, ce: str) -> pd.DataFrame:
        bq = BigQuery(
            projectCode="sleeper",
            custom_startDate=cs,
            custom_endDate=ce,
        )

        df = bq.get_data(CFG["TABLE_NAME"])

        if df.empty:
            return df

        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.normalize()
        df["source_date"] = pd.to_datetime(df["source_date"], errors="coerce")

        # BigQuery 래퍼에서 기간 필터가 event_date 기준으로 걸릴 수 있으므로 start_date 기준으로 재필터
        s_dt = pd.to_datetime(cs, format="%Y%m%d")
        e_dt = pd.to_datetime(ce, format="%Y%m%d") - timedelta(days=1)

        df = df[
            (df["start_date"] >= s_dt) &
            (df["start_date"] <= e_dt)
        ]

        int_cols = [
            "block_order",
            "item_order_in_block",
            "global_item_order",
            "is_our_brand",
        ]

        for c in int_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

        obj_cols = [
            "query",
            "block_type",
            "block_title",
            "source_name",
            "source_url",
            "item_title",
            "item_url",
            "item_desc",
        ]

        for c in obj_cols:
            if c in df.columns:
                df[c] = df[c].apply(clean_text)

        return df

    with st.spinner("로딩 중입니다..."):
        df_raw = load_data(cs, ce)

    if df_raw.empty:
        st.warning("조회 기간 내 데이터가 없습니다.")
        st.stop()

    query_opts = sorted(df_raw["query"].dropna().unique().tolist())

    if len(query_opts) == 0:
        st.warning("조회 가능한 키워드가 없습니다.")
        st.stop()

    st.divider()

    # ──────────────────────────────────
    # 검색결과 상세
    # ──────────────────────────────────
    st.markdown(
        '<p class="sub-title">검색결과 상세</p>',
        unsafe_allow_html=True,
    )

    c1, c2 = st.columns([2, 1])

    with c1:
        sel_keyword = st.selectbox(
            "키워드 선택",
            query_opts,
            index=0,
        )

    keyword_dates = sorted(
        df_raw[df_raw["query"].eq(sel_keyword)]["start_date"].dropna().unique().tolist()
    )

    if len(keyword_dates) == 0:
        st.warning("선택한 키워드의 수집일 데이터가 없습니다.")
        st.stop()

    with c2:
        sel_date = st.selectbox(
            "검색 기준일",
            keyword_dates,
            index=len(keyword_dates) - 1,
            format_func=lambda x: pd.to_datetime(x).strftime("%m월 %d일"),
        )

    df_search = build_search_result(df_raw, sel_keyword, sel_date)

    render_search_cards(df_search, sel_keyword, sel_date)





    # ──────────────────────────────────
    # 노출 추이
    # ──────────────────────────────────
    st.markdown(
        '<p class="sub-title">노출 추이</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="desc">block_order를 기준으로 intentblock끼리만 순위를 다시 매겨 스블1·스블2·스블3으로 집계하고, 인플·브콘과 함께 날짜별 자사 노출 개수를 확인합니다.</div>',
        unsafe_allow_html=True,
    )

    sel_tr_queries = st.multiselect(
        "추이 키워드 선택",
        query_opts,
        default=query_opts,
        key="trend_queries",
    )

    df_tr = df_raw[df_raw["query"].isin(sel_tr_queries)]

    daily_tr, piv_tr = build_area_trend(df_tr)

    if daily_tr.empty or piv_tr.empty:
        st.info("추이로 볼 데이터가 없습니다.")
    else:
        x_vals = [pd.to_datetime(c).strftime("%m-%d") for c in piv_tr.columns.tolist()]
        y_vals = piv_tr.index.tolist()
        z_vals = piv_tr.values

        fig = go.Figure(
            data=go.Heatmap(
                z=z_vals,
                x=x_vals,
                y=y_vals,
                colorscale="YlOrRd",
                text=z_vals,
                texttemplate="%{text}",
                hovertemplate="구분=%{y}<br>날짜=%{x}<br>노출수=%{z}<extra></extra>",
                colorbar=dict(title="노출수"),
            )
        )

        fig.update_layout(
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="날짜",
            yaxis_title="구분",
        )

        st.plotly_chart(fig, use_container_width=True)

        df_tbl = piv_tr.reset_index().rename(columns={"area": "구분"})
        df_tbl.columns = ["구분"] + [
            pd.to_datetime(c).strftime("%m월 %d일")
            for c in piv_tr.columns.tolist()
        ]

        st.dataframe(
            df_tbl,
            use_container_width=True,
            hide_index=True,
        )

if __name__ == "__main__":
    main()