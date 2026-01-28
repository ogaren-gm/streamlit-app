# SEOHEE
# 2026-01-27 ver.

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
from modules.style import style_format


# ──────────────────────────────────
# 공통 유틸 함수
# ──────────────────────────────────
def add_period_columns(df: pd.DataFrame, date_col: str, mode: str) -> pd.DataFrame:
    """기간 파생 컬럼(_period_dt, _period) 생성.
    일별/주별 기준으로 라벨과 정렬용 datetime을 만든다."""
    w = df.copy()
    w[date_col] = pd.to_datetime(w[date_col], errors="coerce")
    w = w.dropna(subset=[date_col])

    if mode == "일별":
        dt = w[date_col].dt.floor("D")
        w["_period_dt"] = dt
        w["_period"] = dt.dt.strftime("%Y-%m-%d")
    else:
        ws = w[date_col].dt.floor("D") - pd.to_timedelta(w[date_col].dt.weekday, unit="D")
        we = ws + pd.to_timedelta(6, unit="D")
        w["_period_dt"] = ws
        w["_period"] = ws.dt.strftime("%Y-%m-%d") + " ~ " + we.dt.strftime("%Y-%m-%d")

    return w


def sort_period_labels(cols: list) -> list:
    """기간 라벨을 시작일 기준으로 정렬.
    datetime/문자열(YYYY-MM-DD, YYYY-MM-DD ~ YYYY-MM-DD) 모두 지원."""
    def _key(v):
        if isinstance(v, (pd.Timestamp, datetime, np.datetime64)):
            return pd.to_datetime(v, errors="coerce")
        s = str(v)
        s0 = s.split(" ~ ", 1)[0].strip() if " ~ " in s else s.strip()
        return pd.to_datetime(s0, errors="coerce")

    return sorted(cols, key=_key)


def get_topk_values(s: pd.Series, k: int) -> list[str]:
    """빈값 제외 후 빈도 상위 k개 값 반환."""
    vc = s.replace("", np.nan).dropna().value_counts()
    return vc.head(k).index.tolist()


def add_weekend_shading(fig, x_vals: pd.Series):
    """일별 datetime 축에서 주말(토/일) 음영 추가.
    정오~정오 방식 유지."""
    xs = pd.to_datetime(pd.Series(x_vals), errors="coerce").dropna().dt.floor("D").unique()

    for d in xs:
        d = pd.Timestamp(d).date()
        start = datetime.combine(d, datetime.min.time()) + timedelta(hours=12)
        end = start + timedelta(hours=24)

        if d.weekday() == 4:
            fig.add_vrect(x0=start, x1=end, fillcolor="blue", opacity=0.05, layer="below", line_width=0)
        elif d.weekday() == 5:
            fig.add_vrect(x0=start, x1=end, fillcolor="red", opacity=0.05, layer="below", line_width=0)


def _is_datetime_like(s: pd.Series) -> bool:
    """Series가 datetime 계열처럼 보이면 True."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return True
    try:
        return bool(pd.to_datetime(s.dropna().iloc[:5], errors="coerce").notna().all())
    except Exception:
        return False


def _looks_weekly_dt(x_dt: pd.Series) -> bool:
    """주 시작일 단위 datetime처럼 보이면 True.
    날짜 간격 중앙값이 6일 이상."""
    x_u = pd.to_datetime(pd.Series(x_dt), errors="coerce").dropna().dt.floor("D").drop_duplicates().sort_values()
    if len(x_u) <= 2:
        return False
    diffs = x_u.diff().dropna().dt.days
    return False if diffs.empty else float(diffs.median()) >= 6.0


# ──────────────────────────────────
# 공통 렌더 함수
# ──────────────────────────────────
def render_line_graph(df: pd.DataFrame, x: str, y: list[str] | str, height: int = 360, title: str | None = None, key: str | None = None) -> None:
    """라인 차트 렌더링.
    일별 datetime 축일 때만 주말 음영 적용."""
    y_cols = [y] if isinstance(y, str) else y

    fig = px.line(df, x=x, y=y_cols, markers=True, labels={"variable": ""}, title=title)
    fig.update_traces(hovertemplate="%{fullData.name}<br>값: %{y:,.0f}<extra></extra>")

    use_dt = (x in df.columns) and _is_datetime_like(df[x])

    if use_dt:
        x_dt = pd.to_datetime(df[x], errors="coerce")
        if len(x_dt) and not _looks_weekly_dt(x_dt):
            try:
                add_weekend_shading(fig, x_dt)
            except Exception:
                pass
        fig.update_xaxes(type="date", tickformat="%Y-%m-%d")
    else:
        fig.update_xaxes(type="category")

    fig.update_layout(
        height=height,
        xaxis_title=None,
        yaxis_title=None,
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
        legend_title_text="",
    )

    st.plotly_chart(fig, use_container_width=True, key=key)


def render_stack_graph(df: pd.DataFrame, x: str, y: str, color: str, height: int = 360, opacity: float = 0.6, title: str | None = None, show_value_in_hover: bool = False, key: str | None = None) -> None:
    """누적 막대 차트 렌더링.
    일별 datetime 축일 때만 주말 음영 적용."""
    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    d = df.copy()
    d[y] = pd.to_numeric(d[y], errors="coerce").fillna(0)

    tot = d.groupby(x, dropna=False)[y].transform("sum").replace(0, np.nan)
    d["_share_pct"] = ((d[y] / tot) * 100).fillna(0)

    use_dt = (x in d.columns) and _is_datetime_like(d[x])
    x_plot = x
    x_cat_order = None

    if use_dt:
        d[x_plot] = pd.to_datetime(d[x_plot], errors="coerce")
        d = d.dropna(subset=[x_plot])
    else:
        s = d[x_plot].astype(str)
        x_cat_order = sort_period_labels(s.dropna().unique().tolist())
        d[x_plot] = pd.Categorical(s, categories=x_cat_order, ordered=True)
        d = d.sort_values(x_plot).reset_index(drop=True)

    fig = px.bar(d, x=x_plot, y=y, color=color, barmode="relative", opacity=opacity, title=title, custom_data=[color, "_share_pct", y])
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))

    if show_value_in_hover:
        fig.update_traces(hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]:.1f}%<br>값: %{customdata[2]:,.0f}<extra></extra>")
    else:
        fig.update_traces(hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]:.1f}%<extra></extra>")

    if use_dt:
        fig.update_xaxes(type="date", tickformat="%Y-%m-%d")
        x_dt = pd.to_datetime(d[x_plot], errors="coerce")
        if len(x_dt) and not _looks_weekly_dt(x_dt):
            try:
                add_weekend_shading(fig, x_dt)
            except Exception:
                pass
    else:
        fig.update_xaxes(type="category")
        if x_cat_order:
            fig.update_xaxes(categoryorder="array", categoryarray=x_cat_order)

    fig.update_layout(
        height=height,
        xaxis_title=None,
        yaxis_title=None,
        legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom"),
        legend_title_text="",
        bargap=0.5,
        bargroupgap=0.1,
    )
    
    st.plotly_chart(fig, use_container_width=True, key=key)


def build_pivot_table(long: pd.DataFrame, index_col: str, col_col: str, val_col: str) -> pd.DataFrame:
    """long 데이터를 wide pivot으로 변환.
    기간 컬럼 정렬 후 합계 기준으로 행 정렬."""
    pv = long.pivot_table(index=index_col, columns=col_col, values=val_col, aggfunc="sum", fill_value=0).reset_index()

    period_cols = [c for c in pv.columns if c != index_col]
    rename_map = {}
    for c in period_cols:
        if isinstance(c, (pd.Timestamp, datetime, np.datetime64)):
            cc = pd.to_datetime(c, errors="coerce")
            if pd.notna(cc):
                rename_map[c] = cc.strftime("%Y-%m-%d")
    if rename_map:
        pv = pv.rename(columns=rename_map)

    ordered = sort_period_labels([c for c in pv.columns if c != index_col])
    pv = pv[[index_col] + ordered]

    num_cols = [c for c in ordered if pd.api.types.is_numeric_dtype(pv[c])]
    if num_cols:
        pv["_sum"] = pv[num_cols].sum(axis=1)
        pv = pv.sort_values("_sum", ascending=False).drop(columns=["_sum"])

    return pv


def render_table(df_wide: pd.DataFrame, index_col: str, decimals: int = 0):
    styled = style_format(df_wide, decimals_map={c: decimals for c in df_wide.columns if c != index_col})
    st.dataframe(styled, row_height=30, hide_index=True)
