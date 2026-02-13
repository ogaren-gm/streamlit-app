# SEOHEE
# 2026-02-11 ver.

from __future__ import annotations
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta


# ──────────────────────────────────
# TRANSFORM (데이터 변환/정렬)
# ──────────────────────────────────
def add_period_columns(df: pd.DataFrame, date_col: str, mode: str) -> pd.DataFrame:
    """
    _period_dt와 _period를 만듦
    
    - date_col을 datetime으로 만들고
    - _period_dt (정렬용 datetime), _period (표시용 라벨) 생성
    - 일별이면 YYYY-MM-DD, 주별이면 YYYY-MM-DD ~ YYYY-MM-DD
    """
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
    """
    기간 컬럼을 "시작일 기준"으로 정렬
    
    - datetime 지원
    - 문자열(YYYY-MM-DD, YYYY-MM-DD ~ YYYY-MM-DD) 지원
    """
    def _key(v):
        if isinstance(v, (pd.Timestamp, datetime, np.datetime64)):
            return pd.to_datetime(v, errors="coerce")
        s = str(v)
        s0 = s.split(" ~ ", 1)[0].strip() if " ~ " in s else s.strip()
        return pd.to_datetime(s0, errors="coerce")

    return sorted(cols, key=_key)


def build_pivot_table(long: pd.DataFrame, index_col: str, col_col: str, val_col: str) -> pd.DataFrame:
    """
    long -> wide pivot
    
    - 만약 기간 컬럼이 datetime이면, 문자열 YYYY-MM-DD로 통일
    - sort_period_labels()로 컬럼 정렬 (기간 합계 기준으로 행 내림차순 정렬)
    """
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


def get_topk_values(s: pd.Series, k: int) -> list[str]:
    """
    Top K 생성 함수
    
    - 빈값 제외
    - value_counts(빈도) 기준 상위 k개 값 리스트 반환
    """
    vc = s.replace("", np.nan).dropna().value_counts()
    return vc.head(k).index.tolist()


# ──────────────────────────────────
# VIZ (Plotly)
# ──────────────────────────────────
def add_weekend_shading(fig, x_vals: pd.Series):
    """
    주말 셰이딩
    
    - x축 날짜를 기준
    - 토/일요일에 정오~정오 구간으로 vrect 음영 넣음
    """
    xs = pd.to_datetime(pd.Series(x_vals), errors="coerce").dropna().dt.floor("D").unique()

    for d in xs:
        d = pd.Timestamp(d).date()
        start = datetime.combine(d, datetime.min.time()) + timedelta(hours=12)
        end = start + timedelta(hours=24)

        if d.weekday() == 4:
            fig.add_vrect(x0=start, x1=end, fillcolor="blue", opacity=0.05, layer="below", line_width=0)
        elif d.weekday() == 5:
            fig.add_vrect(x0=start, x1=end, fillcolor="red", opacity=0.05, layer="below", line_width=0)


def _isWeeklyPeriod(x_dt: pd.Series) -> bool:
    """
    부울 함수

    - 날짜 간격 중앙값이 6일 이상이면 주 단위 데이터로 간주하고 True 반환 
    - 주 단위일 때는 일별 주말 음영을 안 넣기 위해. 
    """
    x_u = pd.to_datetime(pd.Series(x_dt), errors="coerce").dropna().dt.floor("D").drop_duplicates().sort_values()
    if len(x_u) <= 2:
        return False
    diffs = x_u.diff().dropna().dt.days
    return False if diffs.empty else float(diffs.median()) >= 6.0


def _isDatetime(s: pd.Series) -> bool:
    """
    부울 함수
    
    - Series가 datetime dtpye 이거나, 앞 5개의 샘플이 전부 datetime 변환 가능하면 True 반환
    """
    if pd.api.types.is_datetime64_any_dtype(s):
        return True
    try:
        return bool(pd.to_datetime(s.dropna().iloc[:5], errors="coerce").notna().all())
    except Exception:
        return False


def render_line_graph(
    df: pd.DataFrame,
    x: str,
    y: list[str] | str,
    height: int = 360,
    title: str | None = None,
    key: str | None = None
) -> None:
    """
    라인 차트 렌더링 (px.line)

    - hover 포맷 포함
    - 범례 상단 포함
    - 주 단위 제외 일 단위 일때만 셰이딩 포함
    """
    y_cols = [y] if isinstance(y, str) else y

    fig = px.line(df, x=x, y=y_cols, markers=True, labels={"variable": ""}, title=title)
    fig.update_traces(hovertemplate="%{fullData.name}<br>값: %{y:,.0f}<extra></extra>")

    use_dt = (x in df.columns) and _isDatetime(df[x])

    if use_dt:
        x_dt = pd.to_datetime(df[x], errors="coerce")

        # ✅ x축 tick을 "유니크 날짜"로 강제 (중복 라벨 방지)
        x_u = (
            x_dt.dt.floor("D")
               .dropna()
               .drop_duplicates()
               .sort_values()
        )
        if len(x_u):
            fig.update_xaxes(
                tickmode="array",
                tickvals=x_u,
                ticktext=[ts.strftime("%Y-%m-%d") for ts in x_u],
            )

        if len(x_dt) and not _isWeeklyPeriod(x_dt):
            try:
                add_weekend_shading(fig, x_dt)
            except Exception:
                pass

        fig.update_xaxes(type="date")
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


def render_stack_graph(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str,
    height: int = 360,
    opacity: float = 0.6,
    title: str | None = None,
    show_value_in_hover: bool = False,
    key: str | None = None,
    **px_kwargs,   # ✅ 추가: plotly express kwargs 통과
) -> None:
    """
    누적막대 차트 렌더링 (px.bar)

    - hover 포맷 포함
    - 범례 상단 포함 ?
    - 주 단위 제외 일 단위 일때만 셰이딩 포함
    """
    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    d = df.copy()
    d[y] = pd.to_numeric(d[y], errors="coerce").fillna(0)

    tot = d.groupby(x, dropna=False)[y].transform("sum").replace(0, np.nan)
    d["_share_pct"] = ((d[y] / tot) * 100).fillna(0)

    use_dt = (x in d.columns) and _isDatetime(d[x])
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

    fig = px.bar(
        d,
        x=x_plot,
        y=y,
        color=color,
        barmode="stack", #relative?
        opacity=opacity,
        title=title,
        custom_data=[color, "_share_pct", y],
        **px_kwargs,  # ✅ 추가: 외부에서 준 color_discrete_map/sequence/orders 받음
    )
    fig.for_each_trace(lambda t: t.update(offsetgroup="__stack__", alignmentgroup="__stack__"))

    if show_value_in_hover:
        fig.update_traces(hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]:.1f}%<br>값: %{customdata[2]:,.0f}<extra></extra>")
    else:
        fig.update_traces(hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]:.1f}%<extra></extra>")

    if use_dt:
        x_dt = pd.to_datetime(d[x_plot], errors="coerce")

        # ✅ x축 tick을 "유니크 날짜"로 강제 (중복 라벨 방지)
        x_u = (
            x_dt.dt.floor("D")
               .dropna()
               .drop_duplicates()
               .sort_values()
        )
        if len(x_u):
            fig.update_xaxes(
                tickmode="array",
                tickvals=x_u,
                ticktext=[ts.strftime("%Y-%m-%d") for ts in x_u],
            )

        fig.update_xaxes(type="date")

        if len(x_dt) and not _isWeeklyPeriod(x_dt):
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


# ──────────────────────────────────
# STYLE
# ──────────────────────────────────
def style_format(
    df: pd.DataFrame,
    decimals_map: dict,
    suffix_map: dict | None = None,
    thousands: str = ",",
):
    """
    데이터프레임 숫자 포맷 유틸

    기능
    - decimals_map에 지정된 컬럼에만 숫자 포맷(천단위/소수자릿수)을 적용
    - suffix_map에 지정된 컬럼은 접미사(예: " %")를 붙여 표시
    - 숫자 컬럼 우측 정렬 강제 (문자 컬럼은 그대로)

    파라미터
    - decimals_map : {컬럼명: 소수자릿수(int)}  ※ MultiIndex 튜플도 키로 가능
    - suffix_map   : {컬럼명: 접미사 문자열} (옵션)
    - thousands    : 천단위 구분자(기본 ",")
    """
    
    suffix_map = suffix_map or {}
    formatter: dict = {}
    num_cols: list = []

    def _isNumeric(col) -> bool: # dtype이 숫자이거나, object라도 95% 이상 숫자 변환 가능하면 숫자 컬럼으로 취급
        if col not in df.columns:
            return False
        if pd.api.types.is_numeric_dtype(df[col]):
            return True

        s = df[col]
        nn = s.notna()
        if nn.sum() == 0:
            return False

        conv = pd.to_numeric(s[nn], errors="coerce")
        return float(conv.notna().mean()) >= 0.95

    def _fmt_num(v, d: int, sfx: str = "") -> str: # 함수 포맷터: 접미사 붙이기/문자 섞임 안전/NaN 안전
        if pd.isna(v):
            return ""
        try:
            vv = float(v)
            if not np.isfinite(vv):
                return ""
            return f"{vv:,.{d}f}{sfx}"
        except Exception:
            return str(v)

    for col, dec in (decimals_map or {}).items():
        if col not in df.columns:
            continue

        d = int(dec)

        # 1) 접미사 컬럼: 무조건 함수 포맷터(문자 섞여도 안전)
        if col in suffix_map:
            sfx = suffix_map[col]
            formatter[col] = (lambda d=d, sfx=sfx: (lambda v: _fmt_num(v, d, sfx)))()
            continue

        # 2) 숫자 컬럼: 포맷 문자열 적용(문자 섞이면 터질 수 있으니 판정 필수)
        if _isNumeric(col):
            formatter[col] = f"{{:,.{d}f}}"
            num_cols.append(col)

    styler = df.style.format(formatter=formatter, thousands=thousands)

    # 3) 숫자 컬럼 우측 정렬 강제
    if num_cols:
        styler = styler.set_properties(subset=pd.IndexSlice[:, num_cols], **{"text-align": "right"})

    return styler


def style_cmap(
    df_or_styler,
    gradient_rules: list[dict],
    *,
    default_cmap: str = "OrRd",
    na_color: str = "#ffffff",
) -> pd.io.formats.style.Styler:
    """
    - cmap_span : 컬러맵 일부 구간만 사용 (색 강도 제어)
    - robust_clip : 분위수 기반 min/max (이상치 영향 제거)
    - pad_ratio : 데이터 범위 기준 여백 추가
    - zero_as_white : 0 값은 흰색 처리

    ※ vmin / vmax 사용자 지정 기능 제거
    """

    # --- styler 확보 ---
    if isinstance(df_or_styler, pd.io.formats.style.Styler):
        styler = df_or_styler
        df = styler.data
    else:
        df = df_or_styler
        styler = df.style

    idx = pd.IndexSlice
    rows = df.index

    # --- cmap 일부 구간만 사용 ---
    from matplotlib import cm
    from matplotlib.colors import LinearSegmentedColormap

    def _slice_cmap(cmap_name: str, span: tuple[float, float]):
        a, b = span
        a = float(max(0.0, min(1.0, a)))
        b = float(max(0.0, min(1.0, b)))
        if b < a:
            a, b = b, a
        if abs(b - a) < 1e-9:
            b = min(1.0, a + 1e-3)

        base = cm.get_cmap(cmap_name)
        xs = np.linspace(a, b, 256)
        colors = base(xs)
        return LinearSegmentedColormap.from_list(f"{cmap_name}_{a:.2f}_{b:.2f}", colors)

    # --- 메인 ---
    for sp in gradient_rules:
        cols = sp.get("cols")
        col = sp.get("col")

        if cols is not None:
            targets = [c for c in cols if c in df.columns]
        elif col is not None and col in df.columns:
            targets = [col]
        else:
            continue

        cmap_name   = sp.get("cmap", default_cmap)
        cmap_span   = sp.get("cmap_span", (0.0, 1.0))
        robust_clip = sp.get("robust_clip", None)     # (q_low, q_high)
        pad_ratio   = sp.get("pad_ratio", (0.0, 0.0)) # (pad_low, pad_high)
        zero_as_white = sp.get("zero_as_white", True)
        low  = sp.get("low", 0.0)
        high = sp.get("high", 0.0)

        # cmap span 적용
        try:
            cmap_obj = _slice_cmap(cmap_name, cmap_span)
        except Exception:
            cmap_obj = cmap_name

        for c in targets:
            s = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan)
            s_finite = s.dropna()
            if s_finite.empty:
                continue

            # --- 데이터 기준 min/max ---
            if robust_clip and isinstance(robust_clip, (tuple, list)) and len(robust_clip) == 2:
                ql, qh = robust_clip
                ql = max(0.0, min(1.0, ql))
                qh = max(0.0, min(1.0, qh))
                if qh < ql:
                    ql, qh = qh, ql
                vmin_raw = float(s_finite.quantile(ql))
                vmax_raw = float(s_finite.quantile(qh))
            else:
                nonzero_min = s_finite[s_finite != 0].min()
                vmin_raw = float(nonzero_min) if pd.notna(nonzero_min) else float(s_finite.min())
                vmax_raw = float(s_finite.max())

            # --- pad_ratio 적용 ---
            if isinstance(pad_ratio, (tuple, list)) and len(pad_ratio) == 2:
                pl, ph = pad_ratio
            else:
                pl, ph = 0.0, 0.0

            span = vmax_raw - vmin_raw
            if np.isfinite(span) and span != 0:
                vmin_c = vmin_raw - span * pl
                vmax_c = vmax_raw + span * ph
            else:
                vmin_c, vmax_c = vmin_raw, vmax_raw

            # 안전 처리
            if pd.isna(vmin_c) or pd.isna(vmax_c):
                continue
            if vmin_c > vmax_c:
                vmin_c, vmax_c = vmax_c, vmin_c

            styler = styler.background_gradient(
                subset=idx[rows, [c]],
                cmap=cmap_obj,
                vmin=vmin_c,
                vmax=vmax_c,
                low=low,
                high=high,
                text_color_threshold=0
            )

            # --- 0 흰색 ---
            if zero_as_white:
                styler = styler.apply(
                    lambda col_: [
                        f"background-color: {na_color}" if pd.to_numeric(v, errors="coerce") == 0 else ""
                        for v in col_
                    ],
                    subset=idx[rows, [c]],
                )

    return styler
