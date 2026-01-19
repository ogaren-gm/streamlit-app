# modules/ui_common.py
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
    """
    날짜 컬럼을 기준으로 분석용 기간 컬럼을 생성하는 공통 유틸 함수.

    기능:
    - date_col을 datetime으로 안전하게 변환 (yyyymmdd, yyyy-mm-dd 형식 모두 처리)
    - 일별 / 주별 기준으로 기간 라벨 생성

    결과:
    - _period_dt : 정렬·집계용 datetime 컬럼
    - _period    : 화면 표시용 기간 문자열
        - 일별  → YYYY-MM-DD
        - 주별  → YYYY-MM-DD ~ YYYY-MM-DD (월~일)
    """
    w = df.copy()

    # 문자열/숫자 혼합 날짜 → datetime (yyyymmdd, yyyy-mm-dd 모두 허용)
    w[date_col] = pd.to_datetime(w[date_col], errors="coerce")
    w = w.dropna(subset=[date_col])

    if mode == "일별":
        dt = w[date_col].dt.floor("D")
        w["_period_dt"] = dt
        w["_period"] = dt.dt.strftime("%m월 %d일")
    else:
        ws = w[date_col].dt.floor("D") - pd.to_timedelta(w[date_col].dt.weekday, unit="D")
        we = ws + pd.to_timedelta(6, unit="D")
        w["_period_dt"] = ws
        w["_period"] = (
            ws.dt.strftime("%m월 %d일")
            + " ~ "
            + we.dt.strftime("%m월 %d일")
        )


    return w



def sort_period_labels(cols: list[str]) -> list[str]:
    """
    기간 컬럼을 시간 순서대로 정렬하는 유틸 함수.

    기능
    - pivot 결과처럼 기간이 가로 컬럼이 되었을 때 사용
    - 일별(YYYY-MM-DD) / 주별(YYYY-MM-DD ~ YYYY-MM-DD) 형식을 모두 처리

    결과
    - 시간 흐름에 맞는 컬럼 순서를 보장
    """
    return sorted(cols, key=lambda x: x.split(" ~ ")[0] if " ~ " in x else x)


def get_topk_values(s: pd.Series, k: int) -> list[str]:
    """
    시리즈에서 등장 빈도 기준 Top K 값 목록을 반환하는 함수.

    기능
    - 빈 문자열("")는 제외
    - 결측값(NaN)은 제외
    - value_counts 기준으로 상위 K개 추출

    결과
    - 빈도순으로 정렬된 문자열 리스트 (길이 ≤ K)
    """
    vc = s.replace("", np.nan).dropna().value_counts()
    return vc.head(k).index.tolist()


def add_weekend_shading(fig, x_vals: pd.Series):
    """
    날짜 축을 기준으로 주말(토/일) 구간에 음영(vrect)을 추가하는 함수.

    기능
    - x축 날짜 값에서 일 단위 날짜를 추출
    - 토요일 / 일요일에 해당하는 구간에만 배경 음영 표시
    - 그래프 중앙 기준(정오~다음날 정오)으로 vrect 적용

    결과
    - Plotly 그래프에 주말 구간이 시각적으로 강조됨
    """
    xs = pd.to_datetime(pd.Series(x_vals), errors="coerce").dropna().dt.floor("D").unique()

    for d in xs:
        d = pd.Timestamp(d).date()
        start = datetime.combine(d, datetime.min.time()) + timedelta(hours=12)
        end = start + timedelta(hours=24)

        if d.weekday() == 4:      # 토요일
            fig.add_vrect(
                x0=start, x1=end,
                fillcolor="blue", opacity=0.05,
                layer="below", line_width=0
            )
        elif d.weekday() == 5:    # 일요일
            fig.add_vrect(
                x0=start, x1=end,
                fillcolor="red", opacity=0.05,
                layer="below", line_width=0
            )


# ──────────────────────────────────
# 공통 렌더 함수
# ──────────────────────────────────
def render_line_graph(df: pd.DataFrame, x: str, y: list[str] | str, height: int = 360, title: str | None = None) -> None:
    """
    기간 추이를 라인 차트로 시각화하는 공통 렌더 함수.

    기능
    - px.line 기반 라인 차트 생성 (마커 포함)
    - 단일 y / 복수 y 컬럼 모두 지원
    - 주말(토·일) 구간 음영 자동 표시
    - 범례를 그래프 상단에 고정해 겹침 방지
    - x축 날짜 포맷을 '%m월 %d일'로 통일

    결과
    - Streamlit 화면에 일관된 스타일의 추이 그래프 출력
    """
    # 단일 y → 리스트로 통일
    y_cols = [y] if isinstance(y, str) else y

    # 라인 차트 생성
    fig = px.line(
        df,
        x=x,
        y=y_cols,
        markers=True,
        labels={"variable": ""},
        title=title,
    )

    # ✅ hover 값: 지표명 + 천단위 콤마
    fig.update_traces(
        hovertemplate="%{fullData.name}<br>값: %{y:,.0f}<extra></extra>"
    )




    # 주말 음영 추가 (주별 라벨이면 적용 안 함)
    try:
        x0 = df[x].astype(str).iloc[0] if len(df) else ""
    except Exception:
        x0 = ""

    if " ~ " not in str(x0):  # 일별(YYYY-MM-DD)만 shading
        add_weekend_shading(fig, df[x])


    # 레이아웃 통일 (범례 상단 고정)
    fig.update_layout(
        height=height,
        xaxis_title=None,
        yaxis_title=None,
        legend=dict(
            orientation="h",
            y=1.02,
            x=1,
            xanchor="right",
            yanchor="bottom",
        ),
        legend_title_text="",   # ✅ 추가
    )

    # x축 날짜 표시 포맷
    fig.update_xaxes(tickformat="%m월 %d일")

    # Streamlit 렌더
    st.plotly_chart(fig, use_container_width=True)


def render_stack_graph(df: pd.DataFrame, x: str, y: str, color: str, height: int = 360, opacity: float = 0.6, title: str | None = None, show_value_in_hover: bool = False, key: str | None = None) -> None:
    """
    기간 추이를 누적 막대그래프로 시각화하는 공통 렌더 함수.

    기능
    - px.bar(stacked) 기반 누적 막대 그래프 생성
    - x별 비중(%) 계산 후 hover에 표시
    - (옵션) hover에 원값(value) 함께 표시 가능
    - 주말(토·일) 구간 음영 자동 표시 (일별일 때만)
    - 범례 상단 고정
    - x축 날짜 포맷 '%m월 %d일' 적용 (일별일 때만)
    """
    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    d = df.copy()

    # --- x별 비중 계산 ---
    d[y] = pd.to_numeric(d[y], errors="coerce").fillna(0)
    tot = d.groupby(x, dropna=False)[y].transform("sum").replace(0, np.nan)
    d["_share_pct"] = ((d[y] / tot) * 100).fillna(0)

    # ✅ 핵심: customdata mismatch 방지
    # - px가 color별로 trace를 쪼개므로, update_traces(customdata=np.stack(...))를 쓰면 어긋날 수 있음
    # - 반드시 px.bar(custom_data=...)로 row 단위 매핑을 태워야 함
    fig = px.bar(
        d,
        x=x,
        y=y,
        color=color,
        barmode="relative",
        opacity=opacity,
        title=title,
        custom_data=[color, "_share_pct", y],
    )

    if show_value_in_hover:
        fig.update_traces(
            hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]:.1f}%<br>값: %{customdata[2]:,.0f}<extra></extra>"
        )
    else:
        fig.update_traces(
            hovertemplate="%{customdata[0]}<br>비중: %{customdata[1]:.1f}%<extra></extra>"
        )

    # ✅ [CHANGED] 일별/주별 판단을 dtype가 아니라 " ~ " 여부로 통일 (render_line_graph와 동일)
    try:
        x0 = d[x].astype(str).iloc[0] if len(d) else ""
    except Exception:
        x0 = ""

    if " ~ " not in str(x0):  # 일별(YYYY-MM-DD)만 shading + 00월 00일 포맷
        add_weekend_shading(fig, d[x])
        fig.update_xaxes(tickformat="%m월 %d일")

    # 레이아웃 통일
    fig.update_layout(
        height=height,
        xaxis_title=None,
        yaxis_title=None,
        legend=dict(
            orientation="h",
            y=1.02,
            x=1,
            xanchor="right",
            yanchor="bottom",
        ),
        legend_title_text="",      # ✅ 범례 제목 제거
        bargap=0.5,
        bargroupgap=0.1,
    )

    st.plotly_chart(fig, use_container_width=True, key=key)



def build_pivot_table(long: pd.DataFrame, index_col: str, col_col: str, val_col: str) -> pd.DataFrame:
    """
    long(df) → wide(pivot)로 변환한 뒤, 기간 컬럼을 시간순으로 정렬하고
    행은 전체 합계 기준으로 내림차순 정렬해 반환합니다.

    결과
    - index_col: 행(카테고리/항목)
    - col_col:   열(기간/날짜)
    - val_col:   값(집계값)
    """
    # 1) long → wide (없으면 0으로 채움)
    pv = (
        long.pivot_table(
            index=index_col,
            columns=col_col,
            values=val_col,
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    # 2) 기간(열)만 뽑아서 시간순 정렬 후 재배치
    period_cols = [c for c in pv.columns if c != index_col]
    pv = pv[[index_col] + sort_period_labels(period_cols)]
    
    # ✅ 합계/정렬은 "숫자 컬럼"만 대상으로 수행 (Timestamp 섞여도 안전)
    num_cols = [c for c in period_cols if pd.api.types.is_numeric_dtype(pv[c])]

    if num_cols:
        pv["_sum"] = pv[num_cols].sum(axis=1)
        pv = pv.sort_values("_sum", ascending=False).drop(columns=["_sum"])

    return pv


def render_table(df_wide: pd.DataFrame, index_col: str, decimals: int = 0):
    """
    wide 형태의 DataFrame을 표로 렌더링합니다.

    기능
    - index_col을 제외한 모든 숫자 컬럼에 소수점 포맷 적용
    - 공통 style_format을 사용해 숫자 표현 통일
    - Streamlit dataframe으로 출력

    결과
    - 그래프 하단에 바로 붙여 쓰기 좋은 정렬된 테이블
    """
    # index_col을 제외한 숫자 컬럼에 동일한 소수점 포맷 적용
    styled = style_format(
        df_wide,
        decimals_map={c: decimals for c in df_wide.columns if c != index_col},
    )

    # 표 렌더링 (index 숨김, 행 높이 고정)
    st.dataframe(styled, row_height=30, hide_index=True)
