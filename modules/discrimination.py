# Discrimination | 판별 로직
import numpy as np
import pandas as pd
import plotly.graph_objects as go

def build_scroll_exit_fig(
    df: pd.DataFrame,
    col_depth: str = "depth",
    col_rate: str = "직전구간이탈(%)",
    # [ 전반 탐지 ]
    early_max: int = 70,    # 전반부를 어디까지 볼지(<=70)
    z_k: float = 1.5,       # 임계값 계산에서 표준편차 계수(μ + z_k·σ)
    # [ 후반 탐지 ]
    tail_min: int = 60,     # 후반 탐색 시작 depth(>=60)
    n_pos: int = 2,         # 전환점 이후 연속 양수(+)를 몇 번 요구할지
    # 경계 충돌 방지
    min_gap: int = 10,      # b1/b2가 겹치거나 역전될 때 최소 간격(기본 10)
    # shading -> 3구간
    shade_rgba: tuple[str, str, str] = ( # 좌/중/우 구간 색(투명도 포함)
        "rgba(0, 128, 0, 0.08)",
        "rgba(255, 165, 0, 0.10)",
        "rgba(255, 0, 0, 0.10)",
    ),
    shade_y0: float = 0.0,
    shade_y1: float = 1.0,
    shade_layer: str = "below",
    # 라벨(주석) 표시
    show_annotations: bool = True,
    ann_y: float = 1.02,
    ann_font_size: int = 12,
    ann_text_1: str = "이탈 1차: {b1}%",
    ann_text_2: str = "이탈 2차: {b2}%",
    # series style
    mode: str = "markers+lines",
    fill: str = "tozeroy",
    line_opacity: float = 0.55,
    hovertemplate: str = "Depth %{x}%<br>직전구간이탈 %{y:.2f}%<extra></extra>",
    # vline style
    show_vlines: bool = True,
    vline_width: int = 1,
    vline_dash: str = "dot",
    # layout
    height: int = 150,
    margin: dict | None = None,
    use_tickvals: bool = True,
    ):
    """
    [ 입력 ]
    - depth : 10, 20, ... , 100
    - 직전구간이탈(%) : 각 depth에서의 이탈률(퍼센트)

    [ 출력 ]
    """

    # 시작 & 전처리
    if margin is None:
        margin = dict(l=10, r=10, t=10, b=10)

    if df is None or df.empty or col_depth not in df.columns or col_rate not in df.columns:
        fig = go.Figure()
        fig.update_layout(height=height, margin=margin)
        meta = {"b1": None, "b2": None, "first_depth": None, "second_depth": None,
                "early_th": None, "early_mu": None, "early_sd": None, "xmin": None, "xmax": None}
        return fig, meta

    dff = df.copy()
    dff[col_depth] = pd.to_numeric(dff[col_depth], errors="coerce")
    dff[col_rate] = pd.to_numeric(dff[col_rate], errors="coerce")
    dff = dff.dropna(subset=[col_depth, col_rate]).sort_values(col_depth).reset_index(drop=True)

    if dff.empty:
        fig = go.Figure()
        fig.update_layout(height=height, margin=margin)
        meta = {"b1": None, "b2": None, "first_depth": None, "second_depth": None,
                "early_th": None, "early_mu": None, "early_sd": None, "xmin": None, "xmax": None}
        return fig, meta
    
    # x/y 리스트 + 차분(dy)
    x = dff[col_depth].astype(int).tolist() # depth와 이탈률을 리스트로 변환(루프/인덱스 기반 처리용)
    y = dff[col_rate].astype(float).tolist()
    dy = [np.nan] + list(np.diff(y)) # 1차 차분 (dy[i] = y[i] - y[i-1])

    # [ 전반 탐지 ]
    # - 전반부 범위 (기본 70)에 해당하는 dy만 보아서 배열로 만들고,  
    # - 전반부 변화량의 평균/표준편차/임계값(z_k) 계산 
    # - 전반부에서 임계값 초과하는 dy 지점들을 찾고 그 중 가장 첫 번째 지점을 병목으로 채택
    # - 임계값 초과가 하나도 없으면 dy가 가장 큰 지점을 병목으로 강제 
    early_idx = [i for i, xi in enumerate(x) if (xi <= early_max) and (i >= 1)]
    first_i = None
    early_mu = None
    early_sd = None
    early_th = None

    if len(early_idx) >= 2:
        early_dy = np.array([dy[i] for i in early_idx], dtype=float)
        early_mu = float(np.nanmean(early_dy))
        early_sd = float(np.nanstd(early_dy))
        early_th = float(early_mu + (z_k * early_sd))

    # (수정 Gemini)  dy의 local peak(국소 최대) 중 최대를 병목으로 선택
    first_i = None

    if len(early_idx) >= 3:
        peaks = []
        for i in early_idx:
            # i는 dy 인덱스. local peak 판별은 i-1, i, i+1 비교가 필요
            if (i - 1) < 1 or (i + 1) >= len(dy):  # 범위 보호
                continue
            if np.isnan(dy[i - 1]) or np.isnan(dy[i]) or np.isnan(dy[i + 1]):
                continue

            # ✅ local peak: 직전보다 크고, 직후보다 크고, 상승(>0)인 지점
            if (dy[i] > 0) and (dy[i] > dy[i - 1]) and (dy[i] > dy[i + 1]):
                peaks.append(i)

        # local peak 중 dy가 가장 큰 지점
        if len(peaks) > 0:
            first_i = max(peaks, key=lambda j: dy[j])

    # 폴백: local peak 없으면(단조/평탄) dy 최대
    if first_i is None and len(early_idx) > 0:
        first_i = max(early_idx, key=lambda i: (-1e18 if np.isnan(dy[i]) else dy[i]))

    first_depth = x[first_i] if first_i is not None else x[0]


    # [ 후반 탐지 ]
    # - 후반부 범위 (기본 60)에 해당하는 후보 인덱스 추림
    # - 전환점 조건은 직전은 하락/정체(<=0), 지금은 상승 (>0)
    # - 전환점 이후 n_pos번 연속으로 dy가 양수인지 검사하고 하나라도 끊기면 ok = False (조건 만족)
    # - 전환점 + 연속상승 규칙으로 못 찾으면 후반부에서 이탈률(y)이 가장 낮은 지점을 시작점으로 폴백
    tail_candidates = [i for i, xi in enumerate(x) if (xi >= tail_min) and (i >= 2)]
    second_start_i = None

    for i in tail_candidates:
        if np.isnan(dy[i]) or np.isnan(dy[i - 1]):
            continue
        if (dy[i] > 0) and (dy[i - 1] <= 0):
            ok = True
            for k in range(n_pos):
                j = i + k
                if j >= len(dy) or np.isnan(dy[j]) or dy[j] <= 0:
                    ok = False
                    break
            if ok:
                second_start_i = i
                break

    if second_start_i is None:
        tail_y_idx = [i for i, xi in enumerate(x) if xi >= tail_min]
        if len(tail_y_idx) > 0:
            second_start_i = min(tail_y_idx, key=lambda i: y[i])

    second_depth = x[second_start_i] if second_start_i is not None else x[-1]

    # 경계 정리(b1/b2) + 충돌 방지
    xmin, xmax = x[0], x[-1]
    b1 = int(first_depth)
    b2 = int(second_depth)

    if b1 >= b2:
        b2 = b1 + int(min_gap)
        if b2 > xmax:
            b2 = xmax

    # Figure
    fig = go.Figure()

    # shading 3 zones
    c1, c2, c3 = shade_rgba
    fig.update_layout(
        shapes=[
            dict(
                type="rect", xref="x", yref="paper",
                x0=xmin, x1=b1, y0=shade_y0, y1=shade_y1,
                fillcolor=c1, line=dict(width=0),
                layer=shade_layer
            ),
            dict(
                type="rect", xref="x", yref="paper",
                x0=b1, x1=b2, y0=shade_y0, y1=shade_y1,
                fillcolor=c2, line=dict(width=0),
                layer=shade_layer
            ),
            dict(
                type="rect", xref="x", yref="paper",
                x0=b2, x1=xmax, y0=shade_y0, y1=shade_y1,
                fillcolor=c3, line=dict(width=0),
                layer=shade_layer
            ),
        ]
    )

    if show_annotations:
        fig.update_layout(
            annotations=[
                dict(
                    x=b1, y=ann_y, xref="x", yref="paper",
                    text=ann_text_1.format(b1=b1, b2=b2),
                    showarrow=False, font=dict(size=ann_font_size)
                ),
                dict(
                    x=b2, y=ann_y, xref="x", yref="paper",
                    text=ann_text_2.format(b1=b1, b2=b2),
                    showarrow=False, font=dict(size=ann_font_size)
                ),
            ]
        )

    # line/fill
    fig.add_trace(go.Scatter(
        x=dff[col_depth],
        y=dff[col_rate],
        mode=mode,
        fill=fill,
        opacity=line_opacity,
        hovertemplate=hovertemplate,
    ))

    # vlines
    if show_vlines:
        fig.add_vline(x=b1, line_width=vline_width, line_dash=vline_dash)
        fig.add_vline(x=b2, line_width=vline_width, line_dash=vline_dash)

    # layout
    fig.update_layout(height=150, margin=dict(l=10,r=10,t=10,b=10))
    
    meta = {
        "b1": b1,
        "b2": b2,
        "first_depth": int(first_depth) if first_depth is not None else None,
        "second_depth": int(second_depth) if second_depth is not None else None,
        "early_th": early_th,
        "early_mu": early_mu,
        "early_sd": early_sd,
        "xmin": int(xmin),
        "xmax": int(xmax),
    }
    return fig, meta


        