from __future__ import annotations
import pandas as pd
import numpy as np


def style_format(df: pd.DataFrame,
                decimals_map: dict,
                suffix_map: dict | None = None,
                thousands: str = ","):
    """
    - decimals_map: {컬럼명(or MultiIndex 튜플): 소수자릿수(int)}
    - suffix_map:   {컬럼명: ' %' 처럼 붙일 접미사} (옵션)
    - thousands:    천단위 구분자(기본 ',')
    반환: pandas Styler
    """
    suffix_map = suffix_map or {}
    formatter: dict = {}

    for col, dec in decimals_map.items():
        if col not in df.columns:
            continue
        d = int(dec)
        if col in suffix_map:  # 접미사 있는 컬럼: 함수 포맷터
            sfx = suffix_map[col]
            formatter[col] = (lambda d=d, sfx=sfx:
                            (lambda v: "" if pd.isna(v) else f"{float(v):,.{d}f}{sfx}"))()
        else:                  # 숫자 포맷만: 포맷 문자열
            formatter[col] = f"{{:,.{d}f}}"

    # 핵심: thousands="," 를 최상위에 지정해야 쉼표가 유지됩니다.
    return df.style.format(formatter=formatter, thousands=thousands)



# def style_cmap(df_or_styler,
#                gradient_rules: list[dict],
#                *,
#                default_cmap: str = "Greens",
#                na_color: str = "#ffffff"
#             ) -> pd.io.formats.style.Styler:
#     """
#     gradient_rules 예:
#       [
#         {"cols": [("COST","매출"), ("COST","주문수")], "cmap":"Blues", "low":0.2, "high":0},
#         {"col":  ("PERFORMANCE","ROAS(광고수익률)"),   "cmap":"Greens", "vmin":0, "vmax":200, "low":0.1},
#       ]
#     """
#     # 1) DataFrame or Styler 모두 허용
#     if isinstance(df_or_styler, pd.io.formats.style.Styler):
#         styler = df_or_styler
#         df = styler.data  # pandas>=1.4/2.x
#     else:
#         df = df_or_styler
#         styler = df.style

#     # 3) 규칙 적용
#     for sp in gradient_rules:
#         cols = sp.get("cols")
#         col  = sp.get("col")

#         # 대상 열 목록 만들기
#         if cols is not None:
#             targets = [c for c in cols if c in df.columns]
#         elif col is not None and col in df.columns:
#             targets = [col]
#         else:
#             continue  # 대상 없음

#         cmap  = sp.get("cmap", default_cmap)
#         low   = sp.get("low", 0.0)
#         high  = sp.get("high", 0.0)
#         vmin  = sp.get("vmin", None)
#         vmax  = sp.get("vmax", None)

#         for c in targets:
#             s = pd.to_numeric(df[c], errors="coerce")
#             vmin_c = s.min() if vmin is None else vmin
#             vmax_c = s.max() if vmax is None else vmax
#             styler = styler.background_gradient(
#                 subset=[c],
#                 cmap=cmap,
#                 vmin=vmin_c, vmax=vmax_c,
#                 low=low, high=high,
#             )

#     return styler



def style_cmap(df_or_styler,
               gradient_rules: list[dict],
               *,
               default_cmap: str = "Greens",
               na_color: str = "#ffffff"
            ) -> pd.io.formats.style.Styler:

    if isinstance(df_or_styler, pd.io.formats.style.Styler):
        styler = df_or_styler
        df = styler.data
    else:
        df = df_or_styler
        styler = df.style

    idx = pd.IndexSlice  # 행/열 동시 subset

    for sp in gradient_rules:
        cols = sp.get("cols")
        col  = sp.get("col")

        if cols is not None:
            targets = [c for c in cols if c in df.columns]
        elif col is not None and col in df.columns:
            targets = [col]
        else:
            continue

        cmap  = sp.get("cmap", default_cmap)
        low   = sp.get("low", 0.0)
        high  = sp.get("high", 0.0)
        vmin  = sp.get("vmin", None)
        vmax  = sp.get("vmax", None)

        # 합계/평균 행 제외 마스크 (하드코딩)
        if ("기본정보","날짜") in df.columns:
            mask = ~df[("기본정보","날짜")].isin(["합계","평균"])
        elif "event_date" in df.columns:
            mask = ~df["event_date"].isin(["합계","평균"])
        elif "날짜" in df.columns:
            mask = ~df["날짜"].isin(["합계","평균"])
        else:
            mask = pd.Series(True, index=df.index)

        rows = df.index[mask]
        if len(rows) == 0:
            # 색칠할 행 자체가 없으면 이 규칙 스킵
            continue

        for c in targets:
            
            # 대상 값 추출 + 유효값만
            s = pd.to_numeric(df.loc[rows, c], errors="coerce").replace([np.inf, -np.inf], np.nan)
            
            s_finite = s.dropna()
            if s_finite.empty:
                # 전부 NaN이면 스킵
                continue
            
            vmin_c = s.min() if vmin is None else vmin
            vmax_c = s.max() if vmax is None else vmax


            # NaN 방지 및 순서 보정
            if pd.isna(vmin_c) and not pd.isna(vmax_c):
                vmin_c = 0
            if pd.isna(vmax_c) and not pd.isna(vmin_c):
                vmax_c = vmin_c
            if pd.isna(vmin_c) and pd.isna(vmax_c):
                continue
            if vmin_c > vmax_c:
                vmin_c, vmax_c = vmax_c, vmin_c

            # 행/열 동시 subset
            styler = styler.background_gradient(
                subset=idx[rows, [c]],
                cmap=cmap,
                vmin=vmin_c, vmax=vmax_c,
                low=low, high=high,
            )

    # (추가) 합계/평균 행 row 단위 컬러링
    def highlight_summary(row):
        if ("기본정보","날짜") in row.index and row[("기본정보","날짜")] in ["합계","평균"]:
            return [f"background-color: #F8F9FB; color: #8E9097;"] * len(row)
        elif "event_date" in row.index and row["event_date"] in ["합계","평균"]:
            return [f"background-color: #F8F9FB; color: #8E9097;"] * len(row)
        elif "날짜" in row.index and row["날짜"] in ["합계","평균"]:
            return [f"background-color: #F8F9FB; color: #8E9097;"] * len(row)
        return [""] * len(row)

    styler = styler.apply(highlight_summary, axis=1)


    return styler
