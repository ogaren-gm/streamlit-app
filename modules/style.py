from __future__ import annotations
import pandas as pd


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



def style_cmap(df_or_styler,
               gradient_rules: list[dict],
               *,
               default_cmap: str = "Greens",
               na_color: str = "#ffffff"
            ) -> pd.io.formats.style.Styler:
    """
    gradient_rules 예:
      [
        {"cols": [("COST","매출"), ("COST","주문수")], "cmap":"Blues", "low":0.2, "high":0},
        {"col":  ("PERFORMANCE","ROAS(광고수익률)"),   "cmap":"Greens", "vmin":0, "vmax":200, "low":0.1},
      ]
    """
    # 1) DataFrame or Styler 모두 허용
    if isinstance(df_or_styler, pd.io.formats.style.Styler):
        styler = df_or_styler
        df = styler.data  # pandas>=1.4/2.x
    else:
        df = df_or_styler
        styler = df.style

    # 3) 규칙 적용
    for sp in gradient_rules:
        cols = sp.get("cols")
        col  = sp.get("col")

        # 대상 열 목록 만들기
        if cols is not None:
            targets = [c for c in cols if c in df.columns]
        elif col is not None and col in df.columns:
            targets = [col]
        else:
            continue  # 대상 없음

        cmap  = sp.get("cmap", default_cmap)
        low   = sp.get("low", 0.0)
        high  = sp.get("high", 0.0)
        vmin  = sp.get("vmin", None)
        vmax  = sp.get("vmax", None)

        for c in targets:
            s = pd.to_numeric(df[c], errors="coerce")
            vmin_c = s.min() if vmin is None else vmin
            vmax_c = s.max() if vmax is None else vmax
            styler = styler.background_gradient(
                subset=[c],
                cmap=cmap,
                vmin=vmin_c, vmax=vmax_c,
                low=low, high=high,
            )

    return styler
