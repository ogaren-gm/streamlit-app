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