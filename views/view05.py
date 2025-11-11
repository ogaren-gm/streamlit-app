# 서희_최신수정일_25-09-09

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import importlib, io, re, math, json
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd
import gspread
from google.oauth2.service_account import Credentials

import sys
import modules.style
importlib.reload(sys.modules['modules.style'])
from modules.style import style_format, style_cmap

from zoneinfo import ZoneInfo


def main():
    # ──────────────────────────────────
    # 스트림릿 페이지 설정
    # ──────────────────────────────────
    st.markdown(
        """
        <style>
            /* 전체 컨테이너의 패딩 조정 */
            .block-container {
                max-width: 100% !important;
                padding-top: 1rem;   /* 위쪽 여백 */
                padding-bottom: 8rem;
                padding-left: 5rem; 
                padding-right: 4rem; 
            }
        </style>
        """,
        unsafe_allow_html=True
    )    
    # 탭 간격 CSS
    st.markdown("""
        <style>
            [role="tablist"] [role="tab"] { margin-right: 1rem; }
        </style>
    """, unsafe_allow_html=True)

    
    @st.cache_data(ttl=3600)
    def load_data():
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        try:
            creds = Credentials.from_service_account_file(
                "C:/_code/auth/sleeper-461005-c74c5cd91818.json", scopes=scope
            )
        except:
            sa_info = st.secrets["sleeper-462701-admin"]
            if isinstance(sa_info, str):
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scope)

        gc = gspread.authorize(creds)
        sh = gc.open_by_url(
            "https://docs.google.com/spreadsheets/d/1HFPuxQSJqIY7VY_3YcAwEPfw_SjnApH_txRPS69s4xk/edit?gid=1274042914#gid=1274042914"
        )
        wsa = sh.worksheet("query_demographic")
        data = wsa.get("A1:E")
        df = pd.DataFrame(data[1:], columns=data[0])
        
        last_updated_time = df['날짜'].max()
        
        return df, last_updated_time

    with st.spinner("데이터를 불러오는 중입니다. 잠시만 기다려 주세요."):
        df, last_updated_time = load_data()

    # 공통 전처리
    df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
    df['abs_age'] = pd.to_numeric(df['abs_age'], errors='coerce')


    # ─────────────────────────────
    # 카드보드 
    # ─────────────────────────────
    # 스타일 (테두리, 그리드, 증감색)
    st.markdown("""
    <style>
    .kpi-wrap { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 12px; }
    .kpi-card { border: 1px solid #e9ecef; border-radius: 12px; padding: 14px; background:#fff; }
    .kpi-head { display:flex; align-items:center; justify-content:space-between; margin-bottom: 10px; }
    .kpi-title { font-weight: 700; font-size: 14px; color:#212529; }
    .kpi-delta { font-weight: 600; font-size: 16px; }
    .kpi-delta.up { color:#2e7d32; }        /* 상승: 녹색 */
    .kpi-delta.down { color:#c92a2a; }      /* 하락: 붉은색 */
    .kpi-delta.flat { color:#6c757d; }      /* 변화 없음: 회색 */
    .kpi-body { display:grid; grid-template-columns: 1fr 1px 1fr; gap: 12px; align-items: stretch; }
    .kpi-divider { background:#e9ecef; width:1px; }
    .kpi-block .label { font-size:12px; color:#6c757d; margin-bottom:4px; }
    .kpi-block .value { font-size:20px; font-weight:800; color:#212529; line-height:1.1; }
    .kpi-block .range { font-size:12px; color:#6c757d; margin-top:4px; }
    </style>
    """, unsafe_allow_html=True)

    # 카드보드 전용 기준일 필터
    _valid_dates = pd.to_datetime(df['날짜'], errors='coerce').dropna()
    _min_d = _valid_dates.min().date() if not _valid_dates.empty else datetime.today().date()
    _max_d = _valid_dates.max().date() if not _valid_dates.empty else datetime.today().date()

    # 유틸
    def _fmt_int(n): 
        return f"{int(n):,}" if pd.notna(n) else "-"

    def _fmt_range(s: pd.Timestamp, e: pd.Timestamp) -> str:
        if pd.isna(s) or pd.isna(e): return "-"
        return f"{s.strftime('%Y-%m-%d')} ~ {e.strftime('%Y-%m-%d')}"

    def _delta_parts(cur: int, prev: int):
        diff = int(cur - prev)
        if prev and prev != 0:
            pct = (diff / prev) * 100
            cls = "up" if diff > 0 else ("down" if diff < 0 else "flat")
            text = f"{diff:+,} ({pct:+.1f}%)"
        else:
            cls = "up" if diff > 0 else ("down" if diff < 0 else "flat")
            text = f"{diff:+,}"
        return cls, text

    def _period_sums(df_src: pd.DataFrame, days: int, end_date):
        if df_src.empty or df_src['날짜'].dropna().empty:
            return 0, 0, (pd.NaT, pd.NaT), (pd.NaT, pd.NaT)
        end = pd.to_datetime(end_date).normalize()
        cur_start  = end - pd.Timedelta(days=days-1)
        prev_end   = cur_start - pd.Timedelta(days=1)
        prev_start = prev_end - pd.Timedelta(days=days-1)
        cur_sum  = df_src.loc[(df_src['날짜'] >= cur_start) & (df_src['날짜'] <= end), 'abs_age'].sum()
        prev_sum = df_src.loc[(df_src['날짜'] >= prev_start) & (df_src['날짜'] <= prev_end), 'abs_age'].sum()
        return int(cur_sum), int(prev_sum), (cur_start, end), (prev_start, prev_end)



    # (25.11.10) 제목 + 설명 + 업데이트 시각 + 캐시초기화 
    # last_updated_time
    # 제목
    st.subheader("키워드 대시보드")

    if "refresh" in st.query_params:
        st.cache_data.clear()
        st.query_params.clear()   # 파라미터 제거
        st.rerun()
        
    # 설명
    col1, col2 = st.columns([0.65, 0.35], vertical_alignment="center")
    with col1:
        st.markdown(
            """
            <div style="  
                font-size:14px;       
                line-height:1.5;      
            ">
            설명
            </div>
            <div style="
                color:#6c757d;        
                font-size:14px;       
                line-height:2.0;      
            ">
            ※ 키워드·쿼리 D-1 데이터는 매일 10시 ~ 11시 사이에 업데이트됩니다.
            </div>
            """,
            unsafe_allow_html=True
        )
        
    with col2:
        # last_updated_time
        if isinstance(last_updated_time, str):
            lut = datetime.strptime(last_updated_time, "%Y-%m-%d")
        else:
            lut = last_updated_time
        lut_date = lut.date()
        
        now_kst   = datetime.now(ZoneInfo("Asia/Seoul"))
        today_kst = now_kst.date()
        delta_days = (today_kst - lut_date).days
        
        # 기본값
        # msg    = f"{lut_date.strftime('%m월 %d일')} (D-{delta_days})"
        msg    = f"D-{delta_days} 업데이트 완료"
        sub_bg = "#E6F4EC"
        sub_bd = "#91C7A5"
        sub_fg = "#237A57"
        
        # 렌더링
        st.markdown(
            f"""
            <div style="display:flex;justify-content:flex-end;align-items:center;gap:8px;">
            <span style="
                display:inline-flex;align-items:center;justify-content:center;
                height:26px;padding:0 10px;
                font-size:13px;line-height:1.1;
                color:{sub_fg};background:{sub_bg};border:1px solid {sub_bd};
                border-radius:10px;white-space:nowrap;">
                📊 {msg}
            </span>
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
            unsafe_allow_html=True
        )
    
    st.divider()





    # ──────────────────────────────────
    # 1) 추이 요약
    # ──────────────────────────────────
    st.markdown(" ")
    q1, q2 = st.columns([6,2])
    with q1:
        st.markdown("<h5 style='margin:0'>추이 요약</h5>", unsafe_allow_html=True)      
        st.markdown(":gray-badge[:material/Info: Info]ㅤ기준일을 포함한 **최근 7일 또는 30일** 합계를 직전 동기간과 비교합니다.", unsafe_allow_html=True)
    with q2:
        # 날짜 입력을 먼저!
        card_ref_end = st.date_input(
            "기준일 (카드보드 전용)",
            value=_max_d, min_value=_min_d, max_value=_max_d,
            key="card_ref_end"
        )

        # 이제 카드 계산
        cur7,  prev7,  (c7_s,  c7_e),  (p7_s,  p7_e)  = _period_sums(df, days=7,  end_date=card_ref_end)
        cur30, prev30, (c30_s, c30_e), (p30_s, p30_e) = _period_sums(df, days=30, end_date=card_ref_end)

        cls7,  txt7  = _delta_parts(cur7,  prev7)
        cls30, txt30 = _delta_parts(cur30, prev30)


    # 탭 추가 (전체 · 일반 · 경쟁사 · 소비자 · 자사)
    kpi_tabs = st.tabs(["전체", "일반", "경쟁사", "소비자", "자사"])
    _patterns = {"일반": "일반", "경쟁사": "경쟁사", "소비자": "소비", "자사": "자사"}

    for t, label in zip(kpi_tabs, ["전체", "일반", "경쟁사", "소비자", "자사"]):
        with t:
            # 탭별 데이터 서브셋 (전체는 필터 없음)
            df_tab = df
            if label != "전체":
                df_tab = df[df["키워드유형"].astype(str).str.contains(_patterns[label], na=False)]

            if df_tab.empty:
                st.info(f"{label} 탭에 표시할 데이터가 없습니다.")
                continue

            # 탭별 집계 (기준일은 상단 date_input(card_ref_end) 공통 사용)
            cur7,  prev7,  (c7_s,  c7_e),  (p7_s,  p7_e)  = _period_sums(df_tab, days=7,  end_date=card_ref_end)
            cur30, prev30, (c30_s, c30_e), (p30_s, p30_e) = _period_sums(df_tab, days=30, end_date=card_ref_end)
            cls7,  txt7  = _delta_parts(cur7,  prev7)
            cls30, txt30 = _delta_parts(cur30, prev30)

            # 카드 2개 (좌: 7일, 우: 30일) — 기존 UI 그대로 사용
            colA, colB = st.columns(2)

            with colA:
                st.markdown(f"""
                <div class="kpi-card">
                <div class="kpi-head">
                    <div class="kpi-title">최근 7일 vs 이전 7일</div>
                    <div class="kpi-delta {cls7}">{txt7}</div>
                </div>
                <div class="kpi-body">
                    <div class="kpi-block">
                    <div class="label">최근 7일 합계</div>
                    <div class="value">{_fmt_int(cur7)}</div>
                    <div class="range">{_fmt_range(c7_s, c7_e)}</div>
                    </div>
                    <div class="kpi-divider"></div>
                    <div class="kpi-block">
                    <div class="label">이전 7일 합계</div>
                    <div class="value">{_fmt_int(prev7)}</div>
                    <div class="range">{_fmt_range(p7_s, p7_e)}</div>
                    </div>
                </div>
                </div>
                """, unsafe_allow_html=True)

            with colB:
                st.markdown(f"""
                <div class="kpi-card">
                <div class="kpi-head">
                    <div class="kpi-title">최근 30일 vs 이전 30일</div>
                    <div class="kpi-delta {cls30}">{txt30}</div>
                </div>
                <div class="kpi-body">
                    <div class="kpi-block">
                    <div class="label">최근 30일 합계</div>
                    <div class="value">{_fmt_int(cur30)}</div>
                    <div class="range">{_fmt_range(c30_s, c30_e)}</div>
                    </div>
                    <div class="kpi-divider"></div>
                    <div class="kpi-block">
                    <div class="label">이전 30일 합계</div>
                    <div class="value">{_fmt_int(prev30)}</div>
                    <div class="range">{_fmt_range(p30_s, p30_e)}</div>
                    </div>
                </div>
                </div>
                """, unsafe_allow_html=True)




    # ─────────────────────────────
    # 공통 함수
    # ─────────────────────────────
    def _log_safe(arr):
        a = np.asarray(arr, dtype=float)
        return np.where(a > 0, a, np.nan)

    def _get_date_col_key(df: pd.DataFrame):
        if '날짜' in df.columns:
            return '날짜'
        if isinstance(df.columns, pd.MultiIndex):
            for c in df.columns:
                if isinstance(c, tuple) and c[0] == '날짜':
                    return c
        return df.columns[0]    

    def render_chart_and_table(
        df: pd.DataFrame,
        granularity: str,         # "일" | "월"
        view_mode: str,           # "전체합" | "연령대 산개" | "연령대별(스택)" | "키워드별(스택)"
        chart_kind: str,          # "누적 막대" | "막대" | "꺾은선"
        scale_mode: str,          # "절댓값" | "백분율" | "로그"
        sel_types: list[str],
        sel_ages: list[str],
        title_note: str = ""
    ):
        # 정제
        df['날짜']    = pd.to_datetime(df['날짜'], errors='coerce')
        df['abs_age'] = pd.to_numeric(df['abs_age'], errors='coerce')
        age_order = ['19-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59']
        df['age_info'] = pd.Categorical(df['age_info'], categories=age_order, ordered=True)

        # 필터
        df = df[df['키워드유형'].isin(sel_types) & df['age_info'].isin(sel_ages)].copy()
        if df.empty:
            st.warning("선택 조건에 해당하는 데이터가 없습니다.")
            return

        # 유형 정렬
        type_order = (
            df.groupby('키워드유형')['abs_age']
              .sum().sort_values(ascending=False).index.tolist()
        )
        order_full = " > ".join(type_order)

        # ───── (A) 키워드별(스택) 브랜치 ─────
        if view_mode == "키워드별(스택)":
            df['날짜_dt'] = df['날짜']

            # X축
            if granularity == "일":
                x_index = pd.date_range(df['날짜_dt'].min(), df['날짜_dt'].max(), freq='D')
                x_col = '날짜_dt'
                base = df.groupby(['날짜_dt','키워드유형','키워드'], as_index=False)['abs_age'].sum()
                kw_list = sorted(base['키워드'].dropna().unique().tolist())
                work = (
                    base.set_index(['날짜_dt','키워드유형','키워드'])
                        .reindex(pd.MultiIndex.from_product(
                            [x_index, type_order, kw_list],
                            names=['날짜_dt','키워드유형','키워드']
                        ), fill_value=0)
                        .reset_index()
                )

            elif granularity == "주":
                df['주'] = df['날짜'].dt.to_period('W').dt.to_timestamp()
                min_w, max_w = df['주'].min().to_period('W'), df['주'].max().to_period('W')
                x_index = pd.period_range(min_w, max_w, freq='W').to_timestamp()
                x_col = '주'
                base = df.groupby(['주','키워드유형','키워드'], as_index=False)['abs_age'].sum()
                kw_list = sorted(base['키워드'].dropna().unique().tolist())
                work = (
                    base.set_index(['주','키워드유형','키워드'])
                        .reindex(pd.MultiIndex.from_product(
                            [x_index, type_order, kw_list],
                            names=['주','키워드유형','키워드']
                        ), fill_value=0)
                        .reset_index()
                )

            elif granularity == "월":
                df['월'] = df['날짜'].dt.to_period('M').dt.to_timestamp()
                min_m, max_m = df['월'].min().to_period('M'), df['월'].max().to_period('M')
                x_index = pd.period_range(min_m, max_m, freq='M').to_timestamp()
                x_col = '월'
                base = df.groupby(['월','키워드유형','키워드'], as_index=False)['abs_age'].sum()
                kw_list = sorted(base['키워드'].dropna().unique().tolist())
                work = (
                    base.set_index(['월','키워드유형','키워드'])
                        .reindex(pd.MultiIndex.from_product(
                            [x_index, type_order, kw_list],
                            names=['월','키워드유형','키워드']
                        ), fill_value=0)
                        .reset_index()
                )


            # 값 계산
            y_is_pct = (scale_mode == "백분율")
            y_is_log = (scale_mode == "로그")
            plot_kw  = work.copy()
            if y_is_pct:
                denom = plot_kw.groupby([x_col,'키워드유형'])['abs_age'].transform('sum')
                plot_kw['val'] = np.where(denom > 0, plot_kw['abs_age'] / denom, 0.0)
            else:
                plot_kw['val'] = plot_kw['abs_age']

            # 색상/포맷
            palette = px.colors.qualitative.Pastel
            kw_color = {k: palette[i % len(palette)] for i, k in enumerate(kw_list)}
            date_fmt = "%Y-%m" if granularity == "월" else "%Y-%m-%d"

            is_bar = (chart_kind in ["누적 막대", "막대"])
            fig = go.Figure()

            if is_bar:
                # 유형 병렬, 내부는 키워드 스택
                for t_idx, t in enumerate(type_order):
                    d_t = plot_kw[plot_kw['키워드유형'] == t]
                    for k in kw_list:
                        s = d_t[d_t['키워드'] == k].set_index(x_col)['val'].reindex(x_index, fill_value=0)
                        y_vals = _log_safe(s.values) if y_is_log else s.values
                        fig.add_bar(
                            x=x_index, y=y_vals,
                            name=str(k),
                            legendgroup=f"KW:{k}",
                            showlegend=(t_idx == 0),
                            marker_color=kw_color[k],
                            offsetgroup=str(t),
                            opacity=0.8,
                            hovertemplate=f"{t} • {k}"
                                          + "<br>%{x|"+date_fmt+"}"
                                          + "<br>" + ("%{y:.1%}" if y_is_pct else "%{y:,.0f}") + "<extra></extra>",
                        )
                fig.update_layout(barmode="relative")
            else:
                for t in type_order:
                    d_t = plot_kw[plot_kw['키워드유형'] == t]
                    for k in kw_list:
                        s = d_t[d_t['키워드'] == k].set_index(x_col)['val'].reindex(x_index, fill_value=0)
                        y_vals = _log_safe(s.values) if y_is_log else s.values
                        fig.add_trace(go.Scatter(
                            x=x_index, y=y_vals,
                            mode="lines+markers",
                            name=str(k),
                            legendgroup=f"KW:{k}",
                            marker=dict(size=4),
                            marker_color=kw_color[k],
                            showlegend=(t == type_order[0]),
                            hovertemplate=f"{t} • {k}"
                                          + "<br>%{x|"+date_fmt+"}"
                                          + "<br>" + ("%{y:.1%}" if y_is_pct else "%{y:,.0f}") + "<extra></extra>",
                        ))

            # 주석/레이아웃
            # memo = f"정렬: <b>{order_full}</b>"
            # if title_note:
            #     memo += f" ｜ {title_note}"
            # fig.add_annotation(xref="paper", yref="paper", x=0.0, y=-0.20,
            #                    text=memo, showarrow=False, align="left",
            #                    font=dict(size=11, color="#6c757d"))

            fig.update_layout(
                height=460,
                legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom", title=None),
                xaxis_title=None, yaxis_title=None,
                bargap=0.1, bargroupgap=0.2,
                margin=dict(l=10, r=10, t=20, b=60)
            )
            fig.update_xaxes(tickformat="%Y-%m" if granularity == "월" else "%m월 %d일")
            if y_is_pct:
                fig.update_yaxes(tickformat=".0%")
            elif y_is_log:
                fig.update_yaxes(type="log", tickformat="~s")

            st.plotly_chart(fig, use_container_width=True)

            # 표: 상위=유형, 하위=키워드
            pt = (
                plot_kw.pivot_table(
                    index=x_col,
                    columns=['키워드유형','키워드'],
                    values='val',
                    aggfunc='sum',
                    fill_value=0
                )
                .reindex(columns=pd.MultiIndex.from_product([type_order, kw_list]), fill_value=0)
                .reindex(x_index, fill_value=0)
            )
            pt.columns = pd.MultiIndex.from_tuples(pt.columns, names=['키워드유형','키워드'])
            tbl = pt.reset_index().rename(columns={x_col: '날짜'})

            if y_is_pct:
                scaled_cols = []
                for col in tbl.columns:
                    if isinstance(col, tuple) and pd.api.types.is_numeric_dtype(tbl[col]):
                        tbl[col] = (tbl[col].astype(float) * 100).round(1)
                        scaled_cols.append(col)

                # (%)는 실제로 스케일링한 튜플 컬럼에만 부착
                new_cols = []
                for c in tbl.columns:
                    if isinstance(c, tuple) and c in scaled_cols:
                        new_cols.append((c[0], f"{c[1]} (%)"))
                    else:
                        new_cols.append(c)
                tbl.columns = pd.Index(new_cols)

            date_key = _get_date_col_key(tbl)
            tbl[date_key] = pd.to_datetime(tbl[date_key], errors='coerce')
            tbl[date_key] = tbl[date_key].dt.strftime("%Y-%m" if granularity == "월" else "%Y-%m-%d")

            st.dataframe(tbl, use_container_width=True, hide_index=True, row_height=30)
            return  # 전용 브랜치 종료

        # ───── (B) 기존(연령대/전체합) 브랜치 ─────
        df['날짜_dt'] = df['날짜']
        daily = df.groupby(['날짜_dt','키워드유형','age_info'], as_index=False)['abs_age'].sum()
        daily['키워드유형'] = pd.Categorical(daily['키워드유형'], categories=type_order, ordered=True)

        # X축
        if granularity == "일":
            x_index = pd.date_range(daily['날짜_dt'].min(), daily['날짜_dt'].max(), freq='D')
            x_col = '날짜_dt'
            work = (
                daily.set_index(['날짜_dt','키워드유형','age_info'])
                     .reindex(pd.MultiIndex.from_product([x_index, type_order, sel_ages],
                                                         names=['날짜_dt','키워드유형','age_info']),
                              fill_value=0)
                     .reset_index()
            )

        elif granularity == "주":
            daily['주'] = daily['날짜_dt'].dt.to_period('W').dt.to_timestamp()
            min_w, max_w = daily['주'].min().to_period('W'), daily['주'].max().to_period('W')
            x_index = pd.period_range(min_w, max_w, freq='W').to_timestamp()
            x_col = '주'
            week_base = daily.groupby(['주','키워드유형','age_info'], as_index=False)['abs_age'].sum()
            work = (
                week_base.set_index(['주','키워드유형','age_info'])
                        .reindex(pd.MultiIndex.from_product([x_index, type_order, sel_ages],
                                                            names=['주','키워드유형','age_info']),
                                fill_value=0)
                        .reset_index()
            )
        
        elif granularity == "월":
            daily['월'] = daily['날짜_dt'].dt.to_period('M').dt.to_timestamp()
            min_m, max_m = daily['월'].min().to_period('M'), daily['월'].max().to_period('M')
            x_index = pd.period_range(min_m, max_m, freq='M').to_timestamp()
            x_col = '월'
            month_base = daily.groupby(['월','키워드유형','age_info'], as_index=False)['abs_age'].sum()
            work = (
                month_base.set_index(['월','키워드유형','age_info'])
                          .reindex(pd.MultiIndex.from_product([x_index, type_order, sel_ages],
                                                              names=['월','키워드유형','age_info']),
                                   fill_value=0)
                          .reset_index()
            )

        # 값
        y_is_pct = (scale_mode == "백분율")
        y_is_log = (scale_mode == "로그")
        plot_df  = work.copy()
        if y_is_pct:
            if view_mode in ["연령대별(스택)", "연령대 산개"]:
                denom = plot_df.groupby([x_col,'키워드유형'])['abs_age'].transform('sum')
                plot_df['val'] = np.where(denom > 0, plot_df['abs_age'] / denom, 0.0)
            elif view_mode == "전체합":
                total = plot_df.groupby([x_col,'키워드유형'], as_index=False)['abs_age'].sum()
                denom = total.groupby(x_col)['abs_age'].transform('sum')
                total['val'] = np.where(denom > 0, total['abs_age'] / denom, 0.0)
                plot_df = total
        else:
            if view_mode in ["연령대별(스택)", "연령대 산개"]:
                plot_df['val'] = plot_df['abs_age']
            elif view_mode == "전체합":
                plot_df = (
                    plot_df.groupby([x_col,'키워드유형'], as_index=False)['abs_age']
                           .sum().rename(columns={'abs_age':'val'})
                )

        # 차트
        fig = go.Figure()
        palette = px.colors.qualitative.Pastel
        date_fmt = "%Y-%m" if granularity == "월" else "%Y-%m-%d"

        age_color  = {a: palette[i % len(palette)] for i, a in enumerate(sel_ages)}
        type_color = {t: palette[i % len(palette)] for i, t in enumerate(type_order)}

        bar_mode = "relative" if (chart_kind == "누적 막대" or view_mode == "연령대별(스택)") else "group"
        is_bar   = chart_kind in ["누적 막대", "막대"]

        if is_bar:
            if view_mode in ["연령대별(스택)", "연령대 산개"]:
                for t_idx, t in enumerate(type_order):
                    d_t = plot_df[plot_df['키워드유형'] == t]
                    for a in sel_ages:
                        s = d_t[d_t['age_info'] == a].set_index(x_col)['val'].reindex(x_index, fill_value=0)
                        y_vals = _log_safe(s.values) if y_is_log else s.values
                        fig.add_bar(
                            x=x_index, y=y_vals,
                            name=str(a), legendgroup=f"AGE:{a}", showlegend=(t_idx == 0),
                            marker_color=age_color[a],
                            offsetgroup=str(t),  # 유형별 병렬
                            opacity=0.8,
                            hovertemplate=f"{t} • {a}"
                                          + "<br>%{x|"+date_fmt+"}"
                                          + "<br>" + ("%{y:.1%}" if y_is_pct else "%{y:,.0f}") + "<extra></extra>",
                        )
                fig.update_layout(barmode=bar_mode)
            elif view_mode == "전체합":
                for t in type_order:
                    s = plot_df[plot_df['키워드유형'] == t].set_index(x_col)['val'].reindex(x_index, fill_value=0)
                    y_vals = _log_safe(s.values) if y_is_log else s.values
                    # 누적이면 offsetgroup 제거(진짜 누적), 그룹이면 유지
                    bar_kwargs = {} if bar_mode == "relative" else {"offsetgroup": str(t)}
                    fig.add_bar(
                        x=x_index, y=y_vals,
                        name=str(t),
                        marker_color=type_color[t],
                        opacity=0.8,
                        hovertemplate=f"{t}"
                                      + "<br>%{x|"+date_fmt+"}"
                                      + "<br>" + ("%{y:.1%}" if y_is_pct else "%{y:,.0f}") + "<extra></extra>",
                        **bar_kwargs,
                    )
                fig.update_layout(barmode=bar_mode)
        else:
            # 꺾은선
            if view_mode in ["연령대별(스택)", "연령대 산개"]:
                dash_seq = ["solid","dash","dot","dashdot","longdash","longdashdot"]
                dash_map = {a: dash_seq[i % len(dash_seq)] for i, a in enumerate(sel_ages)}
                for t in type_order:
                    d_t = plot_df[plot_df['키워드유형'] == t]
                    for a in sel_ages:
                        s = d_t[d_t['age_info'] == a].set_index(x_col)['val'].reindex(x_index, fill_value=0)
                        y_vals = _log_safe(s.values) if y_is_log else s.values
                        fig.add_trace(go.Scatter(
                            x=x_index, y=y_vals, mode="lines+markers",
                            name=f"{t} • {a}", legendgroup=f"{t}",
                            marker=dict(size=4), line=dict(dash=dash_map[a]),
                            marker_color=type_color[t],
                            hovertemplate=f"{t} • {a}"
                                          + "<br>%{x|"+date_fmt+"}"
                                          + "<br>" + ("%{y:.1%}" if y_is_pct else "%{y:,.0f}") + "<extra></extra>",
                        ))
            elif view_mode == "전체합":
                for t in type_order:
                    s = plot_df[plot_df['키워드유형'] == t].set_index(x_col)['val'].reindex(x_index, fill_value=0)
                    y_vals = _log_safe(s.values) if y_is_log else s.values
                    fig.add_trace(go.Scatter(
                        x=x_index, y=y_vals, mode="lines+markers",
                        name=str(t),
                        marker=dict(size=4), marker_color=type_color[t],
                        hovertemplate=f"{t}"
                                      + "<br>%{x|"+date_fmt+"}"
                                      + "<br>" + ("%{y:.1%}" if y_is_pct else "%{y:,.0f}") + "<extra></extra>",
                    ))

        # 주석
        # memo = f"정렬: <b>{order_full}</b>"
        # if title_note:
        #     memo += f" ｜ {title_note}"
        # fig.add_annotation(
        #     xref="paper", yref="paper", x=0.0, y=-0.20,
        #     text=memo, showarrow=False, align="left",
        #     font=dict(size=11, color="#6c757d"),
        # )

        # 레이아웃
        fig.update_layout(
            height=460,
            legend=dict(orientation="h", y=1.02, x=1, xanchor="right", yanchor="bottom", title=None),
            xaxis_title=None, yaxis_title=None,
            bargap=0.1, bargroupgap=0.2,
            margin=dict(l=10, r=10, t=20, b=60)
        )
        fig.update_xaxes(tickformat="%Y-%m" if granularity == "월" else "%m월 %d일")
        if y_is_pct:
            fig.update_yaxes(tickformat=".0%")
        elif (scale_mode == "로그"):
            fig.update_yaxes(type="log", tickformat="~s")

        st.plotly_chart(fig, use_container_width=True)

        # 표
        if view_mode in ["연령대별(스택)", "연령대 산개"]:
            pt = (
                plot_df.pivot_table(
                    index=x_col,
                    columns=['키워드유형','age_info'],
                    values='val',
                    aggfunc='sum',
                    fill_value=0
                )
                .reindex(columns=pd.MultiIndex.from_product([type_order, sel_ages]), fill_value=0)
                .reindex(x_index, fill_value=0)
            )
            pt.columns = pd.MultiIndex.from_tuples(pt.columns, names=['키워드유형', '연령대'])
            tbl = pt.reset_index().rename(columns={x_col: '날짜'})
        else:
            pt = (
                plot_df.pivot(index=x_col, columns='키워드유형', values='val')
                      .reindex(columns=type_order, fill_value=0)
                      .reindex(x_index, fill_value=0)
            )
            tbl = pt.reset_index().rename(columns={x_col: '날짜'})

        if y_is_pct:
            # (i) 열 라벨에 튜플(계층 컬럼)이 섞여 있는 경우: 숫자형 튜플 컬럼만 변환
            if any(isinstance(c, tuple) for c in tbl.columns):
                scaled_cols = [c for c in tbl.columns
                            if isinstance(c, tuple) and pd.api.types.is_numeric_dtype(tbl[c])]
                if scaled_cols:
                    tbl[scaled_cols] = (tbl[scaled_cols].astype(float) * 100).round(1)
                new_cols = []
                for c in tbl.columns:
                    if c in scaled_cols:
                        new_cols.append((c[0], f"{c[1]} (%)"))
                    else:
                        new_cols.append(c)
                tbl.columns = pd.Index(new_cols)

            # (ii) 일반 단일 컬럼인 경우: 날짜/비수치 제외하고 변환
            else:
                date_key = _get_date_col_key(tbl)
                num_cols = [c for c in tbl.columns
                            if c != date_key and pd.api.types.is_numeric_dtype(tbl[c])]
                if num_cols:
                    tbl[num_cols] = (tbl[num_cols].astype(float) * 100).round(1)
                    tbl.rename(columns={c: f"{c} (%)" for c in num_cols}, inplace=True)


        date_key = _get_date_col_key(tbl)
        tbl[date_key] = pd.to_datetime(tbl[date_key], errors='coerce')
        tbl[date_key] = tbl[date_key].dt.strftime("%Y-%m" if granularity == "월" else "%Y-%m-%d")

        st.dataframe(tbl, use_container_width=True, hide_index=True, row_height=30)


    # ─────────────────────────────
    # 1. 영역
    # ─────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>키워드 <span style='color:#FF4B4B;'>기본 추이</span></h5>", unsafe_allow_html=True)      
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
    df['날짜_dt'] = df['날짜']

    start_period = df['날짜_dt'].min().to_period("M")
    end_period   = df['날짜_dt'].max().to_period("M")
    month_options = [p.to_timestamp() for p in pd.period_range(start=start_period, end=end_period, freq="M")]
    default_start, default_end = (month_options[-2], month_options[-1]) if len(month_options)>=2 else (month_options[0], month_options[0])

    start_sel, end_sel = st.select_slider(
        "기간(월)", options=month_options, value=(default_start, default_end),
        format_func=lambda x: x.strftime("%Y-%m"), key="v1_period"
    )
    with st.expander("기본 필터", expanded=True):
        c1, c2, c3, c4 = st.columns([2,3,2,3])
        with c1:
            granularity = st.radio("집계 단위", ["일","주","월"], horizontal=True, index=0, key="v1_gran")
        with c2:
            # '연령대 산개' 제거 → 단일 옵션 '전체합'
            view_mode   = st.radio("표시 방식", ["전체합"], horizontal=True, index=0, key="v1_view")
        with c3:
            # 기본 꺾은선 유지(원래 index=1이 '꺾은선')
            chart_kind  = st.radio("그래프", ["누적 막대", "꺾은선"], horizontal=True, index=1, key="v1_chart")
        with c4:
            scale_mode  = st.radio("스케일", ["절댓값", "백분율", "로그"], horizontal=True, index=0, key="v1_scale")

    with st.expander("고급 필터", expanded=False):
        type_all = sorted(df['키워드유형'].dropna().unique().tolist())
        age_order = ['19-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59']

        sel_types = st.multiselect("키워드유형 선택", type_all, default=type_all, key="v1_types")
        sel_ages  = st.multiselect("연령대 선택", age_order, default=age_order, key="v1_ages")

        period_start, period_end = start_sel, end_sel + MonthEnd(0)
        dfp = df[(df['날짜_dt'] >= period_start) & (df['날짜_dt'] <= period_end)].copy()

    render_chart_and_table(
        dfp, granularity, view_mode, chart_kind, scale_mode,
        sel_types, sel_ages,
    )


    # ─────────────────────────────
    # 2. 영역
    # ─────────────────────────────
    st.header(" ")
    st.markdown("<h5 style='margin:0'>키워드 <span style='color:#FF4B4B;'>심화 분석</span></h5>", unsafe_allow_html=True)      
    st.markdown(":gray-badge[:material/Info: Info]ㅤ설명", unsafe_allow_html=True)

    df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
    df['날짜_dt'] = df['날짜']

    start_period = df['날짜_dt'].min().to_period("M")
    end_period   = df['날짜_dt'].max().to_period("M")
    month_options = [p.to_timestamp() for p in pd.period_range(start=start_period, end=end_period, freq="M")]
    default_start, default_end = (month_options[-2], month_options[-1]) if len(month_options)>=2 else (month_options[0], month_options[0])

    start_sel, end_sel = st.select_slider(
        "기간(월)", options=month_options, value=(default_start, default_end),
        format_func=lambda x: x.strftime("%Y-%m"), key="v2_period"
    )

    # 1) 탭을 최상단에 먼저 렌더
    tabs = st.tabs(["일반", "경쟁사", "소비자", "자사"])
    patterns = {"일반":"일반", "경쟁사":"경쟁사", "소비자":"소비", "자사":"자사"}



    # 기간 필터 적용 (글로벌)
    period_start, period_end = start_sel, end_sel + MonthEnd(0)
    dfp_all = df[(df['날짜_dt'] >= period_start) & (df['날짜_dt'] <= period_end)].copy()

    # 3) 각 탭 내부: 키워드 선택 → 연령대 → 그래프/표
    age_order = ['19-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59']

    for tab, label in zip(tabs, ["일반","경쟁사","소비자","자사"]):
        with tab:
            pat = patterns[label]
            dft = dfp_all[dfp_all['키워드유형'].astype(str).str.contains(pat)]
            if dft.empty:
                st.warning(f"{label} 유형 데이터 없음")
                continue

            with st.expander("기본 필터", expanded=True):
                c1, c2, c3, c4 = st.columns([2,3,2,3])
                with c1:
                    granularity = st.radio("집계 단위", ["일","주","월"], horizontal=True, index=0, key=f"v2_gran_{label}")
                with c2:
                    view_mode_ui = st.radio("표시 방식", ["연령대별", "키워드별", "전체합"], horizontal=True, index=0, key=f"v2_view_{label}")
                with c3:
                    chart_kind  = st.radio("그래프", ["누적 막대", "꺾은선"], horizontal=True, index=0, key=f"v2_chart_{label}")
                with c4:
                    scale_mode  = st.radio("스케일", ["절댓값", "백분율", "로그"], horizontal=True, index=0, key=f"v2_scale_{label}")


            with st.expander("고급 필터", expanded=False):

                # (1) 키워드 선택
                kw_all = sorted(dft['키워드'].dropna().unique().tolist()) if '키워드' in dft.columns else []
                sel_kw = st.multiselect("키워드 선택", kw_all, default=kw_all, key=f"v2_kw_{label}")
                if kw_all:
                    dft = dft[dft['키워드'].isin(sel_kw)]

                # (2) 연령대 선택
                sel_ages = st.multiselect("연령대 선택", age_order, default=age_order, key=f"v2_ages_{label}")

                # 이 탭의 유형 고정
                type_all_in_tab = sorted(dft['키워드유형'].dropna().unique().tolist())
                sel_types = type_all_in_tab

            # (3) UI → 내부 표기 매핑
            if view_mode_ui == "연령대별":
                view_mode_effective = "연령대별(스택)"
            elif view_mode_ui == "키워드별":
                view_mode_effective = "키워드별(스택)"
            else:
                view_mode_effective = "전체합"

            # (4) 그래프 + 표
            render_chart_and_table(
                dft, granularity, view_mode_effective, chart_kind, scale_mode,
                sel_types, sel_ages,
                title_note=f"V2 · {label} 탭"
            )

if __name__ == '__main__':
    main()
