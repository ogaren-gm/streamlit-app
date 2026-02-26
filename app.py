import os
import streamlit as st

# ---------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------
st.set_page_config(
    layout="wide",
    page_title="ORANGE 대시보드",
    page_icon="🍊"
)

# ---------------------------------------------------------------
# LOGO
# ---------------------------------------------------------------
# st.logo("https://www.svgrepo.com/show/484669/orange.svg")

st.logo(
    "assets/logo.png",
    icon_image="assets/logo_s.png",
    size="large"
)

# ---------------------------------------------------------------
# MARKDOWN
# ---------------------------------------------------------------
st.markdown("""
<style>

/* 로고 높이 */
[data-testid="stSidebarHeader"] {
    height: 160px;
    align-items: center;
}

/* 메뉴 간격 */
[data-testid="stSidebarNav"] li {
    margin-bottom: 4px;
}

/* 헤더 스타일 */
[data-testid="stNavSectionHeader"] {
    font-size: 16px;
    font-weight: 600;
}

/* 선택된 메뉴 볼드 낮추기 */
[data-testid="stSidebarNavLink"][aria-current="page"] {
    font-weight: 500 !important;
}

</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------
# NAVIGATION
# ---------------------------------------------------------------
pages = {
    "FLOW": [
        st.Page("views/view01.py", title="ㅤ트래픽 대시보드"),
        st.Page("views/view02.py", title="ㅤPDP조회 대시보드"),
        st.Page("views/view03.py", title="ㅤ장바구니 대시보드"),
        st.Page("views/view04.py", title="ㅤ쇼룸 대시보드"),
        st.Page("views/view05.py", title="ㅤCMP 대시보드"),
    ],
    "BUZZ": [
        st.Page("views/view21.py", title="ㅤ(키워드 대시보드)"),
        st.Page("views/view22.py", title="ㅤ(언드·PPL 대시보드)"),
    ],
    "GOAL": [
        st.Page("views/view31.py", title="ㅤ(매출 종합 대시보드)"),
        st.Page("views/view32.py", title="ㅤ퍼포먼스 대시보드"),
    ],
}

pg = st.navigation(pages)
pg.run()
