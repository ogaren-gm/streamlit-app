import os
import streamlit as st
import importlib
from streamlit_option_menu import option_menu

# ---------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------
st.set_page_config(
    layout="wide",
    page_title="ORANGE 대시보드",
    page_icon="🍊"
)

DEV_MODE = os.getenv("DEV_MODE", "0") == "1"   # 운영: 0 / 개발: 1


# ---------------------------------------------------------------
# VIEW REGISTRY
# - lazy import + (optional) reload
# ---------------------------------------------------------------
VIEWS = {
    "🚀 트래픽 대시보드"   : ("views.view01", "main"),
    "🔍 PDP조회 대시보드"  : ("views.view02", "main"),
    "🧺 장바구니 대시보드"  : ("views.view03", "main"),
    "키워드 대시보드"   : ("views.view21", "main"),
    "언드·PPL 대시보드" : ("views.view22", "main"),
    "매출 종합 대시보드" : ("views.view31", "main"),
    "퍼포먼스 대시보드"  : ("views.view32", "main"),
}


def load_view(module_path: str, func_name: str):
    """
    - 운영모드: import만 (빠름)
    - 개발모드: import 후 reload (코드 수정 즉시 반영)
    """
    mod = importlib.import_module(module_path)
    if DEV_MODE:
        importlib.reload(mod)
    return getattr(mod, func_name)


# ---------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------
with st.sidebar:
    st.markdown(
        """
        <div style="display:flex; align-items:baseline;">
            <span style="font-size:26px; font-weight:700; color:#31333F;">O\u200AR\u200AA\u200AN\u200AG\u200AE</span>
            <span style="font-size:16px; color:#8E9087; margin-left:10px;">Dashboard</span>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.header(" ")
    st.divider()
    st.sidebar.header("Menu")

    st.markdown(
        """
        <style>
        .nav-link i { display: none !important; }
        </style>
        """,
        unsafe_allow_html=True
    )

    options = list(VIEWS.keys())

    selected = option_menu(
        menu_title="",
        options=options,
        default_index=0,
        orientation="vertical",
        styles={
            "container": {"padding": "0!important", "background-color": "transparent", "border": "none"},
            "icon": {"display": "none", "width": "0px", "margin": "0px", "padding": "0px", "opacity": "0"},
            "nav-link": {"font-size": "16px", "text-align": "left", "margin": "2px"},
            "nav-link-selected": {"font-weight": "normal"},
        }
    )

    st.markdown("---")


# ---------------------------------------------------------------
# ROUTING
# ---------------------------------------------------------------
module_path, func_name = VIEWS[selected]
view_main = load_view(module_path, func_name)
view_main()
