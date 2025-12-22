import streamlit as st
import importlib

import views.view01
importlib.reload(views.view01)
from views.view01 import main as view01_main

import views.view02
importlib.reload(views.view02)
from views.view02 import main as view02_main

import views.view03
importlib.reload(views.view03)
from views.view03 import main as view03_main

import views.view04
importlib.reload(views.view04)
from views.view04 import main as view04_main

import views.view05
importlib.reload(views.view05)
from views.view05 import main as view05_main

import views.view06
importlib.reload(views.view06)
from views.view06 import main as view06_main

import views.view07
importlib.reload(views.view07)
from views.view07 import main as view07_main

import views.view09
importlib.reload(views.view09)
from views.view09 import main as view09_main

from streamlit_option_menu import option_menu

st.set_page_config(
    layout="wide",
    page_title="ORANGE 대시보드",
    page_icon="🍊"  # 이모지 또는 이미지 URL
)


# 페이지 이름 선언
view01_name = "매출 종합 대시보드"
view02_name = "액션 종합 대시보드"
view03_name = "퍼포먼스 대시보드"
view04_name = "언드·PPL 대시보드"
view05_name = "키워드 대시보드"
view06_name = "GA 트래픽 대시보드"
view07_name = "GA PDP 대시보드"
view09_name = "GA CMP 대시보드"


# 색깔 선언
sub_bg__lut_media = "#eff6ff"
sub_bd__lut_media = "#93c5fd"
sub_fg__lut_media = "#1d4ed8" 
sub_bg__lut_ga = "#fff7ed"
sub_bd__lut_ga = "#fdba74"
sub_fg__lut_ga = "#c2410c"



with st.sidebar:

    # 대시보드 제목
    st.markdown(
        """
        <div style="display:flex; align-items:baseline;">
            <span style="font-size:26px; font-weight:700; color:#31333F;">O\u200AR\u200AA\u200AN\u200AG\u200AE</span>
            <span style="font-size:16px; color:#8E9097; margin-left:10px;">Dashboard</span>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # 공백용
    st.header(" ")
    # st.link_button("🍊 대시보드 활용 가이드", "https://www.notion.so/25b5927da69980ddbafee03fad072d68?source=copy_link", type="secondary")

    # 메뉴
    st.divider()
    st.sidebar.header("Menu")
    
        
    st.markdown(
        """
        <style>
        /* option_menu 왼쪽 아이콘 숨기기 */
        .nav-link i {
            display: none !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    ) 
    
    selected = option_menu(
        menu_title="",
        options=[
            view01_name,
            view02_name,
            view03_name,
            view04_name,
            view05_name,
            view06_name,
            view07_name,
            view09_name,
        ],
        # icons=[
        #     # https://icons.getbootstrap.com
        # ],
        # menu_icon="?",
        default_index=0,
        orientation="vertical",
        styles={
            "container": {
                "padding": "0!important",
                "background-color": "transparent",
                "border": "none"
            },
            "icon": {
                "display": "none",
                "width": "0px",
                "margin": "0px",
                "padding": "0px",
                "opacity": "0"
            },
            "nav-link": {
                "font-size": "16px",
                "text-align": "left",
                "margin": "2px",
                # 아이콘 공간이 사라지니 좌측 패딩도 약간 줄이기(선택)
                # "padding": "6px 8px"
            },
            "nav-link-selected": {"font-weight": "normal"},
        }
    )
    
    st.markdown("---")


if selected == view01_name:
    view01_main()
elif selected == view02_name:
    view02_main()
elif selected == view03_name:
    view03_main()
elif selected == view04_name:
    view04_main()
elif selected == view05_name:
    view05_main()
elif selected == view06_name:
    view06_main()
elif selected == view07_name:
    view07_main()
elif selected == view09_name:
    view09_main()
