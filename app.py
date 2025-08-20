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

from streamlit_option_menu import option_menu

st.set_page_config(
    layout="wide",
    page_title="SLPR Dashboard",
    page_icon="🐋"  # 이모지 또는 이미지 URL
)



view01_name = "WV | 매출 종합 대시보드"
view02_name = "WV | 액션 종합 대시보드"
view03_name = "WV | 퍼포먼스 대시보드"
view04_name = "WV | 언드(PPL) 대시보드"
view05_name = "GA | 트래픽 대시보드"
view06_name = "GA | PDP조회 대시보드"
view07_name = "GA | TEST"

with st.sidebar:
    st.sidebar.header("Menu")
    
    
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
    ],
    # icons=[
    #             # https://icons.getbootstrap.com
    #     "piggy-bank-fill",
    #     "piggy-bank-fill",
    #     "currency-exchange",
    #     "bar-chart-line-fill",
    #     "tags-fill"
    # ],
    # menu_icon="",
    menu_icon="app-indicator",
    default_index=1,
    orientation="vertical",
    styles={
        # 컨테이너 배경을 투명하게, 패딩 최소화
        # "container": {
        #     "padding": "0px 0px 0px 0px",
        #     "background-color": "transparent",
        #     "border": "none"
        # },
        "nav-link": {"font-weight": "normal"},
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
