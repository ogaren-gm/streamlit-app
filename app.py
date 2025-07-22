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

from streamlit_option_menu import option_menu

st.set_page_config(layout="wide", page_title="SLPR Analytics")

view01_name = "트래픽 대시보드"
view03_name = "퍼포먼스 대시보드"
view02_name = "제품 대시보드"

with st.sidebar:
    st.sidebar.header("Menu")
    
    selected = option_menu(
    menu_title="",
    options=[
        view01_name,
        view03_name,
        view02_name
    ],
    icons=[
        # https://icons.getbootstrap.com
        "bar-chart-line-fill",
        "currency-exchange",
        "tags-fill"
    ],
    # menu_icon="",
    menu_icon="app-indicator",
    default_index=1,
    orientation="vertical",
    styles={
        "nav-link": {"font-weight": "normal"},
        "nav-link-selected": {"font-weight": "normal"},
    }
    )
    st.markdown("---")
    # (필터가 필요하면 이 아래에 st.date_input, st.selectbox 등 추가)

if selected == view01_name:
    view01_main()
elif selected == view02_name:
    view02_main()
elif selected == view03_name:
    view03_main()
    
    
    
