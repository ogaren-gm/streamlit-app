import streamlit as st
import time

USER_DB = {
    "orange": {"PW": "1234", "user_level": "A"},
}

def check_login():
    # 1. 이미 인증된 세션인지 확인
    if st.session_state.get("authenticated"):
        return True

    # 2. 로그인 폼 렌더링
    st.subheader("LOGIN")
    st.markdown("대시보드 접근을 위해 인증이 필요합니다.")
    st.subheader(" ")
    
    with st.form("login_form"):
        user_id = st.text_input("아이디").strip()
        user_pw = st.text_input("비밀번호", type="password")
        submit  = st.form_submit_button("ㅤ로그인 하기ㅤ")

        if submit:
            if user_id in USER_DB and USER_DB[user_id]["PW"] == user_pw:
                user_level = USER_DB[user_id]["user_level"]
                
                # 세션 상태 저장
                st.session_state["authenticated"] = True
                st.session_state["user_id"] = user_id
                st.session_state["user_level"] = user_level
                
                if user_level == "A":
                    st.toast("Activated")
                else:
                    st.error("인증 정보가 일치하지 않습니다.")
                time.sleep(0.5)
                st.rerun()
            else:
                st.error("인증 정보가 일치하지 않습니다.")
    
    return False

def logout():
    st.session_state["authenticated"] = False
    st.session_state["user_id"] = None
    st.session_state["user_level"] = None
    st.rerun()



# import os
# import streamlit as st
# import login
# import streamlit.components.v1 as components

# # ---------------------------------------------------------------
# # CONFIG
# # ---------------------------------------------------------------
# if st.session_state.get("authenticated"):
#     layout_setting = "wide"
# else:
#     layout_setting = "centered"
# st.set_page_config(
#     layout=layout_setting,
#     page_title="Orange Dashboard",
#     page_icon="🍊"
# )

# # ---------------------------------------------------------------
# # LOGIN
# # ---------------------------------------------------------------
# if not login.check_login():
#     st.stop()
    
# if st.query_params.get("logout") == "1":
#     st.query_params.clear()
#     login.logout()
#     st.stop()

# # ---------------------------------------------------------------
# # LOGO
# # ---------------------------------------------------------------
# st.logo(
#     "assets/logo.png",
#     icon_image="assets/logo_s.png",
#     size="large"
# )

# # ---------------------------------------------------------------
# # SIDEBAR USER
# # ---------------------------------------------------------------
# with st.sidebar:
#     user_id    = st.session_state.get("user_id", "-")
#     user_level = st.session_state.get("user_level", "-")

#     st.markdown(
#         f"""
#         <div id="sidebar-user-box-source" style="display:none;">
#             <div class="user-box" style="display: flex; align-items: center; gap: 12px;">
#                 <img src="https://cdn-icons-png.flaticon.com/512/149/149071.png" style="width: 38px; height: 38px;">
#                 <div>
#                     <div class="user-box-id">{user_id}</div>
#                     <div class="user-box-level" style="margin-bottom: 0px;">{user_level}</div>
#                 </div>
#             </div>
#         </div>
#         """,
#         unsafe_allow_html=True
#     )

# # ---------------------------------------------------------------
# # MARKDOWN
# # ---------------------------------------------------------------
# st.markdown("""
# <style>
# /* 로고 높이 */
# [data-testid="stSidebarHeader"] {
#     height: 110px;
#     align-items: flex-end;
#     padding-bottom: 20px;
# }

# /* 메뉴 간격 */
# [data-testid="stSidebarNav"] li {
#     margin-bottom: 1px;
# }

# /* 헤더 스타일 */
# [data-testid="stNavSectionHeader"] {
#     font-size: 16px;
#     font-weight: 700;
#     margin-top: 14px;
# }

# /* 선택된 메뉴 볼드 낮추기 */
# [data-testid="stSidebarNavLink"][aria-current="page"] {
#     font-weight: 500 !important;
# }

# /* 유저 박스 */
# [data-testid="stSidebarNav"] > .user-box {
#     margin: 8px 10px 50px 10px;
# }

# .user-box {
#     border: 1px solid rgba(120, 120, 120, 0.20);
#     border-radius: 10px;
#     padding: 12px 14px;
#     background: white;

# }

# .user-box-id {
#     font-size: 15px;
#     font-weight: 600;
#     color: rgba(49, 51, 63);
#     line-height: 1.2;
#     margin-bottom: 3px;
# }

# .user-box-level {
#     font-size: 12px;
#     color: rgba(49, 51, 63, 0.55);
#     letter-spacing: 0.06em;
#     margin-bottom: 0px;
# }

# </style>
# """, unsafe_allow_html=True)


# # ---------------------------------------------------------------
# # NAVIGATION
# # ---------------------------------------------------------------
# pages = {
#     "FLOW": [
#         st.Page("views/view01.py", title="ㅤ트래픽 대시보드"),
#         st.Page("views/view02.py", title="ㅤPDP조회 대시보드"),
#         st.Page("views/view03.py", title="ㅤ장바구니 대시보드"),
#         st.Page("views/view04.py", title="ㅤ쇼룸 대시보드 🛠️"),
#         st.Page("views/view05.py", title="ㅤCMP 대시보드"),
#     ],
#     "GOAL": [
#         st.Page("views/view21.py", title="ㅤ퍼포먼스 대시보드"),
#     ],
#     "BUZZ": [
#         st.Page("views/view31.py", title="ㅤ언드·PPL 대시보드 ✨ᵁᴾᴰᴬᵀᴱ"),
#         st.Page("views/view32.py", title="ㅤ키워드 대시보드 ✨ᵁᴾᴰᴬᵀᴱ"),
#     ],
# }

# pg = st.navigation(pages)

# components.html(
#     """
#     <script>
#     const moveUserBox = () => {
#         const doc = window.parent.document;
#         const nav = doc.querySelector('[data-testid="stSidebarNav"]');
#         const navItems = doc.querySelector('[data-testid="stSidebarNavItems"]');
#         const source = doc.querySelector('#sidebar-user-box-source .user-box');

#         if (!nav || !navItems || !source) return;

#         if (!nav.querySelector(':scope > .user-box')) {
#             const clone = source.cloneNode(true);
#             nav.insertBefore(clone, navItems);
#         }
#     };

#     const timer = setInterval(() => {
#         moveUserBox();
#     }, 300);

#     setTimeout(() => clearInterval(timer), 10000);
#     </script>
#     """,
#     height=0,
# )

# pg.run()