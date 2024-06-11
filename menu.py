import streamlit as st



def menu():
    st.sidebar.page_link("home.py", label="Home")
    st.sidebar.page_link("pages/tablas.py", label="Tablas")
    st.sidebar.page_link("pages/procesos.py", label="Procesos")
    st.sidebar.page_link("pages/roles.py", label="Roles")