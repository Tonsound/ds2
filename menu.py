import streamlit as st



def menu():
    st.sidebar.image("images/images/App-Banco-Ripley.png", use_column_width=True) 
    st.sidebar.page_link("home.py", label="Home")
    st.sidebar.page_link("pages/tablas.py", label="Tablas")
    st.sidebar.page_link("pages/procesos.py", label="Procesos")
    st.sidebar.page_link("pages/roles.py", label="Roles")