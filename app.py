import time
import streamlit as st
from pipeline_01 import pipeline

st.title('Processador de Arquivos')

if st.button('Processar'):
    with st.spinner('Processando...'):
        time.sleep(2)
        logs = pipeline()
        # exibe os logs no streamlit
        for log in logs:
            time.sleep(.5)
            st.write(log)