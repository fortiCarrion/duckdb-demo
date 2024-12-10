FROM python:3.12
RUN pip install -r requirements.txt
COPY . /src
WORKDIR /src
EXPOSE 8051
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]