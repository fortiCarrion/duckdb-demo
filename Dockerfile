FROM python:3.12

# Define o diretório de trabalho na imagem
WORKDIR /app

# Copia o arquivo de dependências para a imagem
COPY requirements.txt .

# Instala as dependências da aplicação
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos da aplicação para o diretório de trabalho
COPY . .

# Especifica a porta de conexão em tempo de execução
EXPOSE 8051
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]