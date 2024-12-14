import os
import gdown
from dotenv import load_dotenv 
import duckdb
from sqlalchemy import create_engine

from datetime import datetime

def conectar_banco():
    """Conecta ao banco de dados DuckDB, cria o banco de dados se nao exister""" 
    return duckdb.connect(database='duckdb.db', read_only=False)

def inicializar_tabela(con):
    """Cria a tabela se ela nao existir"""
    con.execute("""
        CREATE TABLE IF NOT EXISTS historico_arquivos (
            nome_arquivo VARCHAR,
            horario_processamento TIMESTAMP
        )
    """)

def registrar_arquivo(con, nome_arquivo):
    """registra um novo arquivo no banco de dados com o horario atual"""
    con.execute("""
        INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
        VALUES (?, ?)
    """, (nome_arquivo, datetime.now()))

def arquivos_processados(con):
    """retorna um set com os nomes de todos os arquivos ja processados"""
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall())

def baixar_arquivos_google_drive(GOOGLE_DRIVE_FOLDER_URL, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(GOOGLE_DRIVE_FOLDER_URL, output=diretorio_local, quiet=False, use_cookies=False)

def listar_arquivos(diretorio):
    arquivos = []
    lista_arquivos = os.listdir(diretorio)
    for index, file in enumerate(lista_arquivos):
        if file.endswith((".csv", ".json", ".parquet")):
            caminho_completo = os.path.join(diretorio, file)
            arquivos.append(caminho_completo)
    
    print(arquivos)
    return arquivos

def ler_arquivo(caminho_arquivo):
    aux = caminho_arquivo.split(".")[-1]
    caminho_completo_arquivo = "data/gdown/" + caminho_arquivo
    match aux:
        case "json":
            df_duckdb = duckdb.read_json(caminho_completo_arquivo)
        case "csv":
            df_duckdb = duckdb.read_csv(caminho_completo_arquivo)
        case "parquet":
            df_duckdb = duckdb.read_parquet(caminho_completo_arquivo)
        case _:
            print(f"arquivo de extensao {aux} nao suportado...")
    
    #print(df_duckdb)
    #print(type(df_duckdb))
    return df_duckdb

def transforma_duckdb_para_dataframe(df):
    query = """
        SELECT *,
            quantidade * valor AS total_vendas
        FROM df
    """

    df_transformado = duckdb.sql(query).df()
    #print(df_transformado.head())
    return df_transformado

def salvar_no_postgresql(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL) # postgresql+psycopg2://scott:tiger@localhost/test
    # salvar dados no postgresql
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)

def pipeline():
    load_dotenv()  # take environment variables from .env.

    # Acessa a variável de ambiente
    GOOGLE_DRIVE_FOLDER_URL = os.getenv("GOOGLE_DRIVE_FOLDER_URL")

    diretorio_local = "./data/gdown"
    #baixar_arquivos_google_drive(GOOGLE_DRIVE_FOLDER_URL, diretorio_local)

    lista_arquivos = listar_arquivos(diretorio_local)
    con = conectar_banco()
    inicializar_tabela(con)
    processados = arquivos_processados(con)

    logs = []
    for arquivo in lista_arquivos:
        nome_arquivo = os.path.basename(arquivo)
        if nome_arquivo not in processados:
            #print(nome_arquivo)
            df_duckdb = ler_arquivo(nome_arquivo)
            df = transforma_duckdb_para_dataframe(df_duckdb)
            salvar_no_postgresql(df, tabela="vendas_calculado")
            registrar_arquivo(con, nome_arquivo)
            print(f"arquivo {nome_arquivo} processado e salvo no banco de dados")
            logs.append(f"arquivo {nome_arquivo} processado e salvo no banco de dados")
        else:
            print(f"arquivo {nome_arquivo} ja foi processado anteriormente")
            logs.append(f"arquivo {nome_arquivo} ja foi processado anteriormente")
    return logs


if __name__ == "__main__":
    pipeline()