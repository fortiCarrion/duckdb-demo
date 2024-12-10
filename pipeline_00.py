import os
import gdown
from dotenv import load_dotenv 
import duckdb
from sqlalchemy import create_engine

def baixar_arquivos_google_drive(GOOGLE_DRIVE_FOLDER_URL, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(GOOGLE_DRIVE_FOLDER_URL, output=diretorio_local, quiet=False, use_cookies=False)

def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    lista_arquivos = os.listdir(diretorio)
    for index, file in enumerate(lista_arquivos):
        if file.endswith(".csv"):
            caminho_completo = os.path.join(diretorio, file)
            arquivos_csv.append(caminho_completo)
    
    print(arquivos_csv)
    return arquivos_csv

def ler_arquivo_csv(caminho_arquivo):
    df_duckdb = duckdb.read_csv(caminho_arquivo)
    print(df_duckdb)
    print(type(df_duckdb))
    return df_duckdb

def transforma_duckdb_para_dataframe(df):
    query = """
        SELECT *,
            quantidade * valor AS total_vendas
        FROM df
    """

    df_transformado = duckdb.sql(query).df()
    print(df_transformado.head())
    return df_transformado

def salvar_no_postgresql(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL) # postgresql+psycopg2://scott:tiger@localhost/test
    # salvar dados no postgresql
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)


if __name__ == "__main__":
    load_dotenv()  # take environment variables from .env.

    # Acessa a variável de ambiente
    GOOGLE_DRIVE_FOLDER_URL = os.getenv("GOOGLE_DRIVE_FOLDER_URL")

    diretorio_local = "./data/gdown"
    #baixar_arquivos_google_drive(GOOGLE_DRIVE_FOLDER_URL, diretorio_local)

    lista_arquivos = listar_arquivos_csv(diretorio_local)

    for arquivo in lista_arquivos:    
        df_duckdb = ler_arquivo_csv(lista_arquivos)
        df = transforma_duckdb_para_dataframe(df_duckdb)
        salvar_no_postgresql(df, tabela="vendas_calculado")