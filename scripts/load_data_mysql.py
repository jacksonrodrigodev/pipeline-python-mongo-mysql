import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv

def connect_mysql(host_name, user_name, pw):
    cnx = mysql.connector.connect(
        host=host_name,
        user=user_name,
        password=pw
    )
    return cnx

def create_cursor(cnx):
    return cnx.cursor()

def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    for db in cursor:
        print(db)

def create_table(cursor, db_name, query):
    cursor.execute(f"USE {db_name}")
    cursor.execute(query)

def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name}")
    cursor.execute("SHOW TABLES")
    for table in cursor:
        print(table)

def read_csv(path):
    return pd.read_csv(path)

def add_product_data(cnx, cursor, df, db_name, tb_name):
    len_columns = len(df.columns)
    lista_dados = [tuple(row) for i, row in df.iterrows()]
    lista_dados
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES ({','.join(['%s']*len_columns)})"
    cursor.executemany(sql, lista_dados)
    cnx.commit()

if __name__ == "__main__":
    # Conectando ao servidor MySQL
    host_name = os.getenv("DB_HOST")
    user_name = os.getenv("DB_USERNAME")
    pw = os.getenv("DB_PASSWORD")

    cnx = connect_mysql(host_name, user_name, pw)
    cursor = create_cursor(cnx)
    
    # Criando o banco de dados e a tabela
    create_database(cursor, "dbprodutos")
    show_databases(cursor)

    # Criando a tabela tb_livros
    query = """
    CREATE TABLE IF NOT EXISTS dbprodutos.tb_livros(
               id VARCHAR(100),
               Produto VARCHAR(100),
               Categoria_Produto VARCHAR(100),
               Preco FLOAT(10,2),
               Frete FLOAT(10,2),
               Data_Compra DATE,
               Vendedor VARCHAR(100),
               Local_Compra VARCHAR(100),
               Avaliacao_Compra INT,
               Tipo_Pagamento VARCHAR(100),
               Qntd_Parcelas INT,
               Latitude FLOAT(10,2),
               Longitude FLOAT(10,2),
               PRIMARY KEY (id));
    """
    create_table(cursor, "dbprodutos", query)
    show_tables(cursor, "dbprodutos")

    # Adicionando os dados à tabela tb_livros
    df_livros = read_csv("/home/jackson/pipeline-python-mongo-mysql/data/tabela_livros.csv")
    add_product_data(cnx, cursor, df_livros, "dbprodutos", "tb_livros")

    # Criando a tabela tb_produtos_2021_em_diante
    query = """
        CREATE TABLE IF NOT EXISTS dbprodutos.tb_produtos_2021_em_diante(
               id VARCHAR(100),
               Produto VARCHAR(100),
               Categoria_Produto VARCHAR(100),
               Preco FLOAT(10,2),
               Frete FLOAT(10,2),
               Data_Compra DATE,
               Vendedor VARCHAR(100),
               Local_Compra VARCHAR(100),
               Avaliacao_Compra INT,
               Tipo_Pagamento VARCHAR(100),
               Qntd_Parcelas INT,
               Latitude FLOAT(10,2),
               Longitude FLOAT(10,2),

               PRIMARY KEY (id));
    """
    create_table(cursor, "dbprodutos", query)
    show_tables(cursor, "dbprodutos")

    # Adicionando os dados à tabela tb_produtos_2021_em_diante
    df_produtos = read_csv("/home/jackson/pipeline-python-mongo-mysql/data/tabela_2021_em_diante.csv")
    add_product_data(cnx, cursor, df_produtos, "dbprodutos", "tb_produtos_2021_em_diante")

    cursor.execute("SELECT * FROM dbprodutos.tb_livros;")
    for row in cursor:
        print(row)
    
    cursor.execute("SELECT * FROM dbprodutos.tb_produtos_2021_em_diante;")
    for row in cursor:
        print(row)

    cursor.close()
    cnx.close()