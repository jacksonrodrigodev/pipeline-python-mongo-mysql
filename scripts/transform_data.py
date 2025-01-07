from dotenv import load_dotenv
import os
from utils import (
    connect_mongo,
    create_connect_db,
    create_connect_collection,
    visualize_collection,
    rename_column,
    select_per_query,
    format_date,
    save_csv,
)
import pandas as pd

if __name__ == "__main__":
    # URI de conexão com o MongoDB
    load_dotenv()
    uri = os.getenv("URI")

    # Conectando ao MongoDB
    client = connect_mongo(uri)

    # Criando e conectando ao banco de dados
    db = create_connect_db(client, "bd_produtos")

    # Criando e conectando à coleção
    col = create_connect_collection(db, "produtos")

    # Visualizando a coleção
    visualize_collection(col)

    # Renomeando as colunas
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    # Selecionando a categoria "livros"
    query = {"Categoria do Produto": "livros"}
    df_livros = select_per_query(col, query)

    df_livros = format_date(df_livros)

    # Criando o diretório ../data se não existir
    os.makedirs("../data", exist_ok=True)

    # Salvando os dados da categoria "livros" em CSV
    save_csv(df_livros, "../data/tabela_livros2.csv")

    query = {"Data da Compra": {"$regex": "/202[1-9]"}}
    # Selecionando os produtos vendidos a partir de 2021
    df_2021 = select_per_query(col, query)

    # Passo 6: Formatar a coluna de datas usando a função format_date
    df_2021 = format_date(df_2021)

    # Salvando os dados dos produtos vendidos a partir de 2021 em CSV
    save_csv(df_2021, "../data/tabela_2021_em_diante2.csv")

    client.close()
