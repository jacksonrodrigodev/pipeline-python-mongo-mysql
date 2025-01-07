# Importando as bibliotecas
import requests
from pymongo import MongoClient
import pandas as pd

# Função para conectar ao MongoDB
def connect_mongo(uri):
    client = MongoClient(uri)
    return client

# Função para criar e conectar ao banco de dados    
def create_connect_db(client, db_name):
    db = client[db_name]
    return db

# Função para criar e conectar à coleção
def create_connect_collection(db, col_name):
    col = db[col_name]
    return col

# Função para extrair dados da API
def extract_api_data(url):
    response = requests.get(url)
    data = response.json()
    return data

# Função para inserir dados na coleção
def insert_data(col, data):
    result = col.insert_many(data)
    return len(result.inserted_ids)

# Função para visualizar a coleção
def visualize_collection(col):
    for doc in col.find():
        print(doc)

# Função para renomear as colunas
def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {col_name: new_name}})

# Função para criar um dataframe
def create_dataframe(lista):
    df = pd.DataFrame(lista)
    return df

# Função para selecionar a categoria "livros"
def select_per_query(col, query):
    df = create_dataframe(list(col.find(query)))
    return df

# Função para formatar a coluna de datas
def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"],format="%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")

    return df  

# Função para salvar o dataframe em CSV
def save_csv(df, path):
    df.to_csv(path, index=False)