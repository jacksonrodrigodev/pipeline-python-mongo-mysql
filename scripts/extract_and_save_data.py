# Importando as bibliotecas
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
import os


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

    # URL da API
    url = "https://labdados.com/produtos"

    # Extraindo dados da API
    data = extract_api_data(url)

    # Inserindo dados na coleção
    inserted = insert_data(col, data)

    # Imprimindo a quantidade de documentos inseridos
    print(f"Foram inseridos {inserted} documentos na coleção 'produtos'.")

    client.close()