from dotenv import load_dotenv
import os
from utils import (
    connect_mongo,
    create_connect_db,
    create_connect_collection,
    extract_api_data,
    insert_data,
)

if __name__ == "__main__":

    # URI de conexão com o MongoDB
    load_dotenv()
    uri = os.getenv("MONGODB_URI")

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
