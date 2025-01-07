# pipeline-python-mongo-mysql

## Passo a Passo para Rodar o Projeto

1. **Clone o repositório:**
    ```bash
    git clone https://github.com/seu-usuario/pipeline-python-mongo-mysql.git
    cd pipeline-python-mongo-mysql
    ```

2. **Crie um ambiente virtual e ative-o:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Configure as variáveis de ambiente:**
    Crie um arquivo `.env` na raiz do projeto e adicione as seguintes variáveis:
    ```
    MONGO_URI=<sua_uri_mongo>
    MYSQL_HOST=<seu_host_mysql>
    MYSQL_USER=<seu_usuario_mysql>
    MYSQL_PASSWORD=<sua_senha_mysql>
    MYSQL_DB=<seu_banco_de_dados_mysql>
    ```

5. **Crie uma conta no MongoDB Atlas:**
    Acesse o [MongoDB Atlas](https://www.mongodb.com/cloud/atlas) e siga as instruções para criar uma conta e configurar um cluster. Obtenha a URI de conexão para usar na variável `MONGO_URI`.

6. **Crie um banco de dados MySQL:**
    No WSL, instale o MySQL e crie um banco de dados para usar no projeto.

7. **Execute os scripts na ordem correta:**
    ```bash
    python extract_and_save_data.py
    python transform_data.py
    python load_data.py
    ```

Pronto! Agora você deve ter o projeto rodando corretamente.
