import pyodbc
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Dados de conexão ao banco SQL Server
def retornar_conexao_sql():
    server = "LAPTOP-3H7TVLV9"  # Substitua com seu servidor
    database = "Datalake"  # Substitua com o nome do seu banco de dados
    string_conexao = 'Driver={SQL Server Native Client 11.0};Server=' + server + ';Database=' + database + ';Trusted_Connection=yes;'
    conexao = pyodbc.connect(string_conexao)
    return conexao

# Função para extrair dados (Aqui, você pode obter dados de uma API ou fonte de dados externa)
def extrair_dados():
    # Exemplo de dados simulados de usuários
    dados_usuarios = [
        {"nome": "João Silva", "idade": 28, "estado": "SP", "profissao": "Engenheiro"},
        {"nome": "Maria Oliveira", "idade": 34, "estado": "RJ", "profissao": "Médica"},
        {"nome": "Carlos Pereira", "idade": 40, "estado": "MG", "profissao": "Advogado"},
    ]
    return dados_usuarios

# Função para transformar os dados
def transformar_dados():
    # Extrair dados de usuários
    dados = extrair_dados()
    # No exemplo, não estamos transformando os dados, mas você pode modificar conforme necessário
    return dados

# Função para armazenar no banco de dados SQL Server
def carregar_dados():
    conn = retornar_conexao_sql()
    cursor = conn.cursor()
    
    # Criação da tabela, se não existir
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Cadastros' AND xtype='U')
        CREATE TABLE Usuarios.Cadastros (
            id INT IDENTITY(1,1) PRIMARY KEY,
            strNome NVARCHAR(100),
            strIdade INT,
            strEstado NVARCHAR(2),
            strProfissao NVARCHAR(50),
            data_extracao DATETIME DEFAULT GETDATE()
        )
    """)
    
    # Insere os dados extraídos e transformados
    dados = transformar_dados()
    for usuario in dados:
        cursor.execute("""
            INSERT INTO Usuarios.Cadastros (strNome, strIdade, strEstado, strProfissao)
            VALUES (?, ?, ?, ?)""", 
            (usuario["nome"], usuario["idade"], usuario["estado"], usuario["profissao"]))
    
    # Commit e fechamento da conexão
    conn.commit()
    conn.close()

# Definição dos argumentos padrão do DAG
definir_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Definição do DAG
dag = DAG(
    'usuario_pipeline',
    default_args=definir_default_args,
    schedule_interval='@daily'
)

# Tarefas do DAG
task1 = PythonOperator(task_id='extrair', python_callable=extrair_dados, dag=dag)
task2 = PythonOperator(task_id='transformar', python_callable=transformar_dados, dag=dag)
task3 = PythonOperator(task_id='carregar', python_callable=carregar_dados, dag=dag)

task1 >> task2 >> task3