import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente (.env)
load_dotenv()

# Conectar ao banco MySQL
conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE"),
    port=os.getenv("MYSQL_PORT", 3306)
)

cursor = conn.cursor()

# Criar tabela se não existir
cursor.execute("""
CREATE TABLE IF NOT EXISTS vendas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data_venda DATE,
    produto VARCHAR(255),
    categoria VARCHAR(255),
    quantidade INT,
    preco_unitario FLOAT,
    total_venda FLOAT,
    cliente VARCHAR(255)
);
""")

# Ler o CSV
df = pd.read_csv('relatorio_vendas.csv')

# Inserir os dados
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO vendas (data_venda, produto, categoria, quantidade, preco_unitario, total_venda, cliente)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        row['Data da Venda'],
        row['Produto'],
        row['Categoria'],
        row['Quantidade'],
        row['Preço Unitário'],
        row['Total da Venda'],
        row['Cliente']
    ))

conn.commit()
cursor.close()
conn.close()

print("✅ Dados inseridos no MySQL com sucesso.")
