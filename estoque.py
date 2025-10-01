from pymongo import MongoClient
from datetime import datetime

# Conexão com o MongoDB Atlas
conn_str = "mongodb+srv://EXEMPLO:EXEMPLO@cluster0.etopt0b.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(conn_str)

db = client['distribuidora']
estoque = db['produtos']

produtos = [
    {
        "nome": "Água Mineral",
        "preco": 3.50,
        "validade": datetime.strptime("15/06/2027", "%d/%m/%Y"),
        "quantidade": 200,
        "categoria": "Bebida"
    },
    {
        "nome": "Refrigerante Cola",
        "preco": 6.00,
        "validade": datetime.strptime("01/11/2026", "%d/%m/%Y"),
        "quantidade": 150,
        "categoria": "Bebida"
    },
    {
        "nome": "Suco de Laranja",
        "preco": 5.25,
        "validade": datetime.strptime("20/03/2026", "%d/%m/%Y"),
        "quantidade": 80,
        "categoria": "Bebida"
    },
    {
        "nome": "Cerveja Puro Malte",
        "preco": 9.90,
        "validade": datetime.strptime("10/12/2025", "%d/%m/%Y"),
        "quantidade": 120,
        "categoria": "Bebida"
    },
    {
        "nome": "Energético PowerUp",
        "preco": 11.75,
        "validade": datetime.strptime("05/05/2027", "%d/%m/%Y"),
        "quantidade": 60,
        "categoria": "Bebida"
    },
    {
        "nome": "Bala Wals",
        "preco": 2.5,
        "validade": datetime.strptime("05/05/2028", "%d/%m/%Y"),
        "quantidade": 50,
        "categoria": "Doce"
    },
    {
        "nome": "Chiclete BigBig",
        "preco": 0.5,
        "validade": datetime.strptime("05/05/2029", "%d/%m/%Y"),
        "quantidade": 200,
        "categoria": "Doce"
    },
    {
        "nome": "Detergente Neutro",
        "preco": 2.80,
        "validade": datetime.strptime("10/05/2026", "%d/%m/%Y"),
        "quantidade": 300,
        "categoria": "Limpeza"
    },
    {
        "nome": "Sabão em Pó",
        "preco": 15.90,
        "validade": datetime.strptime("22/11/2027", "%d/%m/%Y"),
        "quantidade": 100,
        "categoria": "Limpeza"
    },
    {
        "nome": "Desinfetante Floral",
        "preco": 8.75,
        "validade": datetime.strptime("14/02/2026", "%d/%m/%Y"),
        "quantidade": 80,
        "categoria": "Limpeza"
    }
]


for p in produtos:
    if estoque.find_one({"nome": p["nome"]}):
        print(f"Produto '{p['nome']}' já existe. Pulando...")
    else:
        resultado = estoque.insert_one(p)
        print(f"Produto '{p['nome']}' inserido com ID: {resultado.inserted_id}")