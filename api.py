from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional, Any, Dict
from datetime import datetime
from pydantic import BaseModel
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
import redis
import json
import re

# ===== FastAPI =====
app = FastAPI(title="API Distribuidora", version="1.0.0")

# ===== Conexão MongoDB Atlas =====
conn_str = "mongodb+srv://*******:*******@cluster0.etopt0b.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(conn_str)
db = client["distribuidora"]
estoque = db["produtos"]

# ===== Redis (cache) =====
r = redis.Redis(host="localhost", port=6379, db=0)

# ===== Modelo basico =====
class Produto(BaseModel):
    nome: str
    preco: float
    validade: datetime
    quantidade: int
    categoria: Optional[str] = None

class ProdutoComID(Produto):
    id: str

def _map_doc(d: Dict[str, Any]) -> ProdutoComID:
    return ProdutoComID(
        id=str(d["_id"]),
        nome=d.get("nome"),
        preco=d.get("preco"),
        validade=d.get("validade"),
        quantidade=d.get("quantidade"),
        categoria=d.get("categoria"),
    )

# ===== Índices =====
@app.on_event("startup")
def ensure_indexes():
    estoque.create_index([("nome", ASCENDING)], name="nome_1") 
    estoque.create_index([("categoria", ASCENDING)], name="idx_categoria")


# ===== Rotas básicas =====
@app.get("/")
def home():
    return {"mensagem": "API de Produtos está no ar! Acesse /docs para testar."}

@app.get("/produtos", response_model=List[ProdutoComID])
def listar_produtos(limit: int = Query(200, ge=1, le=1000)):
    cache_key = f"produtos:{limit}"
    if r.exists(cache_key):
        return json.loads(r.get(cache_key))

    cur = estoque.find().limit(limit)
    data = [_map_doc(p).model_dump() for p in cur]

    r.setex(cache_key, 60, json.dumps(data, default=str))
    return data

@app.get("/produtos/busca", response_model=List[ProdutoComID])
def buscar_por_nome(nome: str = Query(..., min_length=1), limit: int = Query(50, ge=1, le=200)):
    """
    Busca por nome usando índice 'idx_nome' com regex ancorada (^).
    Usa Redis para cachear resultados por 60s.
    """
    cache_key = f"busca:{nome}:{limit}"
    if r.exists(cache_key):
        return json.loads(r.get(cache_key))

    pattern = f"^{re.escape(nome)}"
    cur = (
    estoque.find({"nome": {"$regex": pattern, "$options": "i"}})
    .hint("nome_1")  
    .limit(limit)
)

    data = [_map_doc(p).model_dump() for p in cur]

    r.setex(cache_key, 60, json.dumps(data, default=str))
    return data

@app.post("/produtos", response_model=ProdutoComID, status_code=201)
def adicionar_produto(produto: Produto):
    dados = produto.model_dump()
    res = estoque.insert_one(dados)
    novo = estoque.find_one({"_id": res.inserted_id})

    # limpa cache relacionado
    r.flushdb()

    return _map_doc(novo)

@app.delete("/produtos/{id}")
def remover_produto_por_id(id: str):
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido.")
    res = estoque.delete_one({"_id": oid})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Produto não encontrado.")

    # limpa cache
    r.flushdb()

    return {"mensagem": "Produto removido com sucesso.", "id": id}

@app.put("/produtos/{id}", response_model=ProdutoComID)
def atualizar_produto(id: str, produto: Produto):
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido.")
    upd = {"$set": produto.model_dump()}
    res = estoque.update_one({"_id": oid}, upd)
    if res.matched_count == 0:
        raise HTTPException(status_code=404, detail="Produto não encontrado.")
    doc = estoque.find_one({"_id": oid})

    # limpa cache
    r.flushdb()

    return _map_doc(doc)

# ===== Aggregation Pipelines =====

@app.get("/analytics/valor-estoque-por-categoria")
def valor_estoque_por_categoria():
    pipeline = [
        {"$match": {"categoria": {"$exists": True, "$ne": None}}},
        {
            "$group": {
                "_id": "$categoria",
                "qtd_total": {"$sum": "$quantidade"},
                "valor_total": {"$sum": {"$multiply": ["$preco", "$quantidade"]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "categoria": "$_id",
                "qtd_total": 1,
                "valor_total": {"$round": ["$valor_total", 2]},
                "preco_medio_ponderado": {
                    "$cond": [
                        {"$gt": ["$qtd_total", 0]},
                        {"$round": [{"$divide": ["$valor_total", "$qtd_total"]}, 2]},
                        None,
                    ]
                },
            }
        },
        {"$sort": {"valor_total": -1}},
    ]
    return list(estoque.aggregate(pipeline))

@app.get("/analytics/estoque-medio-por-categoria")
def estoque_medio_por_categoria():
    pipeline = [
        {"$match": {"categoria": {"$exists": True, "$ne": None}}},
        {
            "$group": {
                "_id": "$categoria",
                "media_estoque": {"$avg": "$quantidade"},
                "qtd_produtos": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "categoria": "$_id",
                "media_estoque": {"$round": ["$media_estoque", 2]},
                "qtd_produtos": 1,
            }
        },
        {"$sort": {"media_estoque": -1}},
    ]
    return list(estoque.aggregate(pipeline))