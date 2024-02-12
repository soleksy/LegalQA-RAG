import os
import json
import torch
import dotenv
import uvicorn
import asyncio
import logging
import tiktoken

from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi import FastAPI, Depends, HTTPException, status, Request

from typing import Dict, List
from typing_extensions import TypedDict
from contextlib import asynccontextmanager
from sentence_transformers import SentenceTransformer

from mongodb.collections.mongo_leaf_act_collection import MongoLeafActCollection
from qdrantdb.collections.qdrant_question_collection import QdrantQuestionCollection
from qdrantdb.collections.qdrant_act_collection import QdrantActCollection

from models.datamodels.question import Question
from models.datamodels.act_vector import ActVector
from models.datamodels.leaf_act import LeafAct

encoding = tiktoken.get_encoding('cl100k_base')
get_openai_tokens = lambda text: len(encoding.encode(text))


dotenv.load_dotenv()

STATIC_API_KEY = os.getenv("STATIC_API_KEY")

class Query(TypedDict):
    nro: str
    queries: List[str]

async def get_qdrant_act_collection():
    act_collection = await QdrantActCollection.create()
    return act_collection

async def get_qdrant_question_collection():
    question_collection = await QdrantQuestionCollection.create()
    return question_collection

async def get_mongo_leaf_act_collection():
    act_collection = await MongoLeafActCollection.create()
    return act_collection

async def get_model():
    model = SentenceTransformer("sdadas/mmlw-retrieval-roberta-large")
    model = model.to("cuda" if torch.cuda.is_available() else "cpu")
    return model

functions = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    functions['model'] = await get_model()
    functions['qdrant_question_collection']   = await get_qdrant_question_collection()
    functions['qdrant_act_collection']  = await get_qdrant_act_collection()
    functions['mongo_leaf_act_collection']  = await get_mongo_leaf_act_collection()

    yield

    functions.clear()

app = FastAPI(lifespan=lifespan)
templates=Jinja2Templates(directory="templates")

def validate_api_key(request: Request):
    # Extract API key from the headers
    api_key = request.headers.get("X-API-Key")
    if api_key == STATIC_API_KEY:
        return True
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key"
        )

@app.get("/acts/search")
async def get_recommended_acts(query: str , valid: bool = Depends(validate_api_key)) -> List[Dict]:
    if valid:
        vector = functions['model'].encode('zapytanie: ' + query, convert_to_tensor=False, show_progress_bar=False)
        questions = await functions["qdrant_question_collection"].search_questions(limit=20, vector=vector)

        acts = set()
        acts_to_return = []

        for question in questions:
            question = Question(**question.payload)

            for related_act in question.relatedActs:
                if related_act.nro not in acts:
                    acts.add(related_act.nro)
                    if len(acts_to_return) < 5:
                        acts_to_return.append({
                            "nro": related_act.nro,
                            "title": related_act.title
                        })
                    else:
                        break

        return acts_to_return

    else:
        return {"error": "Invalid API key"}
    

@app.post("/acts/retrieve")
async def retrieve_act_parts(queries: Dict[str,List[Query]], valid: bool = Depends(validate_api_key)) -> List[Dict]:

    if valid:
        queries = queries['queries']
        nros_in_order = []
        queries_in_order = []
        encode_in_order = []

        for query in queries:
            nros_in_order.append(int(query['nro']))
            for q in query['queries']:
                encode_in_order.append('zapytanie: ' + q)
                queries_in_order.append(q)

        # Get vectors for each query
        vectors = functions['model'].encode(encode_in_order, convert_to_tensor=False, show_progress_bar=False) 
        
        search_tasks = []
        for vector, nro in zip(vectors, nros_in_order):
            search_tasks.append(functions["qdrant_act_collection"].search_acts_filtered(limit=5, act_nros=[nro], vector=vector))

        # Get act parts for each query
        act_parts = await asyncio.gather(*search_tasks)

        # Retrieve the leaf acts for each 
        leaf_acts = await functions['mongo_leaf_act_collection'].get_leaf_acts(nros=nros_in_order)

        # Map nros to leaf acts
        leaf_act_map : Dict[int, LeafAct] = {}
        for leaf_act in leaf_acts:
            leaf_act = LeafAct(**leaf_act)
            leaf_act_map[leaf_act.nro] = leaf_act

        to_return = []
        for nro, act_parts, query in zip(nros_in_order, act_parts, queries_in_order):
            curr_elements = set()
            
            for act in act_parts:
                act = ActVector(**act.payload)
                if act.parent_id not in curr_elements:
                    curr_elements.add(act.parent_id)
                if act.reconstruct_id not in curr_elements:
                    curr_elements.add(act.reconstruct_id)
            
            act_elements = leaf_act_map[nro].reconstruct
            elements_to_return = []
            
            for element in act_elements:
                if element in curr_elements:
                    act_elements[element].cite_id = leaf_act_map[nro].citeLink + act_elements[element].cite_id
                    elements_to_return.append(act_elements[element].model_dump())
            
            to_return.append({
                "nro": nro,
                "title": leaf_act_map[nro].title,
                "query": query,
                "data": elements_to_return
            })

        return to_return

    else:
        return {"error": "Invalid API key"}

@app.get("/search")
async def get_legal_information(query: str, valid: bool = Depends(validate_api_key)):
    if valid:
        acts = set()
        
        vector = functions['model'].encode('zapytanie: '+query, convert_to_tensor=False, show_progress_bar=False)
        questions = await functions["qdrant_question_collection"].search_questions(limit=5, vector=vector)

        for question in questions:
            question = Question(**question.payload)
            for related_act in question.relatedActs:
                acts.add(related_act.nro)
        
        act_parts = await functions['qdrant_act_collection'].search_acts_filtered(limit=60, act_nros=list(acts), vector=vector)
        leaf_acts = await functions['mongo_leaf_act_collection'].get_leaf_acts(nros=list(acts))

        
        id_set = set()
        to_return = []
        leaf_act_map : Dict[int, LeafAct] = {}

        for leaf_act in leaf_acts:
            leaf_act = LeafAct(**leaf_act)
            leaf_act_map[leaf_act.nro] = leaf_act

        found_nros = set()    
        for act in act_parts:
            act = ActVector(**act.payload)
            found_nros.add(act.act_nro)
            if (act.act_nro, act.parent_id) not in id_set:
                id_set.add((act.act_nro, act.parent_id))
            if (act.act_nro ,act.reconstruct_id) not in id_set:
                id_set.add((act.act_nro,act.reconstruct_id))
                

        for nro in list(found_nros):
            curr_elements = set()
            for id in id_set:
                if id[0] == nro:
                    curr_elements.add(id[1])
            
            act_elements = leaf_act_map[nro].reconstruct
            elements_to_return = []
            for element in act_elements:
                if element in curr_elements:
                    act_elements[element].cite_id = leaf_act_map[nro].citeLink + act_elements[element].cite_id
                    elements_to_return.append(act_elements[element].model_dump())

            curr_act = {
                "nro": nro,
                "title": leaf_act_map[nro].title,                
                "data": elements_to_return
            }

            to_return.append(curr_act)

        json_str = json.dumps(to_return, ensure_ascii=False)
        token_count = get_openai_tokens(json_str)
    
        logging.info(f"Total tokens: {token_count}")
        return to_return
    else:
        return {"error": "Invalid API Key"}
    
@app.get("/privacy", response_class=HTMLResponse)
async def privacy_policy(request: Request):
    return templates.TemplateResponse("privacy_policy.html", {"request": request})

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)