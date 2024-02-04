import torch
import logging

from tqdm.asyncio import tqdm
from transformers import AutoTokenizer
from sentence_transformers import SentenceTransformer

from models.datamodels.act_vector import ActVector
from mongodb.collections.mongo_act_vector_collection import MongoVectorActCollection
from qdrantdb.collections.qdrant_act_collection import QdrantActCollection


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 100

class EmbedActs():
    def __init__(self) -> None:
        self.mongo_act_vector_collection: MongoVectorActCollection  = None
        self.qdrant_act_collection: QdrantActCollection = None
        
        self.tokenizer = AutoTokenizer.from_pretrained("gpt2")
        self.model = SentenceTransformer("sdadas/mmlw-retrieval-roberta-large")
        self.model = self.model.to("cuda" if torch.cuda.is_available() else "cpu")
    
    @classmethod
    async def create(cls) -> 'EmbedActs':
        instance = cls()
        instance.mongo_act_vector_collection = await MongoVectorActCollection.create()
        instance.qdrant_act_collection = await QdrantActCollection.create()
        return instance

    async def embed_act_vectors(self) -> None:
        total_docs = await self.mongo_act_vector_collection.get_number_of_documents()
        already_embedded = await self.qdrant_act_collection.get_act_vector_count()
        processed_acts = 0
        ids = 1
        pbar = tqdm(total=total_docs, desc="Embedding Acts")

        try:
            async for batch in self.mongo_act_vector_collection.scroll_all(batch_size=BATCH_SIZE):
                processed_acts += len(batch)

                if processed_acts >= already_embedded:
                    texts = [act_vector['text'] for act_vector in batch]
                    vectors = self.model.encode(texts, convert_to_tensor=False, show_progress_bar=False)
                    act_vectors = [ActVector(**act_vector) for act_vector in batch]
                    await self.qdrant_act_collection.upsert_batch_act_vectors(act_vectors=act_vectors, ids=[id for id in range(ids, ids+len(batch))], vectors=vectors)

                ids += len(batch)
                pbar.update(len(batch))
        finally:
            pbar.close()

        if not await self.validate_counts():
            logging.error("Counts do not match")
            logging.info(f"Mongo count: {await self.mongo_act_vector_collection.get_number_of_documents()} Qdrant count: {await self.qdrant_act_collection.get_act_vector_count()}")
        else:
            logging.info("Counts match")
    
    async def embed_act_vector(self, act_nro: int, act_reconstruct_id: int)->None:
        id = 1
        act_vector = await self.mongo_act_vector_collection.get_act_vector(act_nro , act_reconstruct_id)
        if act_vector:
            vector = self.model.encode(act_vector['text'], convert_to_tensor=False, show_progress_bar=False)
            act_vector = ActVector(**act_vector)
            await self.qdrant_act_collection.upsert_single_act_vector(act_vector, id, vector)
    
    async def validate_counts(self) -> bool:
        mongo_count = await self.mongo_act_vector_collection.get_number_of_documents()
        qdrant_count = await self.qdrant_act_collection.get_act_vector_count()
        return mongo_count == qdrant_count