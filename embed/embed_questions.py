import torch
import logging

from tqdm.asyncio import tqdm
from transformers import AutoTokenizer
from sentence_transformers import SentenceTransformer

from models.datamodels.question import Question
from mongodb.collections.mongo_question_collection import MongoQuestionCollection
from qdrantdb.collections.qdrant_question_collection import QdrantQuestionCollection


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_SIZE = 100

class EmbedQuestions():
    def __init__(self) -> None:
        self.mongo_question_collection: MongoQuestionCollection  = None
        self.question_qdrant_collection: QdrantQuestionCollection = None
        
        self.tokenizer = AutoTokenizer.from_pretrained("gpt2")
        self.model = SentenceTransformer("sdadas/mmlw-retrieval-roberta-large")
        self.model = self.model.to("cuda" if torch.cuda.is_available() else "cpu")

    @classmethod
    async def create(cls) -> 'EmbedQuestions':
        instance = cls()
        instance.mongo_question_collection = await MongoQuestionCollection.create()
        instance.question_qdrant_collection = await QdrantQuestionCollection.create()
        return instance
    
    async def embed_questions(self) -> None:
        total_docs = await self.mongo_question_collection._get_number_of_documents()
        already_embedded = await self.question_qdrant_collection.get_question_count()
        processed_questions = 0

        pbar = tqdm(total=total_docs, desc="Embedding Questions")

        try:
            async for batch in self.mongo_question_collection.scroll_all(batch_size=BATCH_SIZE):
                processed_questions += len(batch)

                if processed_questions >= already_embedded:
                    titles = [question['title'] for question in batch]
                    vectors = self.model.encode(titles, convert_to_tensor=False, show_progress_bar=False)
                    
                    questions = [Question(**question) for question in batch]
                    await self.question_qdrant_collection.upsert_batch_questions(questions, vectors)

                pbar.update(len(batch))
        finally:
            pbar.close()

        if not await self.validate_counts():
            logging.error("Counts do not match")
            logging.info(f"Mongo count: {await self.mongo_question_collection._get_number_of_documents()} Qdrant count: {await self.question_qdrant_collection.get_question_count()}")
        else:
            logging.info("Counts match")


    
    async def embed_question(self, question_nro: int) -> None:
        question = await self.mongo_question_collection.get_question(question_nro)
        vector = self.model.encode(question['title'], convert_to_tensor=False, show_progress_bar=False)
        await self.question_qdrant_collection.upsert_question(Question(**question), vector)

    async def validate_counts(self) -> bool:
        mongo_count = await self.mongo_question_collection._get_number_of_documents()
        qdrant_count = await self.question_qdrant_collection.get_question_count()
        return mongo_count == qdrant_count