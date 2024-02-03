import tqdm
import logging

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo.errors import DuplicateKeyError

from mongodb.base_database import BaseDatabase
from models.datamodels.question import Question

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoQuestionCollection(BaseDatabase):
    def __init__(self):
        super().__init__()
        self.collection = None
    
    @classmethod
    async def create(cls):
        instance = cls()
        instance.collection: AsyncIOMotorCollection = instance.get_collection('questions')
        await instance.collection.create_index("nro", unique=True)
        return instance
    
    async def _get_number_of_documents(self):
        return await self.collection.count_documents({})
    
    async def _add_question(self, question: Question):
        try:
            await self.collection.insert_one(question.model_dump())
        except DuplicateKeyError as e:
            logger.info(f"Duplicate key error: {e}")
    
    async def add_questions(self, questions: list[Question]):
        for question in tqdm.tqdm(questions):
            await self._add_question(question)
    
    async def delete_question(self, nro:int):
        await self.collection.delete_one({"nro": nro})
    
    async def get_question(self, nro:int):
        return await self.collection.find_one({"nro": nro})
    
    async def scroll_all(self , batch_size: int = 100):
        total_docs = await self._get_number_of_documents()
        skip = 0
        
        while skip < total_docs:
            cursor = self.collection.find().skip(skip).limit(batch_size)

            batch = await cursor.to_list(length=batch_size)
            
            yield batch
            skip += batch_size