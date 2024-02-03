import tqdm
import logging

from pymongo.errors import DuplicateKeyError

from mongodb.base_database import BaseDatabase
from models.datamodels.keyword import Keyword

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoKeywordCollection(BaseDatabase):

    def __init__(self, domains: list[dict] = None):
        super().__init__()
        self.collection = None
        self.domains = domains
    
    @classmethod
    async def create(cls, domains: list[dict] = None):
        instance = cls(domains)
        instance.collection = instance.get_collection('keywords')
        await instance.collection.create_index([('conceptId', 1) , ('instanceOfType' , 1)], unique=True)
        return instance

    async def get_number_of_documents(self):
        return await self.collection.count_documents({})

    async def get_keyword(self, conceptId: int, instanceOfType: int):
        return await self.collection.find_one({"conceptId": conceptId, "instanceOfType": instanceOfType})
    
    async def add_keyword(self, keyword: Keyword):
        try:
            await self.collection.insert_one(keyword.model_dump())
        except DuplicateKeyError as e:
            logger.info(f"Duplicate key error: {e}")
    
    async def delete_keyword(self, conceptId: int, instanceOfType: int):
        await self.collection.delete_one({"conceptId": conceptId, "instanceOfType": instanceOfType})

    async def add_keywords(self, keywords: list[Keyword]):
        for keyword in tqdm.tqdm(keywords):
            await self.add_keyword(keyword)