import tqdm
import logging

from pymongo.errors import DuplicateKeyError

from mongodb.base_database import BaseDatabase
from models.datamodels.act_vector import ActVector


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VectorActCollection(BaseDatabase):
    def __init__(self):
        super().__init__()
        self.collection = None

    @classmethod
    async def create(cls):
        instance = cls()
        instance.collection = instance.get_collection('vector_acts')
        await instance.collection.create_index([("act_nro", 1) , ("reconstruct_id" , 1)], unique=True)
        return instance
    
    async def get_number_of_documents(self):
        return await self.collection.count_documents({})
    
    async def get_act_vector(self, act_nro: int , reconstruct_id: str):
        return await self.collection.find_one({"act_nro": act_nro , "reconstruct_id": reconstruct_id})
    
    async def add_act_vector(self, act_vector: ActVector):
        try:
            await self.collection.insert_one(act_vector.model_dump())
        except DuplicateKeyError as e:
            logger.info(f"Duplicate key error: {e}")
    
    async def delete_act_vector(self, act_nro:int):
        await self.collection.delete_one({"act_nro": act_nro})

    async def delete_collection(self):
        await self.collection.drop()

    async def add_act_vectors(self, act_vectors: list[ActVector]):
        for act_vector in tqdm.tqdm(act_vectors):
            await self.add_act_vector(act_vector)