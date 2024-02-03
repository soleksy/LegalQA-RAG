import tqdm
import logging

from pymongo.errors import DuplicateKeyError

from mongodb.base_database import BaseDatabase
from models.datamodels.leaf_act import LeafAct



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoLeafActCollection(BaseDatabase):
    def __init__(self):
        super().__init__()
        self.collection = None

    @classmethod
    async def create(cls):
        instance = cls()
        instance.collection = instance.get_collection('leaf_acts')
        await instance.collection.create_index("nro", unique=True)
        return instance
    
    async def get_number_of_documents(self):
        return await self.collection.count_documents({})
    
    async def get_leaf_act(self, nro: int):
        return await self.collection.find_one({"nro": nro})
    
    async def add_leaf_act(self, leaf_act: LeafAct):
        try:
            await self.collection.insert_one(leaf_act.model_dump())
        except DuplicateKeyError as e:
            logger.info(f"Duplicate key error: {e}")
    
    async def delete_leaf_act(self, nro:int):
        await self.collection.delete_one({"nro": nro})

    async def add_leaf_acts(self, leaf_acts: list[LeafAct]):
        for leaf_act in tqdm.tqdm(leaf_acts):
            await self.add_leaf_act(leaf_act)