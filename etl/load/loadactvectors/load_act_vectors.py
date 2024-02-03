import os
import tqdm
import logging

from mongodb.base_database import BaseDatabase
from mongodb.collections.mongo_act_vector_collection import MongoVectorActCollection
from etl.common.actindex.leaf_node_act_index import LeafNodeActIndex

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoadActVectors(BaseDatabase):
    def __init__(self):
        super().__init__()
        self.collection: MongoVectorActCollection = None
        self.leaf_act_index = LeafNodeActIndex()
        
    @classmethod
    async def create(cls) -> 'LoadActVectors':
        instance = cls()
        instance.collection = await MongoVectorActCollection.create()
        return instance
    
    async def load_act_vectors(self) -> None:
        await self.collection.add_act_vectors(act_vectors = self.leaf_act_index._retrieve_act_vectors())

    async def validate_loaded_data(self) -> bool:
        index = self.leaf_act_index.leaf_node_acts_data_path

        for leaf_act in tqdm.tqdm(os.listdir(index)):
            if leaf_act.endswith('.json'):
                data = self.leaf_act_index._read_json_file(index+leaf_act)
                elements = data['elements']
                nro = data['nro']

                for element in elements:
                    for vector in data['elements'][element]:
                        act_vector = await self.collection.get_act_vector(nro, vector['reconstruct_id'])
                        if act_vector is None:
                            logger.info(f"Act vector {act_vector} not found in database.")
                            return False
        return True