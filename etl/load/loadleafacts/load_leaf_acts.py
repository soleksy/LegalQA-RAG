import tqdm
import logging

from models.datamodels.leaf_act import LeafAct
from mongodb.base_database import BaseDatabase
from mongodb.collections.leaf_act_collection import LeafActCollection
from etl.common.actindex.leaf_node_act_index import LeafNodeActIndex


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoadLeafActs(BaseDatabase):
    def __init__(self):
        super().__init__()
        self.collection: LeafActCollection = None
        self.leaf_act_index = LeafNodeActIndex()

    @classmethod
    async def create(cls) -> 'LoadLeafActs':
        instance = cls()
        instance.collection = await LeafActCollection.create()
        return instance

    async def load_leaf_acts(self) -> None:
        await self.collection.add_leaf_acts(leaf_acts=self.leaf_act_index._retrieve_leaf_acts())

    async def validate_loaded_data(self) -> bool:
        index = self.leaf_act_index.leaf_node_acts_data_path

        for leaf_act in tqdm.tqdm(index):
            if leaf_act.endswith('.json'):
                nro = int(leaf_act.split('_')[0])
                leaf_act = await self.collection.get_leaf_act(nro)
                if leaf_act is None:
                    logger.info(f"Leaf act {leaf_act} not found in database.")
                    return False
        return True



    
