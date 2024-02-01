import tqdm
import logging

from models.datamodels.keyword import Keyword
from mongodb.base_database import BaseDatabase
from mongodb.collections.keyword_collection import KeywordCollection
from etl.common.keywordindex.transformed_keyword_index import TransformedKeywordIndex

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoadKeywords(BaseDatabase):
    def __init__(self,domains: list[dict] = None):
        super().__init__()
        self.collection:KeywordCollection = None
        self.keyword_index = TransformedKeywordIndex()
        self.domains = domains
    

    @classmethod
    async def create(cls, domains: list[dict] = None) -> 'LoadKeywords':
        instance = cls(domains)
        instance.collection = await KeywordCollection.create()
        return instance
    
    async def load_keywords(self) -> None:
        await self.collection.add_keywords(keywords = self.keyword_index._retrieve_keywords())

    async def validate_loaded_data(self) -> bool:
        index = self.keyword_index._get_filename_index()
        index = self.keyword_index._read_json_file(self.keyword_index.transformed_keyword_index_path+index)

        for keyword in tqdm.tqdm(index):
            keyword = await self.collection.get_keyword(keyword['conceptId'], keyword['instanceOfType'])
            if keyword is None:
                logger.info(f"Keyword {keyword} not found in database.")
                return False
        return True