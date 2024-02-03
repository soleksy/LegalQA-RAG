import tqdm
import logging

from mongodb.collections.mongo_question_collection import MongoQuestionCollection
from etl.common.questionindex.transformed_question_index import TransformedQuestionIndex


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoadQuestions():
    def __init__(self, domains: list[dict] = None):
        self.collection: MongoQuestionCollection = None
        self.question_index = TransformedQuestionIndex()
        self.domains = domains

    @classmethod
    async def create(cls)->'LoadQuestions':
        instance = cls()
        instance.collection = await MongoQuestionCollection.create()
        return instance
    
    async def load_questions(self) -> None:
        await self.collection.add_questions(self.question_index._retrieve_questions(self.domains))

    async def validate_loaded_data(self) -> bool:
        index = self.question_index._get_filename_index(self.domains)
        index = self.question_index._read_json_file(self.question_index.transformed_questions_index_path+index)

        for nro in tqdm.tqdm(index):
            question = await self.collection.get_question(nro)
            if question is None:
                logger.info(f"Question {nro} not found in database.")
                return False
        return True
