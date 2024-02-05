from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from qdrant_client import models
from qdrant_client.models import Record 

from models.datamodels.question import Question
from qdrantdb.qdrant_base_database import QdrantBaseDatabase

MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 2

class QdrantQuestionCollection(QdrantBaseDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.collection_name = 'questions'

    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type(Exception)))
    async def upsert_question(self, question: Question, vector) -> None:
        
        question = question.model_dump()
        point = models.PointStruct(
            id = question['nro'],
            payload = question,
            vector = vector
        )

        await self.client.upsert(collection_name=self.collection_name, points = [point])

    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type(Exception)))
    async def upsert_batch_questions(self, questions: list[Question] , vectors) -> None:
        points = []
        for i, question in enumerate(questions):
            question = question.model_dump()

            point = models.PointStruct(
                id = question['nro'],
                payload = question,
                vector = vectors[i]
            )
            points.append(point)
        await self.client.upsert(collection_name=self.collection_name, points = points)


    async def search_questions(self, limit: int, vector: list[float]) -> list[Record]:
        response = await self.client.search(collection_name=self.collection_name, query_vector=vector, limit=limit, with_payload=True)
        return response
    
    async def retrieve_question(self, question_nro: int) -> Record:
        response = await self.client.retrieve(collection_name=self.collection_name, ids = [question_nro])
        return response

    async def retrieve_batch_questions(self, question_nros: list[int]) -> list[Record]:
        response = await self.client.retrieve(collection_name=self.collection_name, ids = question_nros)
        return response
    
    async def delete_question(self, question_nro: int) -> None:
        await self.client.delete(collection_name=self.collection_name, points_selector = models.PointIdsList(points=[question_nro]))

    async def delete_batch_questions(self, question_nros: list[int]) -> None:
        await self.client.delete(collection_name=self.collection_name, points_selector = models.PointIdsList(points=question_nros))
    
    async def get_question_count(self) -> int:
        response = await self.client.count(collection_name=self.collection_name)
        return response.count
    
    async def delete_collection(self) -> None:
        await self.client.delete_collection(collection_name=self.collection_name)