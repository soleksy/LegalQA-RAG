from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from qdrant_client import models
from qdrant_client.models import Record 

from models.datamodels.act_vector import ActVector
from models.datamodels.keyword import Keyword
from qdrantdb.qdrant_base_database import QdrantBaseDatabase


MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 2

class QdrantActCollection(QdrantBaseDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.collection_name = 'acts'

    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type(Exception)))
    async def upsert_single_act_vector(self, act_vector: ActVector, _id: int, vector) -> None:

        act_vector = act_vector.model_dump()
        point = models.PointStruct(
            id = _id,
            payload = act_vector,
            vector = vector
        )
        await self.client.upsert(collection_name=self.collection_name, points = [point])

    @retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_fixed(RETRY_WAIT_SECONDS),
    retry=(retry_if_exception_type(Exception)))
    async def upsert_batch_act_vectors(self, act_vectors: list[ActVector], ids: list[int], vectors) -> None:
        points = []
        for i, act_vector in enumerate(act_vectors):
            act_vector = act_vector.model_dump()

            point = models.PointStruct(
                id = ids[i],
                payload = act_vector,
                vector = vectors[i]
            )
            points.append(point)
        await self.client.upsert(collection_name=self.collection_name, points = points)

    async def search_acts(self, limit:int , vector: list[float]) -> list[Record]:
        response = await self.client.search(collection_name=self.collection_name, query_vector=vector, limit=limit, with_payload=True)
        return response
    
    async def search_acts_keyword_filtered(self, limit: int, act_nros: list[int] , keywords :list[Keyword] , vector: list[float])-> list[Record]:
        concept_id_set = list(set([keyword.conceptId for keyword in keywords]))
        instance_of_type_set = list(set([keyword.instanceOfType for keyword in keywords]))

        return await self.client.search(
        collection_name=self.collection_name,
        query_vector=vector,
        limit=limit,
        with_payload=True,
        search_params=models.SearchParams(exact=False),
        query_filter=models.Filter(must=[
            models.NestedCondition(nested=models.Nested(key="keywords", filter=models.Filter(
                must=[
                models.FieldCondition(key="conceptId", match=models.MatchAny(any=concept_id_set)),
                models.FieldCondition(key="instanceOfType", match=models.MatchAny(any=instance_of_type_set))
                ]))),
            models.FieldCondition(key="act_nro",match=models.MatchAny(any=act_nros))
            ])
        )

    async def search_acts_filtered(self, limit: int, act_nros: list[int], vector: list[float]) -> list[Record]:
        return await self.client.search(
        collection_name=self.collection_name,
        query_vector=vector,
        limit=limit,
        with_payload=True,
        search_params=models.SearchParams(exact=False),
        query_filter=models.Filter(must=[models.FieldCondition(key="act_nro",match=models.MatchAny(any=act_nros))])
    )
    
    async def retrieve_act_vector(self, act_vector_id: int) -> Record:
        response = await self.client.retrieve(collection_name=self.collection_name, ids = [act_vector_id])
        return response

    async def retrieve_batch_act_vectors(self, act_vector_ids: list[int]) -> list[Record]:
        response = await self.client.retrieve(collection_name=self.collection_name, ids = act_vector_ids)
        return response
    
    async def delete_act_vector(self, act_vector_id: int) -> None:
        await self.client.delete(collection_name=self.collection_name, points_selector = models.PointIdsList(points=[act_vector_id]))
    
    async def delete_batch_questions(self, act_vector_ids: list[int]) -> None:
        await self.client.delete(collection_name=self.collection_name, points_selector = models.PointIdsList(points=act_vector_ids))

    async def get_act_vector_count(self) -> int:
        response = await self.client.count(collection_name=self.collection_name)
        return response.count
    
    async def delete_collection(self) -> None:
        await self.client.delete_collection(collection_name=self.collection_name)