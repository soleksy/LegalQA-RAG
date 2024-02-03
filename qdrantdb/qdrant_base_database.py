import json

from qdrant_client import AsyncQdrantClient , models

from qdrantdb.qdrant_singleton import QdrantSingleton

class QdrantBaseDatabase:

    with open("qdrantdb/config.json") as f:
        config = json.load(f)
        f.close()

    def __init__(self):
        self.client:AsyncQdrantClient = QdrantSingleton().client
        self.collection_config = self.config["collections"]

    
    @classmethod
    async def create(cls) -> 'QdrantBaseDatabase':
        instance = cls()
        await instance.initialize_collections()
        return instance

    async def list_collections(self):
        response = await self.client.get_collections()
        return response

    async def initialize_collections(self) -> None:
        collections = await self.list_collections()
        collections_names = [collection.name for collection in collections.collections]

        for collection in self.collection_config:
            if self.collection_config[collection]['name'] not in collections_names:
                await self.client.create_collection(collection_name=self.collection_config[collection]['name'], 
                                                    vectors_config=models.VectorParams(size=self.collection_config[collection]['vector_size'],
                                                    distance=models.Distance.COSINE)
                                                    )
