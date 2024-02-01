import json

from motor.motor_asyncio import AsyncIOMotorCollection

from mongodb.mongo_singleton import MongoSingleton


class BaseDatabase:
    def __init__(self):
        with open('mongodb/config.json', 'r') as config_file:
            config = json.load(config_file)

            self.config = config['db_config']
            self.client = MongoSingleton().client
            self.db = self.client[self.config['db_name']]

    async def get_database_names(self):
        return await self.client.list_database_names()

    async def list_collection_names(self):
        return await self.db.list_collection_names()
    
    def get_collection(self, collection: str) -> AsyncIOMotorCollection:
        return self.db[collection]