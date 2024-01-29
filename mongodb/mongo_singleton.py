from motor.motor_asyncio import AsyncIOMotorClient
import json

class MongoSingleton:
    with open("mongodb/config.json") as f:
        config = json.load(f)
        f.close()
    uri = config["mongo_db"]["uri"]
    _instance = None
    _client = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MongoSingleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, uri=uri):
        if not self._client:
            self._client = AsyncIOMotorClient(uri)
    @property
    def client(self):
        return self._client