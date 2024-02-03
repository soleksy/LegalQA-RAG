import json
import qdrant_client

class QdrantSingleton:
    _instance = None
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QdrantSingleton, cls).__new__(cls)
            cls._instance._initialize_client()
        return cls._instance

    def _initialize_client(self):
        with open("qdrantdb/config.json") as f:
            config = json.load(f)
        
        host = config["qdrant_db"]["host"]
        port = config["qdrant_db"]["port"]

        if self._client is None:
            self._client = qdrant_client.AsyncQdrantClient(host, port=port)
    
    @property
    def client(self):
        return self._client
