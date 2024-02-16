from pydantic import BaseModel

class Keyword(BaseModel):
    conceptId: int
    instanceOfType: int

class Query(BaseModel):
    nro: int
    query: str