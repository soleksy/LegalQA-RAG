from typing import List, Optional
from pydantic import BaseModel

class RelatedKeyword(BaseModel):
    label: str
    conceptId: int
    instanceOfType: int

class CitationData(BaseModel):
    nro: int
    id: str
    name: str

class RelatedAct(BaseModel):
    nro: int
    title: str
    validity: str
    relationData: Optional[List[CitationData]] = None

class Question(BaseModel):
    nro: int
    title: str
    relatedActs: List[RelatedAct]
    relatedKeywords: List[RelatedKeyword]