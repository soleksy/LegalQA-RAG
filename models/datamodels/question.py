from typing import List, Optional
from pydantic import BaseModel , validator

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
    relationData: Optional[List[CitationData]] = []

class Question(BaseModel):
    nro: int
    title: str
    relatedActs: Optional[List[RelatedAct]] = []
    keywords: Optional[List[RelatedKeyword]] = []

    @validator('relatedActs', 'keywords', pre=True, always=True)
    def set_none_to_empty_list(cls, v):
        return [] if v is None else v