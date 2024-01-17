from pydantic import BaseModel , validator
from typing import Dict, List, Optional

class RelatedKeyword(BaseModel):
    label: str
    conceptId: int
    instanceOfType: int

class Element(BaseModel):
    children: List[str]
    parent: Optional[str]
    text: str
    keywords: Optional[List[RelatedKeyword]] = []

    @validator('keywords', pre=True, always=True)
    def validate_keywords(cls, v):
        return [] if v is None else v

class TreeAct(BaseModel):
    nro: int
    id: str
    actLawType: Optional[str] = None
    title: Optional[str] = None
    shortQuote: Optional[str] = None
    elements: Dict[str, Element]
    keywords: List[RelatedKeyword]