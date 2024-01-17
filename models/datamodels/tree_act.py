from pydantic import BaseModel
from typing import Dict, List, Optional

class RelatedKeyword(BaseModel):
    label: str
    conceptId: int
    instanceOfType: int

class Element(BaseModel):
    children: List[str]
    parent: Optional[str]
    text: str
    keywords: List[RelatedKeyword]

class TreeAct(BaseModel):
    nro: int
    id: str
    actLawType: Optional[str] = None
    title: Optional[str] = None
    shortQuote: Optional[str] = None
    elements: Dict[str, Element]
    keywords: List[RelatedKeyword]