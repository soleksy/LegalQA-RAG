from pydantic import BaseModel
from typing import Dict, List, Optional

class Element(BaseModel):
    children: List[str]
    parent: Optional[str]
    text: str
    keywords: List[str]

class Document(BaseModel):
    nro: int
    document_type: str
    title: str
    short_quote: str
    elements: Dict[str, Element]