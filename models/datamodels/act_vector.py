from typing import Optional, List
from pydantic import BaseModel

class Keyword(BaseModel):
    label: str
    conceptId: int
    instanceOfType: int

class ActVector(BaseModel):
    act_nro: int
    parent_id: str
    reconstruct_id: str
    text: str
    parent_tokens: int
    text_tokens: int
    chunk_id: Optional[int] = None
    total_chunks: Optional[int] = None
    keywords: List[Keyword] = []