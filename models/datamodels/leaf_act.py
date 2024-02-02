from pydantic import BaseModel
from typing import Dict

class ArticleDetail(BaseModel):
    cite_id: str
    text: str

class LeafAct(BaseModel):
    nro: int
    title: str
    actLawType: str
    citeLink: str
    reconstruct: Dict[str, ArticleDetail]