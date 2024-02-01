from typing import List, Optional, Union
from pydantic import BaseModel, validator

class Unit(BaseModel):
    nro: int
    id: str
    name: str
    stateName: str

    @validator('name')
    def replace_non_breaking_spaces(cls, v):
        return v.replace('\xa0', ' ')

class RelationData(BaseModel):
    units: Optional[List[Unit]] = None

class ActRelation(BaseModel):
    title: str
    nro: int
    lawType: str
    validity: str
    relationData: Union[RelationData, dict]

    @validator('relationData', pre=True)
    def empty_dict_to_relation_data(cls, v):
        if v == {}:
            return v 
        else:
            return RelationData(**v)

class Keyword(BaseModel):
    conceptId: int
    instanceOfType: int
    actRelations: List[ActRelation]