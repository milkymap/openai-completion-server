from pydantic import BaseModel
from enum import Enum 

class Role(str, Enum):
    SYSTEM:str='system'
    USER:str='user'
    ASSISTANT:str='assistant'

class Message(BaseModel):
    role:Role 
    content:str 