from pydantic import BaseModel

from typing import List, Optional, Dict 
from .completion_schema import Role, Message

class LivenessResponseModel(BaseModel):
    status:bool
    message:str 

class CompletionRequestModel(BaseModel):
    model:str 
    messages:List[Message]
    stream:bool=False
    response_format:Optional[Dict]=None

class CompletionResponseModel(BaseModel):
    assistant_response:Message 