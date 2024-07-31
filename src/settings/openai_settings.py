from pydantic_settings import BaseSettings
from pydantic import Field

class OpenaiSettings(BaseSettings):
    api_key:str=Field(validation_alias='OPENAI_API_KEY')
    