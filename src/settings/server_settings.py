from pydantic_settings import BaseSettings
from pydantic import Field

SERVER_DESCRIPTION = """
This FastAPI server acts as an <span style="color: #4169e1;">**intelligent proxy**</span> for OpenAI's chat completion API. 🔗 
It provides a seamless interface for clients while <span style="color: #32cd32;">internally managing rate limiting, token usage, and request distribution</span>. 
🛠️ Utilizing <span style="color: #ff6347;">**ZeroMQ**</span> for task queuing, <span style="color: #ff6347;">**semaphores**</span> for concurrency control, and a <span style="color: #ff6347;">**sophisticated tracking system**</span>, 
💡 it <span style="color: #ffd700;">dynamically adjusts request rates and distributes tasks across workers</span>. 
🎛️ This approach <span style="color: #9932cc;">**prevents rate limit errors and optimizes costs**</span> without requiring additional effort from the end-user, 
🌟 ensuring reliable and cost-effective access to OpenAI's powerful language models.
"""

class ServerSettings(BaseSettings):
    title:str=Field(validation_alias='TITLE', default='OPENAI-COMPLETION-SERVER')
    version:str=Field(validation_alias='VERSION', default='0.0.1')
    description:str=Field(validation_alias='DESCRIPTION', default=SERVER_DESCRIPTION)
    host:str=Field(validation_alias='HOST', default='0.0.0.0')
    port:int=Field(validation_alias='PORT', default=8000)
    workers:int=Field(validation_alias='NB_UVICORN_WORKERS', default=2)
