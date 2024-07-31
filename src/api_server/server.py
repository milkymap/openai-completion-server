import signal 

import asyncio 
import uvicorn

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from contextlib import asynccontextmanager

from typing import List, Dict, Tuple 
from typing import Any, Optional

from .mapper import Mapper
from ..settings.server_settings import ServerSettings

from src.log import logger 

from .api_schemas import LivenessResponseModel, CompletionRequestModel, CompletionResponseModel

class APIServer:
    def __init__(self, server_settings:ServerSettings, mapper:Mapper):
        self.server_settings = server_settings 
        self.app = FastAPI(
            title=self.server_settings.title,
            version=self.server_settings.version,
            description=self.server_settings.description,
            lifespan=self.lifespan
        )
        self.mapper = mapper 

    async def liveness(self):
        return LivenessResponseModel(
            status=True,
            message=f'server is running at {self.server_settings.host}:{self.server_settings.port}'
        )
    
    async def make_completion(self, incoming_req:CompletionRequestModel):
        self.mapper.track_task()
        assistant_response = await self.mapper.make_completion(incoming_req)
        return CompletionResponseModel(
            assistant_response=assistant_response
        )

    async def release_resources(self):
        logger.info('resources will be released')
        tasks = asyncio.all_tasks()
        tasks_to_cancel:List[asyncio.Task] = []
        for task in tasks:
            if task.get_name().startswith(Mapper.BACKGROUND_TASK):
                task.cancel()
        
        _  = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        loop = asyncio.get_running_loop()
        loop.remove_signal_handler(sig=signal.SIGINT)
        self.server.should_exit = True 

    @asynccontextmanager
    async def lifespan(self, app:FastAPI):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(sig=signal.SIGTERM, callback=lambda: signal.raise_signal(signal.SIGINT))
        loop.add_signal_handler(sig=signal.SIGINT, callback=lambda: asyncio.create_task(self.release_resources()))
        logger.info('server has started, all resources initialized')
        app.add_api_route(path='/liveness', endpoint=self.liveness, response_model=LivenessResponseModel, status_code=status.HTTP_200_OK, description='check service liveness')
        app.add_api_route(path='/make_completion', endpoint=self.make_completion, response_model=CompletionResponseModel, methods=['POST'], status_code=status.HTTP_200_OK, description='openai chat completion')
        yield 
        loop.remove_signal_handler(sig=signal.SIGINT)
        loop.remove_signal_handler(sig=signal.SIGTERM)
        logger.info('server shutdown, all resources released')
        
    async def run(self):
        self.config = uvicorn.Config(
            app=self.app, 
            host=self.server_settings.host, 
            port=self.server_settings.port,
            workers=self.server_settings.workers
        )
        self.server = uvicorn.Server(config=self.config)
        await self.server.serve()
    



