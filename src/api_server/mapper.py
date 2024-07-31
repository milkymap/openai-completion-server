import zmq 
import zmq.asyncio

import json 
import pickle 

import asyncio
from uuid import uuid4

from asyncio import Lock, Event, Semaphore
from operator import attrgetter, itemgetter

from hashlib import sha256

from async_timeout import timeout

from src.log import logger 
from fastapi import HTTPException
from contextlib import asynccontextmanager
from src.settings.openai_settings import OpenaiSettings
from typing import List, Tuple, Dict, Union , AsyncGenerator, Optional, Any, AsyncIterable 

import openai
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionChunk, ChatCompletion

from .api_schemas import CompletionRequestModel
from .completion_schema import Role, Message

from time import time 

from tenacity import retry, stop_after_attempt, retry_if_exception_type, wait_exponential

class Mapper:
    """
    A class to manage and route API requests to OpenAI, handling rate limiting and concurrency.
    """
    BACKGROUND_TASK:str='BACKGROUND-TASK-'

    def __init__(self, openai_settings:OpenaiSettings, max_concurrent_reqs:int, rpm:int, tpm:int, period_length:int=60, max_nb_sockets:int=1024, max_nb_retries:int=2):
        """
        Initialize the Mapper with given settings.

        Args:
            openai_settings (OpenaiSettings): Settings for OpenAI API.
            max_concurrent_reqs (int): Maximum number of concurrent requests.
            rpm (int): Maximum requests per minute.
            tpm (int): Maximum tokens per minute.
            period_length (int, optional): Length of the rate limiting period in seconds. Defaults to 60.
            max_nb_sockets (int, optional): Maximum number of sockets. Defaults to 1024.
            max_nb_retries (int, optional): Maximum number of retries for rate-limited requests. Defaults to 2.
        """
         
        self.openai_settings = openai_settings
        self.max_concurrent_reqs = max_concurrent_reqs  # maximum number of concurrent request
        self.rpm = rpm  # maximum number of requests per minute
        self.tpm = tpm  # maximum number of tokens per minute
        self.period_length = period_length # period_length
        self.max_nb_sockets = max_nb_sockets

        self.max_nb_retries = max_nb_retries

        self.nb_reqs = 0
        self.nb_tokens = 0
        self.background_tasks:List[asyncio.Task] = []
        self.workers_queue:List[bytes] = []
                
        self.rate_limit_reqs_cache:Dict[str, int] = {}
        self.rate_limit_retry_queue:List[Tuple[bytes, bytes]] = []
        self.rate_limit_signal:bool = False
        self.estimated_nb_tokens:int = 0  # track the input nb tokens : simple heuristic
        self.time_marker = time()
    
    def reset_states(self):
        self.nb_reqs = 0
        self.nb_tokens = 0
        self.time_marker = time()
        self.estimated_nb_tokens = 0
        self.rate_limit_signal = False 
        logger.info('router reset the nb_tokens and nb_reqs ofr a new period')

    async def make_completion(self, incoming_req:CompletionRequestModel, delay:int=10) -> Optional[Message]:
        semaphore_acquired = False 
        try:
            async with timeout(delay=delay):
                semaphore_acquired = await self.semaphore.acquire()
        except asyncio.TimeoutError:
            pass 
        except asyncio.CancelledError:
            pass 

        if not semaphore_acquired:
            raise HTTPException(
                status_code=500,
                detail=f'can not acquire the internal semaphore..timeout delay => {delay}'
            )
        
        outgoing_res, error = await self.client(incoming_req=incoming_req)
        self.semaphore.release()

        if outgoing_res is None:
            raise HTTPException(
                status_code=500,
                detail=f'failed to perform the task | reason => {str(error)}'
            )
        return outgoing_res

    @retry(
        reraise=True,
        stop=stop_after_attempt(2),
        retry=(retry_if_exception_type(openai.APIConnectionError) | retry_if_exception_type(openai.InternalServerError) | retry_if_exception_type(openai.APITimeoutError) | retry_if_exception_type(openai.ConflictError)),
        wait=wait_exponential(multiplier=1, min=3, max=9)
    )
    async def _completion(self, messages:List[Message], model:str, stream:bool=False, response_format:Optional[Dict]=None) -> Tuple[Message, int]:
        payload = {
            'messages':messages,
            'model':model,
            'stream': stream and False,
            'response_format':response_format
        }
        completion_fun = self.openai_client.chat.completions.create
        completion_res:ChatCompletion = await completion_fun(**payload)
        total_tokens = completion_res.usage.total_tokens
        return Message(role=Role.ASSISTANT, content=completion_res.choices[0].message.content), total_tokens 
        
    async def _wrap_completion(self, encoded_client_message_data:bytes) -> Tuple[Optional[Message], int, Optional[str]]:
        completion_res, nb_tokens, error = None, 0, None 
        plain_client_message_data = json.loads(encoded_client_message_data.decode())
        nb_tokens = self.compute_nb_tokens(plain_client_message_data['messages'])
            
        try:
            completion_res, nb_tokens = await self._completion(**plain_client_message_data)
        except Exception as e:
            error = e
            logger.error(e)
        return completion_res, nb_tokens, error
        
    @asynccontextmanager
    async def create_socket(self, socket_type:int, method:str, addr:str) -> AsyncGenerator[zmq.asyncio.Socket, None]:
        socket:zmq.asyncio.Socket = self.ctx.socket(socket_type)
        socket_connected:bool=False
        try:
            attrgetter(method)(socket)(addr=addr)  # connect | bind 
            socket_connected = True
            yield socket 
        except Exception as e:
            logger.error(e)
        finally:
            if socket_connected: socket.close(linger=0)
    
    def compute_nb_tokens(self, messages:List[Message]) -> int:
        try:
            return sum([ int(len(msg['content']) / 3.5) for msg in messages ])
        except Exception as e:
            logger.warning(f"compute token estimation error => {str(e)}")
            return 0 
        
    async def client(self, incoming_req:CompletionRequestModel) -> Tuple[Optional[Message], Optional[str]]:
        outgoing_res:Optional[Message] = None 
        error = None 
        async with self.create_socket(socket_type=zmq.DEALER, method='connect', addr='inproc://outer_openai_queue') as socket:
            await socket.send_multipart([b''], flags=zmq.SNDMORE)
            await socket.send_json(incoming_req.model_dump())
            while True:
                try:
                    incoming_signal = await socket.poll(timeout=100)
                    if incoming_signal != zmq.POLLIN:
                        continue
                    _, encoded_worker_message_data = await socket.recv_multipart()
                    plain_worker_message_data = json.loads(encoded_worker_message_data)
                    if plain_worker_message_data['data'] is not None:
                        outgoing_res = Message(**plain_worker_message_data['data'])
                    error = plain_worker_message_data['error']
                    break 
                except asyncio.CancelledError:
                    break 
                except Exception as e:
                    logger.error(e)
                    break 
    
        return outgoing_res, error 
    
    async def _handle_worker_data(self, source_worker_id:bytes, message_type:bytes, target_client_id:bytes, encoded_worker_message_data:bytes, outer_socket:zmq.asyncio.Socket):
        assert message_type in [b'FREE', b'DATA', b'RATE_LIMIT'], "message_type is not a valid event"

        # one worker is available for future tasks
        if message_type == b'FREE':   
            self.workers_queue.append(source_worker_id)
            return 
        
        # one worker push its final response for a specific task
        if message_type == b'DATA':  
            plain_worker_message_data = json.loads(encoded_worker_message_data)
            self.nb_tokens = self.nb_tokens + plain_worker_message_data['nb_tokens']
            await outer_socket.send_multipart([target_client_id, b'', encoded_worker_message_data])
            return 
                
        # handle rate_limit from a worker 
        fingerprint = sha256(encoded_worker_message_data).hexdigest()
        counter = self.rate_limit_reqs_cache.get(fingerprint, 0)
        self.rate_limit_reqs_cache[fingerprint] = counter
        if self.rate_limit_reqs_cache[fingerprint] >= self.max_nb_retries:
            await outer_socket.send_multipart([target_client_id, b''], flags=zmq.SNDMORE)
            await outer_socket.send_json({
                'data': None,
                'nb_tokens': 0,  # use nb input tokens
                'error': 'maxiumum number of retries reached...! can not make new attempts: RATE-LIMIT'
            })
            del self.rate_limit_reqs_cache[fingerprint]
        else:
            self.rate_limit_retry_queue.append((target_client_id, encoded_worker_message_data))  # cache the request
            self.rate_limit_signal = True 
            self.time_marker = time()  # reset the timer, shift the period 
            logger.info('push to rate limit retry queue')
    
    async def _handle_client_data(self, outer_socket:zmq.asyncio.Socket, inner_socket:zmq.asyncio.Socket):
        if self.nb_reqs == 0: self.time_marker = time()  # slide the period                      
        target_worker_id = self.workers_queue.pop(0)  
        self.nb_reqs = self.nb_reqs + 1

        if len(self.rate_limit_retry_queue) == 0:
            source_client_id, _, encoded_client_message_data = await outer_socket.recv_multipart()
        else:  # pop one task from the retry_queue
            source_client_id, encoded_client_message_data = self.rate_limit_retry_queue.pop(0)
            fingerprint = sha256(encoded_client_message_data).hexdigest()
            self.rate_limit_reqs_cache[fingerprint] += 1 # retry = retry + 1 
        
        self.estimated_nb_tokens += self.compute_nb_tokens(json.loads(encoded_client_message_data.decode())['messages'])
        await inner_socket.send_multipart([target_worker_id, b'', source_client_id, encoded_client_message_data])

    def is_rate_limited(self) -> bool:
        return self.nb_reqs >= self.rpm or self.nb_tokens >= 0.8 * self.tpm or self.estimated_nb_tokens >= 0.8 * self.tpm or self.rate_limit_signal

    async def router(self):
        async with self.create_socket(socket_type=zmq.ROUTER, method='bind', addr='inproc://outer_openai_queue') as outer_socket:
            async with self.create_socket(socket_type=zmq.ROUTER, method='bind', addr='inproc://inner_openai_queue') as inner_socket:    
                poller = zmq.asyncio.Poller()
                poller.register(outer_socket, zmq.POLLIN)
                poller.register(inner_socket, zmq.POLLIN)
                self.event.set()  # send signal to workers 
                logger.info('api mapper router initialized')
                while True:
                    try:
                        socket_states_hmap:Dict[zmq.asyncio.Socket, int] = dict(await poller.poll(timeout=100))
                        duration = time() - self.time_marker
                        if duration >= self.period_length: self.reset_states() 
                        logger.info(f'api mapper router is running nb_reqs: {self.nb_reqs} nb_toks:{self.nb_tokens} nb_workers : {len(self.workers_queue):03d} estimated tokens : {self.estimated_nb_tokens} | perdiod : {int(duration)} seconds')

                        if socket_states_hmap.get(inner_socket) == zmq.POLLIN:
                            source_worker_id, _, message_type, target_client_id, encoded_worker_message_data = await inner_socket.recv_multipart()
                            await self._handle_worker_data(source_worker_id, message_type, target_client_id, encoded_worker_message_data, outer_socket)

                        if self.is_rate_limited():
                            await asyncio.sleep(0.1) # 100ms 
                            continue
                                
                        if len(self.workers_queue) == 0:
                            continue  # no worker available for processing a task 

                        if socket_states_hmap.get(outer_socket) != zmq.POLLIN and len(self.rate_limit_retry_queue) == 0:
                            continue

                        await self._handle_client_data()
                    except asyncio.CancelledError:
                        break 
                    except Exception as e:
                        logger.error(e)
                        break 

    async def worker(self):
        async with self.create_socket(socket_type=zmq.DEALER, method='connect', addr='inproc://inner_openai_queue') as socket:
            await self.event.wait()  # wait signal from router 
            await socket.send_multipart([b'', b'FREE', b'', b''])  # delimeter, message_type, target_client, completion_res
            while True:
                try:
                    incoming_signal = await socket.poll(timeout=10)  #10ms 
                    if incoming_signal != zmq.POLLIN:
                        continue
                    _, source_client_id, encoded_client_message_data = await socket.recv_multipart()
                    completion_res, nb_tokens, error = await self._wrap_completion(encoded_client_message_data)
                    
                    if isinstance(error, openai.RateLimitError) or isinstance(error, ValueError):
                        await socket.send_multipart([b'', b'RATE_LIMIT', source_client_id, encoded_client_message_data])
                        await socket.send_multipart([b'', b'FREE', b'', b''])  # worker is available for next task        
                        continue

                    encoded_completion_res = completion_res.model_dump() if completion_res is not None else None 
                    await socket.send_multipart([b'', b'DATA', source_client_id], flags=zmq.SNDMORE)
                    await socket.send_json({'data': encoded_completion_res, 'nb_tokens': nb_tokens, 'error': error})
                    await socket.send_multipart([b'', b'FREE', b'', b''])  # worker is available for next task        
                except asyncio.CancelledError:
                    break 
                except Exception as e:
                    logger.error(e)
                    break 

    async def __aenter__(self):
        self.ctx = zmq.asyncio.Context()
        self.ctx.set(zmq.MAX_SOCKETS, self.max_nb_sockets)
        self.lock = Lock()
        self.event = Event()
        self.semaphore = Semaphore(value=self.max_nb_sockets // 2)
        self.openai_client = AsyncOpenAI(api_key=self.openai_settings.api_key, timeout=30, max_retries=0)
        for _ in range(self.max_concurrent_reqs):
            self.background_tasks.append(asyncio.create_task(self.worker()))
        
        self.background_tasks.append(asyncio.create_task(self.router()))
        logger.info(f'api mapper was loaded => nb task {len(self.background_tasks)}')
        return self 
    
    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            logger.warning(exc_value)
            logger.exception(traceback)
        for task in self.background_tasks:
            task.cancel()
        _ = await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.ctx.term()
        logger.info('api mapper shutdown')
    
    def track_task(self) -> str:
        task = asyncio.current_task()
        task_id = str(uuid4())
        task.set_name(f'{self.BACKGROUND_TASK}{task_id}')
        return task_id