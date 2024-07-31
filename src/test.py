import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion, ChatCompletionMessage
from src.settings.openai_settings import OpenaiSettings
from .api_server.api_schemas import CompletionRequestModel
from .api_server.completion_schema import Role, Message

# Assuming your Mapper class is in a file named mapper.py
from .api_server.mapper import Mapper

@pytest.fixture
def openai_settings():
    return OpenaiSettings(api_key="test_api_key")

@pytest.fixture
def mapper(openai_settings):
    return Mapper(
        openai_settings=openai_settings,
        max_concurrent_reqs=5,
        rpm=60,
        tpm=40000,
        period_length=60,
        max_nb_sockets=1024
    )

@pytest.mark.asyncio
async def test_make_completion(mapper):
    # Mock the client method
    mapper.client = AsyncMock(return_value=(Message(role=Role.ASSISTANT, content="Test response"), None))
    
    request = CompletionRequestModel(
        model="gpt-3.5-turbo",
        messages=[Message(role=Role.USER, content="Test message")]
    )
    
    result = await mapper.make_completion(request)
    
    assert isinstance(result, Message)
    assert result.role == Role.ASSISTANT
    assert result.content == "Test response"

@pytest.mark.asyncio
async def test_make_completion_semaphore_timeout(mapper):
    # Set a very low delay to force a timeout
    mapper.semaphore = AsyncMock()
    mapper.semaphore.acquire.side_effect = asyncio.TimeoutError()
    
    request = CompletionRequestModel(
        model="gpt-3.5-turbo",
        messages=[Message(role=Role.USER, content="Test message")]
    )
    
    with pytest.raises(HTTPException) as exc_info:
        await mapper.make_completion(request, delay=0.1)
    
    assert exc_info.value.status_code == 500
    assert "can not acquire the internal semaphore" in str(exc_info.value.detail)

@pytest.mark.asyncio
async def test_completion_success(mapper):
    # Mock the AsyncOpenAI client
    mock_completion = ChatCompletion(
        id="test_id",
        choices=[
            MagicMock(message=ChatCompletionMessage(content="Test response", role="assistant"))
        ],
        usage=MagicMock(total_tokens=10)
    )
    mapper.openai_client.chat.completions.create = AsyncMock(return_value=mock_completion)
    
    result, tokens, error = await mapper._completion(
        messages=[Message(role=Role.USER, content="Test message")],
        model="gpt-3.5-turbo"
    )
    
    assert isinstance(result, Message)
    assert result.role == Role.ASSISTANT
    assert result.content == "Test response"
    assert tokens == 10
    assert error is None

@pytest.mark.asyncio
async def test_completion_rate_limit_error(mapper):
    # Mock the AsyncOpenAI client to raise a rate limit error
    mapper.openai_client.chat.completions.create = AsyncMock(side_effect=ValueError('rate limit'))
    
    result, tokens, error = await mapper._completion(
        messages=[Message(role=Role.USER, content="Test message")],
        model="gpt-3.5-turbo"
    )
    
    assert result is None
    assert tokens == 0
    assert error == 'rate-limit-error'

@pytest.mark.asyncio
async def test_router(mapper):
    # This is a complex method to test fully, so we'll just test that it runs without error
    # and responds to a cancellation signal
    mapper.event = asyncio.Event()
    mapper.event.set()  # Simulate the event being set
    
    # Mock the socket operations
    mock_socket = AsyncMock()
    mock_socket.recv_multipart.return_value = [b'worker_id', b'', b'FREE', b'client_id', b'{}']
    mock_socket.poll.return_value = {mock_socket: 1}
    
    with patch('zmq.asyncio.Socket', return_value=mock_socket):
        router_task = asyncio.create_task(mapper.router())
        await asyncio.sleep(0.1)  # Let the router run for a short time
        router_task.cancel()
        
        with pytest.raises(asyncio.CancelledError):
            await router_task

@pytest.mark.asyncio
async def test_worker(mapper):
    # Similar to the router test, we'll just ensure it runs and responds to cancellation
    mapper.event = asyncio.Event()
    mapper.event.set()  # Simulate the event being set
    
    mock_socket = AsyncMock()
    mock_socket.recv_multipart.return_value = [b'', b'client_id', b'{"messages": []}']
    mapper._completion = AsyncMock(return_value=(Message(role=Role.ASSISTANT, content="Test"), 10, None))
    
    with patch('zmq.asyncio.Socket', return_value=mock_socket):
        worker_task = asyncio.create_task(mapper.worker())
        await asyncio.sleep(0.1)  # Let the worker run for a short time
        worker_task.cancel()
        
        with pytest.raises(asyncio.CancelledError):
            await worker_task

@pytest.mark.asyncio
async def test_context_manager(mapper):
    async with mapper as m:
        assert isinstance(m.openai_client, AsyncOpenAI)
        assert len(m.background_tasks) == m.max_concurrent_reqs + 1  # workers + router
    
    # Check that all tasks were cancelled
    for task in mapper.background_tasks:
        assert task.cancelled()

def test_compute_nb_tokens(mapper):
    messages = [
        Message(role=Role.USER, content="This is a test message"),
        Message(role=Role.ASSISTANT, content="This is a response")
    ]
    
    tokens = mapper.compute_nb_tokens(messages)
    assert tokens > 0  # Basic check that it returns a positive number

# Integration test
@pytest.mark.asyncio
async def test_end_to_end(mapper):
    request = CompletionRequestModel(
        model="gpt-3.5-turbo",
        messages=[Message(role=Role.USER, content="What is the capital of France?")]
    )
    
    # Mock the OpenAI client to return a predetermined response
    mock_completion = ChatCompletion(
        id="test_id",
        choices=[
            MagicMock(message=ChatCompletionMessage(content="The capital of France is Paris.", role="assistant"))
        ],
        usage=MagicMock(total_tokens=10)
    )
    mapper.openai_client.chat.completions.create = AsyncMock(return_value=mock_completion)
    
    async with mapper:
        result = await mapper.make_completion(request)
    
    assert isinstance(result, Message)
    assert result.role == Role.ASSISTANT
    assert "Paris" in result.content

if __name__ == "__main__":
    pytest.main()