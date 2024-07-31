import click 

import asyncio 

from dotenv import load_dotenv

from src.settings.openai_settings import OpenaiSettings
from src.settings.server_settings import ServerSettings

from src.api_server.mapper import Mapper
from src.api_server.server import APIServer

@click.group(chain=False, invoke_without_command=True)
@click.pass_context
def group_handler(ctx:click.core.Context):
    ctx.ensure_object(dict)
    ctx.obj['openai_settings'] = OpenaiSettings()
    ctx.obj['server_settings'] = ServerSettings() 

@group_handler.command()
@click.option('--max_concurrent_reqs', type=int, required=True, help='maximum number of concurrent requests')
@click.option('--rpm', type=int, required=True, help='maxium requests per minute')
@click.option('--tpm', type=int, required=True, help='maxium tokens per minute')
@click.option('--period_length', type=int, default=60, help='rate limit period length => default 1mn')
@click.option('--max_nb_sockets', type=int, required=True, help='maxium number of sockets', default=1024)
@click.pass_context
def launch_engine(ctx: click.core.Context, max_concurrent_reqs: int, rpm: int, tpm: int, period_length:int, max_nb_sockets:int):
    openai_settings:OpenaiSettings = ctx.obj['openai_settings']
    server_settings:ServerSettings = ctx.obj['server_settings']
    async def launch_server():
        async with Mapper(openai_settings=openai_settings, max_concurrent_reqs=max_concurrent_reqs, rpm=rpm, tpm=tpm, period_length=period_length, max_nb_sockets=max_nb_sockets) as mapper:
            api_server = APIServer(server_settings=server_settings, mapper=mapper)
            await api_server.run()
        
    try:
        asyncio.run(main=launch_server())
    except KeyboardInterrupt:
        pass 


if __name__ == '__main__':
    load_dotenv()
    group_handler()

