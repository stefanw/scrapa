import asyncio
import os

from aiohttp import web


def get_path():
    return os.path.dirname(os.path.realpath(__file__))


@asyncio.coroutine
def index(request):

    f = open(os.path.join(get_path(), 'web', 'index.html'), 'rb')
    return web.Response(body=f.read())


@asyncio.coroutine
def static(request):
    path = request.match_info['path']
    full_path = os.path.join(get_path(), 'web', 'static', path)
    if os.path.exists(full_path):
        f = open(full_path, 'rb')
        return web.Response(body=f.read())
    return web.Response(status=404)
