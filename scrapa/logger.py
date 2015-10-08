import asyncio
import logging

import aiohttp
from aiohttp import web

from .utils import json_dumps

LEVEL_DICT = {
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'WARN': logging.WARN,
    'ERROR': logging.ERROR,
}


def make_logger(name, level='DEBUG'):
    """ Create two log handlers, one to output info-level ouput to the
    console, the other to store all logging in a JSON file which will
    later be used to generate reports. """

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(LEVEL_DICT[level])
    fmt = '%(name)s [%(levelname)-8s]: %(message)s'
    formatter = logging.Formatter(fmt)
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    return logger


@asyncio.coroutine
def add_websocket_handler(logger, **kwargs):
    handler = WebsocketHandler(logger)
    handler.setLevel(LEVEL_DICT['DEBUG'])
    yield from handler.make_server(logger=logger, **kwargs)
    logger.addHandler(handler)
    return handler


class WebsocketHandler(logging.Handler):
    def __init__(self, logger, *args, **kwargs):
        self.logger = logger
        super(WebsocketHandler, self).__init__(*args, **kwargs)
        self.web_server = None
        self.dashboard_subscribers = []

    def emit(self, update):
        if hasattr(update, 'scrapa'):
            for sub in self.dashboard_subscribers:
                if not sub.closed:
                    sub.send_str(json_dumps(update.scrapa))

    @asyncio.coroutine
    def websocket_handler(self, request):
        ws = web.WebSocketResponse()
        ws.start(request)

        self.dashboard_subscribers.append(ws)

        while not ws.closed:
            msg = yield from ws.receive()
            if msg.tp == aiohttp.MsgType.text:
                if msg.data == 'close':
                    yield from ws.close()
            elif msg.tp == aiohttp.MsgType.close:
                self.logger.debug('websocket connection closed')
            elif msg.tp == aiohttp.MsgType.error:
                self.logger.debug('ws connection closed with exception %s',
                      ws.exception())

        self.dashboard_subscribers.remove(ws)
        return ws

    @asyncio.coroutine
    def make_server(self, logger=None, host='127.0.0.1', port='8080', loop=None):
        from .dashboard import index, static

        if loop is None:
            loop = asyncio.get_event_loop()

        self.web_app = web.Application()
        self.web_app.router.add_route('GET', '/', index)
        self.web_app.router.add_route('GET', '/ws', self.websocket_handler)
        self.web_app.router.add_route('GET', r'/static/{path:.+}', static)
        self.web_handler = self.web_app.make_handler()
        self.web_server = yield from loop.create_server(
            self.web_handler, host, port)
        logger.info('Serving on http://%s:%s' % self.web_server.sockets[0].getsockname())

    @asyncio.coroutine
    def close_server(self):
        yield from self.web_handler.finish_connections(1.0)
        self.web_server.close()
        yield from self.web_server.wait_closed()
        yield from self.web_app.finish()
