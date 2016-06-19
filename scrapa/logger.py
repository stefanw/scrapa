import asyncio
import logging
import sys

import aiohttp
from aiohttp import web

from diagnostics.models import ExceptionInfo
from diagnostics.logging import HtmlFormatter

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
        self.html_formatter = HtmlFormatter()

    def emit(self, update):
        scrapa_data = None
        if hasattr(update, 'scrapa'):
            scrapa_data = update.scrapa

        if scrapa_data is not None:
            self.emit_to_subscribers(json_dumps(scrapa_data))

        data = self.get_html_for_exception(update)
        if scrapa_data and data is not None:
            exception_data = dict(scrapa_data)
            exception_data['data'] = data
            exception_data['name'] = sys.exc_info()[0].__name__
            self.emit_to_subscribers(json_dumps(exception_data))

    def emit_to_subscribers(self, data):
        for sub in self.dashboard_subscribers:
            if not sub.closed:
                sub.send_str(data)

    def get_html_for_exception(self, record):
        exception_info = record.exc_info
        record.exc_info = False

        if isinstance(exception_info, tuple):
            exception_info = ExceptionInfo.from_values(*exception_info)
        elif exception_info:
            exception_info = ExceptionInfo.new()

        if exception_info:
            return self.html_formatter.format_exception(exception_info,
                record.getMessage())

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
    def make_server(self, logger=None, host='127.0.0.1', port='5494', loop=None):
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
