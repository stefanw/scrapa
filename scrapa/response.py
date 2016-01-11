import asyncio
import json

from lxml import html
from aiohttp.client import ClientResponse, ClientRequest
from aiohttp import hdrs, helpers


class ScrapaClientRequest(ClientRequest):
    def send(self, *args, **kwargs):
        response = super(ScrapaClientRequest, self).send(*args, **kwargs)
        response.request = self
        return response

    def to_curl(self):
        headers = ' -H '.join("'{}: {}'".format(k.title(), v) for k, v in self.headers.items())
        curl = "curl '{url}' -X {method} -H {headers}".format(
            url=self.url, method=self.method, headers=headers,
        )
        if self.body:
            for part in self.body:
                curl += " --data '{body}'".format(body=part.decode('utf-8'))
        return curl

    def __str__(self):
        return '''
        URL: {url}
        Method: {method}
        Headers: {headers}
        Body: {body}
        '''.format(url=self.url, method=self.method, headers=self.headers,
                   body=self.body)


class ScrapaClientResponse(ClientResponse):
    def get_mimetype(self):
        ctype = self.headers.get(hdrs.CONTENT_TYPE, '').lower()
        return helpers.parse_mimetype(ctype)

    @asyncio.coroutine
    def text(self, *args, **kwargs):
        encoding = kwargs.pop('encoding', self.scrapa.config.ENCODING)
        try:
            try_encoding = self._get_encoding()
            text = yield from super(ScrapaClientResponse, self).text()
        except UnicodeDecodeError as e:
            try:
                text = yield from super(ScrapaClientResponse, self).text(encoding=encoding)
            except UnicodeDecodeError as e:
                self.scrapa.logger.error('Could not decode %s with [%s]', self, [try_encoding, encoding])
                raise e
        return text

    @asyncio.coroutine
    def json(self, *args, **kwargs):
        text = yield from self.text(*args, **kwargs)
        return json.loads(text)

    @asyncio.coroutine
    def dom(self, *args, **kwargs):
        text = yield from self.text(*args, **kwargs)
        return html.fromstring(text.encode('utf-8'))


class CachedResponse(ScrapaClientResponse):
    def __init__(self, url, content):
        self.url = url
        self._content = content
        self.status = 200
