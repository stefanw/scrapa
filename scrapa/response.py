import asyncio
from json import loads as json_loads

from lxml import html
from lxml import etree

from aiohttp.client import ClientResponse, ClientRequest
from aiohttp import hdrs, helpers

from .utils import show_in_browser


class VerboseElement(html.HtmlElement):
    def __str__(self):
        return '<%s: %s>' % (self.tag, etree.tostring(self))


parser_lookup = etree.ElementDefaultClassLookup(element=VerboseElement)
html_parser = etree.HTMLParser()
html_parser.set_element_class_lookup(parser_lookup)


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
    def get_text(self, *args, **kwargs):
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
    def get_json(self, *args, **kwargs):
        text = yield from self.get_text(*args, **kwargs)
        return self._get_json(text, **kwargs)

    def json(self, encoding=None, **kwargs):
        text = self.text(encoding)
        return self._get_json(text, **kwargs)

    def _get_json(self, text, loads=json_loads):
        return loads(text)

    @asyncio.coroutine
    def get_dom(self, *args, **kwargs):
        text = yield from self.get_text(*args, **kwargs)
        return self._get_dom(text)

    def _get_dom(self, text):
        return html.fromstring(text.encode('utf-8'), parser=html_parser)

    def text(self, encoding=None):
        if self._content is None:
            raise Exception('Response not read, need to use yield from get_* instead!')

        if encoding is None:
            encoding = self._get_encoding()

        return self._content.decode(encoding)

    def dom(self, encoding=None):
        return self._get_dom(self.text(encoding=encoding))

    def xpath(self, xpath):
        return self.dom().xpath(xpath)

    def cssselect(self, selector):
        return self.dom().cssselect(selector)

    def show_in_browser(self):
        show_in_browser(self.text())


class CachedResponse(ScrapaClientResponse):
    def __init__(self, scrapa, url, content):
        self.scrapa = scrapa
        self.url = url
        self._content = content
        self.status = 200

    def _get_encoding(self):
        return 'utf-8'
