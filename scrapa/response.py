from json import loads as json_loads

from lxml import html
from lxml import etree

from aiohttp.client import ClientResponse
from aiohttp import hdrs, helpers

from .utils import show_in_browser


class VerboseElement(html.HtmlElement):
    def __str__(self):
        return '<%s: %s>' % (self.tag, etree.tostring(self))


parser_lookup = etree.ElementDefaultClassLookup(element=VerboseElement)
html_parser = etree.HTMLParser()
html_parser.set_element_class_lookup(parser_lookup)


class ScrapaClientResponse(ClientResponse):
    def get_mimetype(self):
        ctype = self.headers.get(hdrs.CONTENT_TYPE, '').lower()
        return helpers.parse_mimetype(ctype)

    async def get_text(self, *args, **kwargs):
        # FIXME: Change Priorities here
        encoding = kwargs.pop('encoding', self.scrapa.config.ENCODING)
        try:
            try_encoding = self._get_encoding()
            text = await super(ScrapaClientResponse, self).text()
        except UnicodeDecodeError as e:
            try:
                text = await super(ScrapaClientResponse, self).text(encoding=encoding)
            except UnicodeDecodeError as e:
                self.scrapa.logger.error('Could not decode %s with [%s]', self, [try_encoding, encoding])
                raise e
        return text

    async def get_json(self, *args, **kwargs):
        text = await self.get_text(*args, **kwargs)
        return self._get_json(text, **kwargs)

    def json(self, encoding=None, **kwargs):
        text = self.text(encoding)
        return self._get_json(text, **kwargs)

    def _get_json(self, text, loads=json_loads):
        return loads(text)

    async def get_dom(self, *args, **kwargs):
        text = await self.get_text(*args, **kwargs)
        return self._get_dom(text)

    def _get_dom(self, text):
        return html.fromstring(text.encode('utf-8'), parser=html_parser)

    def text(self, encoding=None, errors='ignore'):
        if self._content is None:
            raise Exception('Response not read, need to use await get_* instead!')

        if encoding is None:
            encoding = self._get_encoding()

        return self._content.decode(encoding, errors)

    def dom(self, encoding=None, errors='strict'):
        return self._get_dom(self.text(encoding=encoding, errors=errors))

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
