import asyncio
import base64
from contextlib import contextmanager
from datetime import datetime
from urllib.parse import urljoin, urlsplit, urlencode, parse_qsl, urlunsplit
import uuid
import ssl

import aiohttp
from aiohttp.client import ClientRequest

from .exceptions import HttpConnectionError, HttpError
from .session import SessionWrapper
from .response import ScrapaClientResponse, CachedResponse
from .utils import get_cache_id


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
                if part:
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


class RequestMixin():
    async def request(self, method, url='', **kwargs):
        session_arg = kwargs.pop('session', None)
        status_only = kwargs.pop('status_only', False)
        raise_for_status = kwargs.pop('raise_for_status', True)
        response = None
        error_msg = None
        req_uuid = str(uuid.uuid4())
        for retry_num in range(self.config.MAX_RETRIES):
            with (await self.http_semaphore):
                with self.use_request_session(session_arg) as session:
                    url = self.get_full_url(url)
                    b64_data = None
                    try:
                        start_time = datetime.utcnow()
                        response = await asyncio.wait_for(
                            session.request(method, url, **kwargs),
                            self.config.CONNECT_TIMEOUT)
                        response.scrapa = self
                        if not status_only:
                            b64_data = await response.read()
                            b64_data = base64.b64encode(b64_data).decode('utf-8'),
                    except asyncio.TimeoutError as e:
                        error_msg = 'Request timed out'
                        self.timeout_count += 1
                        if self.timeout_count > self.config.MAX_TIMEOUT_COUNT:
                            self.reset_session(session)
                    except aiohttp.ClientError as e:
                        error_msg = 'Request connection error: {}'.format(e)
                    except aiohttp.ServerDisconnectedError as e:
                        error_msg = 'Server disconnected error: {}'.format(e)
                    else:
                        error_msg = None
                        self.timeout_count = 0
                        break
                    finally:
                        self.log_request({
                            'req_uuid': req_uuid,
                            'method': method,
                            'kwargs': kwargs,
                            'session_id': id(session),
                            'url': url,
                            'status': response.status if response else None,
                            'retry': retry_num,
                            'message': error_msg,
                            'timestamp': start_time,
                            'data': b64_data,
                            'duration': int((datetime.utcnow() - start_time).total_seconds() * 1000)
                        })
                        try:
                            if response is not None:
                                await response.release()
                        except (aiohttp.DisconnectedError, RuntimeError):
                            # Ignore disconnect errors on release
                            # Ignore pause_reading errors
                            pass
            if error_msg is not None:
                self.logger.warn(error_msg)
        if response is None:
            raise HttpConnectionError(error_msg)
        if raise_for_status:
            self.check_status(response, url)
        return response

    def check_status(self, response, url):
        http_error_msg = ''
        if 400 <= response.status < 500:
            http_error_msg = '%s Client Error for url: %s' % (response.status, url)

        elif 500 <= response.status < 600:
            http_error_msg = '%s Server Error for url: %s' % (response.status, url)

        if http_error_msg:
            response.close()
            raise HttpError(code=response.status, message=http_error_msg, headers=response.headers)

    def get_default_session_kwargs(self):
        return {
            'headers': self.get_default_session_headers()
        }

    def get_default_session_headers(self):
        return {'User-Agent': self.config.DEFAULT_USER_AGENT}

    def create_session(self, **kwargs):
        kwargs.setdefault('response_class', ScrapaClientResponse)
        kwargs.setdefault('request_class', ScrapaClientRequest)
        connector = self.get_connector()
        request_kwargs = self.get_default_session_kwargs()
        request_kwargs.update(kwargs)
        self.logger.debug('Creating new session with %s and %s', connector, request_kwargs)
        return SessionWrapper(self, aiohttp.ClientSession(connector=connector, **request_kwargs))

    @contextmanager
    def get_session(self, **kwargs):
        try:
            session = self.create_session(**kwargs)
            yield session
        finally:
            session.close()

    def reset_session(self, session):
        session.close()

    def get_session_from_pool(self):
        pool_size = len(self._session_pool)
        pool_index = self._session_query_count % pool_size
        session = self._session_pool[pool_index]

        if session is not None and getattr(session, '_use_count', 0) > self.config.REUSE_SESSION_COUNT:
            session.close()

        if session is None or session.closed:
            self._session_pool[pool_index] = self.create_session()
            session = self._session_pool[pool_index]
            session._use_count = 0
        return session

    @contextmanager
    def use_request_session(self, session=None):
        current_session = session or self.get_session_from_pool()
        try:
            yield current_session
        finally:
            self._session_query_count += 1
            if not hasattr(current_session, '_use_count'):
                current_session._use_count = 1
            else:
                current_session._use_count += 1

    def get_connector(self):
        custom_kwargs = {
            'verify_ssl': self.config.VERIFY_SSL
        }

        if self.config.CUSTOM_CA:
            ssl_ctx = ssl.create_default_context(cafile=self.config.CUSTOM_CA)
            custom_kwargs['ssl_context'] = ssl_ctx

        if self.config.PROXY is not None:
            conn = aiohttp.connector.ProxyConnector(
                proxy=self.config.PROXY,
                conn_timeout=self.config.CONNECT_TIMEOUT,
                limit=self.config.CONNECTOR_LIMIT,
                **custom_kwargs)
        else:
            conn = aiohttp.TCPConnector(
                conn_timeout=self.config.CONNECT_TIMEOUT,
                limit=self.config.CONNECTOR_LIMIT,
                **custom_kwargs
            )
        return conn

    async def get(self, url='', *args, **kwargs):
        cache_url = self.get_full_url(url, params=kwargs.get('params', {}))
        cache = kwargs.pop('cache', False)
        if cache:
            start_time = datetime.utcnow()
            cache_id = get_cache_id(cache_url, *args, **kwargs)
            cached_result = await self.get_cached_content(cache_id)
            if cached_result is not None:
                self.log_request({
                    'req_uuid': str(uuid.uuid4()),
                    'method': 'GET',
                    'kwargs': kwargs,
                    'session_id': None,
                    'url': cache_url,
                    'status': 200,
                    'retry': 0,
                    'message': None,
                    'timestamp': start_time,
                    'data': cached_result.decode('utf-8', 'replace'),
                    'duration': int((datetime.utcnow() - start_time).total_seconds() * 1000)
                })

                return CachedResponse(self, cache_url, cached_result)
        response = await self.request('GET', url, *args, **kwargs)
        if cache:
            await self.set_cached_content(cache_id, response.url, response)
        return response

    async def get_text(self, url='', *args, **kwargs):
        encoding = kwargs.pop('encoding', self.config.ENCODING)
        response = await self.get(url, *args, **kwargs)
        text = await response.get_text(encoding=encoding)
        return text

    async def get_json(self, *args, **kwargs):
        encoding = kwargs.pop('encoding', self.config.ENCODING)
        response = await self.get(*args, **kwargs)
        obj = await response.get_json(encoding=encoding)
        return obj

    async def get_dom(self, *args, **kwargs):
        encoding = kwargs.pop('encoding', self.config.ENCODING)
        response = await self.get(*args, **kwargs)
        dom = await response.get_dom(encoding=encoding)
        return dom

    async def post(self, *args, **kwargs):
        response = await self.request('POST', *args, **kwargs)
        return response

    def get_full_url(self, url, base_url=None, params=None):
        if base_url is None:
            base_url = self.config.BASE_URL
        if base_url and not url.startswith(('http://', 'https://')):
            return urljoin(base_url, url)
        if params is not None:
            url_parts = urlsplit(url)
            url_dict = url_parts._asdict()
            qs_list = parse_qsl(url_dict['query'])
            qs_list += list(params.items())
            qs_string = urlencode(qs_list)
            url_dict['query'] = qs_string
            # url_dict is an OrderedDict, that's why this works
            url = urlunsplit(url_dict.values())
        return url
