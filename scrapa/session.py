import asyncio


class SessionWrapper():
    def __init__(self, scraper, session):
        self.scraper = scraper
        self.session = session

    def __getattr__(self, name):
        return getattr(self.session, name)

    @asyncio.coroutine
    def _run(self, name, *args, **kwargs):
        kwargs['session'] = self.session
        return (yield from getattr(self.scraper, name)(*args, **kwargs))

    @asyncio.coroutine
    def get(self, *args, **kwargs):
        return (yield from self._run('get', *args, **kwargs))

    @asyncio.coroutine
    def post(self, *args, **kwargs):
        return (yield from self._run('post', *args, **kwargs))

    @asyncio.coroutine
    def request(self, *args, **kwargs):
        return (yield from self._run('request', *args, **kwargs))
