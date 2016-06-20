class SessionWrapper():
    def __init__(self, scraper, session):
        self.scraper = scraper
        self.session = session

    def __getattr__(self, name):
        return getattr(self.session, name)

    async def _run(self, name, *args, **kwargs):
        kwargs['session'] = self.session
        return await getattr(self.scraper, name)(*args, **kwargs)

    async def get(self, *args, **kwargs):
        return await self._run('get', *args, **kwargs)

    async def post(self, *args, **kwargs):
        return await self._run('post', *args, **kwargs)

    async def request(self, *args, **kwargs):
        return await self._run('request', *args, **kwargs)
