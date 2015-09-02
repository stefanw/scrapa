from aiohttp import HttpProcessingError, DisconnectedError, ClientError


class HttpError(HttpProcessingError):
    pass


class HttpConnectionError(DisconnectedError, ClientError):
    pass
