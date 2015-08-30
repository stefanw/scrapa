class HTTPError(Exception):
    def __init__(self, message, response):
        self.message = message
        self.response = response


class HTTPConnectionError(Exception):
    pass
