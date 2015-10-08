import os

from .storage import DatabaseStorage


def get_default_storage(obj):
    path = os.path.join(os.getcwd(), 'scrapa.db'),
    db_url = 'sqlite:///%s' % path
    return DatabaseStorage(db_url=db_url)


class DefaultValue(object):
    def get(self, obj):
        raise NotImplementedError


class CallableDefaultValue(DefaultValue):
    def __init__(self, func):
        self.func = func

    def get(self, obj):
        return self.func(obj)


class ScrapaConfig:
    BASE_URL = ''
    PROGRESS_INTERVAL = 5
    CONNECTOR_LIMIT = 10
    HTTP_CONCURENCY_LIMIT = 10
    CONNECT_TIMEOUT = 30
    MAX_RETRIES = 3
    ENABLE_WEBSERVER = True
    ENABLE_QUEUE = True
    MAX_TIMEOUT_COUNT = 3
    TASK_RETRY_COUNT = 3
    QUEUE_SIZE = 0
    CONSUMER_COUNT = 10
    REUSE_SESSION = True
    REUSE_SESSION_COUNT = 1000
    STORAGE_ENABLED = True
    DEFAULT_USER_AGENT = 'Scrapa'
    LOGLEVEL = 'INFO'
    PROXY = None
    ENCODING = 'utf-8'
    STORAGE = CallableDefaultValue(get_default_storage)
    NAME = CallableDefaultValue(lambda x: x.__class__.__name__)
