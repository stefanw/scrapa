import asyncio
import os

from .storage import DatabaseStorage
from .logger import make_logger


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
    QUEUE_SIZE = 0
    CONSUMER_COUNT = 10
    SESSION_POOL_SIZE = 10
    REUSE_SESSION = True
    REUSE_SESSION_COUNT = 1000
    CONNECT_TIMEOUT = 30
    MAX_RETRIES = 3
    CUSTOM_CA = None
    VERIFY_SSL = True
    ENABLE_WEBSERVER = True
    ENABLE_QUEUE = True
    MAX_TIMEOUT_COUNT = 3
    TASK_RETRY_COUNT = 3
    STORAGE_ENABLED = True
    DEFAULT_USER_AGENT = 'Scrapa'
    LOGLEVEL = 'INFO'
    PROXY = None
    DEBUG_EXCEPTIONS = False
    ENCODING = 'utf-8'
    STORAGE = CallableDefaultValue(get_default_storage)
    NAME = CallableDefaultValue(lambda x: x.__class__.__name__)


class ConfigurationMixin():
    def init_configuration(self, config):
        proper_config = {}
        for key in (k for k in dir(ScrapaConfig) if not k.startswith('_')):
            val = config.get(key.lower(),
                             self.config_kwargs.get(key.lower(),
                             getattr(self, key, getattr(ScrapaConfig, key)))
            )
            if isinstance(val, DefaultValue):
                val = val.get(self)
            proper_config[key] = val

        self.config = type('CustomScrapaConfig', (ScrapaConfig,), proper_config)

        self.stopping = False
        self.consumer_count = 0
        self.tasks_running = 0
        self.timeout_count = 0
        self.storage = None
        self.logger = make_logger(self.config.NAME, level=self.config.LOGLEVEL)

        self.queue = asyncio.Queue(self.config.QUEUE_SIZE)
        self.http_semaphore = asyncio.Semaphore(self.config.HTTP_CONCURENCY_LIMIT)
        self._session_pool = [None for _ in range(self.config.SESSION_POOL_SIZE)]
        self._session_query_count = 0
