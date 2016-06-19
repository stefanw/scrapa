import asyncio
import base64
import functools
import webbrowser

from lxml import html

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.remote.webelement import WebElement

from .response import html_parser
from .utils import show_in_browser


def get_selenium_driver(driver_name):
    if driver_name == 'chrome':
        from selenium.webdriver.chrome.webdriver import WebDriver as ChromeDriver
        return ChromeDriver()
    elif driver_name == 'phantomjs':
        from selenium.webdriver import PhantomJS
        return PhantomJS()
    elif driver_name == 'firefox':
        from selenium.webdriver.firefox.webdriver import WebDriver as FirefoxDriver
        return FirefoxDriver()
    raise ValueError('Unkown driver name')


class SeleniumContext(object):
    def __init__(self, **kwargs):
        self.driver_name = kwargs.get('driver_name', 'phantomjs')

    def __enter__(self):
        self.driver = get_selenium_driver(self.driver_name)
        self.driver.implicitly_wait(3)
        return SeleniumWrapper(self.driver)

    def __exit__(self, exc_type, exc_value, traceback):
        self.driver.quit()


class SeleniumResponse():
    def __init__(self, content):
        self.content = content


class SeleniumElementWrapper(object):
    def __init__(self, element, driver):
        self.el = element
        self.driver = driver

    def __str__(self):
        return "<SeleniumElementWrapper of '%s'>" % self.el

    def __getattr__(self, name):
        return getattr(self.el, name)

    def make_async(self, name, *args, **kwargs):
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(None, functools.partial(
            getattr(self.el, name),
            *args, **kwargs)
        )
        yield from future

    def click(self):
        return functools.partial(self.make_async, 'click')

    def js_click(self):
        self.driver.execute_script("var evt = document.createEvent('MouseEvents');" +
            "evt.initMouseEvent('click',true, true, window, 0, 0, 0, 0, 0, false, false, false, false, 0,null);" +
            "arguments[0].dispatchEvent(evt);", self.el)


def conditional_element_wrapper(func, driver):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        res = func(*args, **kwargs)
        if isinstance(res, WebElement):
            res = SeleniumElementWrapper(res, driver)
        elif isinstance(res, list):
            res = [SeleniumElementWrapper(r, driver) if isinstance(r, WebElement) else r
                    for r in res]
        return res

    return wrapper


class SeleniumWrapper(object):
    def __init__(self, driver):
        self.driver = driver

    def __getattr__(self, name):
        return conditional_element_wrapper(getattr(self.driver, name), self.driver)

    @asyncio.coroutine
    def async_driver(self, driver_func_name, *args, **kwargs):
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(None, functools.partial(
                getattr(self.driver, driver_func_name), *args, **kwargs))
        yield from future

    @asyncio.coroutine
    def get(self, *args, **kwargs):
        yield from self.async_driver('get', *args, **kwargs)
        return SeleniumResponse(self.driver.page_source)

    @asyncio.coroutine
    def wait(self, condition_func, wait=5):
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(None, lambda: WebDriverWait(self.driver,
                wait).until(condition_func))
        yield from future

    def show_in_browser(self):
        show_in_browser(self.text())

    def text(self):
        return self.driver.page_source

    def dom(self):
        return html.fromstring(self.text().encode('utf-8'), parser=html_parser)


class SeleniumMixin(object):
    def selenium(self, **kwargs):
        return SeleniumContext(**kwargs)
