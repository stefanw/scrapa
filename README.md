# scrapa

A scraping library on top of Python 3 `asyncio` and `aiohttp`.


## Example


```python
import asyncio
from itertools import chain

from scrapa import Scraper


class MyScraper(Scraper):
    BASE_URL = 'http://example.org/'

    # All methods that do IO need to be marked as coroutines
    @asyncio.coroutine
    def start(self):
        # Get links on a page
        page_links = yield from self.get_page_links()
        # Get more links on all these pages in parallel.
        # This returns a set of link generators (one result for each task)
        page_link_sets = yield from self.run_many(self.get_page_links, page_links)
        # Chain the generators in these sets to make one generator
        page_link_sets = chain.from_iterable(page_link_sets)
        # Schedule tasks to get all images from these pages
        # Scheduled tasks are put in a queue and do not block until done
        yield from self.schedule_many(self.get_images, page_link_sets)
        print('DONE')

    @asyncio.coroutine
    def get_page_links(self, url=''):
        # Get lxml DOM from url
        doc = yield from self.get_dom(url)
        # Return a generator of urls (but could return anything)
        return (link.attrib['href'] for link in doc.xpath('.//a'))

    @asyncio.coroutine
    def get_images(self, url):
        doc = yield from self.get_dom(url)
        for i in doc.xpath('.//img'):
            print({'alt': i.attrib['alt'], 'src': i.attrib['src']})


def main():
    scraper = MyScraper('example')
    scraper.run()

if __name__ == '__main__':
    main()

```
