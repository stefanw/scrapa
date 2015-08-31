# scrapa

A scraping library on top of Python 3 `asyncio` and `aiohttp`.


## Example


```python
from itertools import chain

from scrapa import Scraper, async
from scrapa.storage.database import DatabaseStorage


class MyScraper(Scraper):
    BASE_URL = 'http://example.org/'

    # All methods that do IO need to be marked as coroutines
    @async
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

    @async
    def get_page_links(self, url=''):
        # Get lxml DOM from url
        doc = yield from self.get_dom(url)
        # Return a generator of urls (but could return anything)
        return (link.attrib['href'] for link in doc.xpath('.//a'))

    # Store these tasks
    @async(store=True)
    def get_images(self, url):
        doc = yield from self.get_dom(url)
        for i in doc.xpath('.//img'):
            # Store result with id, type and some data
            yield from self.store_result(i.attrib['src'], 'image', i.attrib['alt'])


def main():
    path = os.path.join(os.getcwd(), 'tasks.db'),
    db_url = 'sqlite:///%s' % path
    scraper = MyScraper(storage=DatabaseStorage(db_url=db_url))
    # Run with command line arguments
    scraper.run_from_cli()

if __name__ == '__main__':
    main()

```
