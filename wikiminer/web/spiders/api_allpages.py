"""API Spider: all pages list iterator endpoint."""
import jmespath as jmp
from scrapy import Request
from . import ApiSpider


class ApiAllPages(ApiSpider):
    """API spider for all pages list iterator endpoint."""
    name = 'api_allpages'

    _attributes_schema = {
        'apfrom': { 'type': 'string' },
        'apnamespace': { 'type': 'string', 'required': True },
        'apminsize': { 'type': 'integer', 'coerce': int },
        'aplimit': { 'type': 'integer', 'coerce': int, 'default': 500 },
        'apfilterredir': { 'type': 'string', 'default': 'nonredirects' }
    }

    def make_start_request(self, **kwds):
        attrs = self.parse_attributes(return_null=False)
        url = self.make_query(
            list='allpages',
            **attrs,
            **kwds
        )
        return Request(url)

    def start_requests(self):
        yield self.make_start_request()

    def parse(self, response):
        data = super().parse(response)
        cont = jmp.search('continue.apcontinue', data)
        if cont:
            request = self.make_start_request(apcontinue=cont)
            yield request
        allpages = jmp.search('query.allpages', data)
        for page in allpages:
            yield page
