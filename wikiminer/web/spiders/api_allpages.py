"""API Spider: all pages list iterator endpoint."""
# pylint: disable=no-member
import jmespath as jmp
from scrapy import Request
from dzeta.schema import Schema, fields
from . import ApiSpider


class ApiAllPages(ApiSpider):
    """API spider for all pages list iterator endpoint.

    _Attributes_ section describes available user-provided arguments.
     See _Wikipedia API_ docs_ for more info.

    .. _docs: https://en.wikipedia.org/w/api.php?action=help&modules=query%2Ballpages

    Attributes
    ----------
    apfrom : str
        Prefix to start enumerating pages from.
    apnamespace : int
        Namespace to enumarate from. Defaults to ``0`` (main).
    apminsize : int
        Minimum byte size of a page.
    apmaxsize : int
        Maximum byte size of a page.
    aplimit : int
        Pagination size for querying API. Defaults to ``500``.
    apfilterredir : str
        Flag for filtering redirects.
        Defaults to ``'nonredirects'``.
    """
    name = 'api_allpages'

    class Args(Schema):
        apfrom = fields.Str(required=False)
        apnamespace = fields.Int(missing=0, strict=False)
        apminsize = fields.Int(required=False, strict=False)
        apmaxsize = fields.Int(required=False, strict=False)
        aplimit = fields.Int(missing=500, strict=False)
        apfilterredir = fields.Str(missing='nonredirects')

    def make_start_request(self, **kwds):
        url = self.make_query(
            list='allpages',
            **self.args,
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
