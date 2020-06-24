"""API Spider: update cirrusdoc data for pages."""
# pylint: disable=no-member
from more_itertools import chunked
import jmespath as jmp
from scrapy import Request
from dzeta.schema import Schema, fields
from . import ApiSpider
from ... import _


class ApiPagesCirrus(ApiSpider):
    """API spider for updating cirrus data of pages.

     _Attributes_ section describes available user-provided arguments.
    See _Wikipedia API_ docs_ for more info.

    .. _docs: https://en.wikipedia.org/w/api.php?action=help&modules=query%2Bcirrusdoc

    Atttributes
    -----------
    model : str, optional
        Mongoengine collection class name to determine userset.
        For instance ``'wikiminer.mongo.models.WikiProjectPage'``.
        Do not pass anything to get all pages.
    ns : int, optional
        Limit results to a given namespace.
    limit : int
        Number of pages in one batch. Defaults to ``50``.
    missing_only : {'yes', 'true', 'no', 'false'}
        Should only data for pages without cirrus data be fetched.
    """
    name = 'api_pages_cirrus'

    class Args(Schema):
        model = fields.Str(required=False)
        ns = fields.Int(required=False, strict=False)
        limit = fields.Int(missing=50, strict=False, validate=[
            lambda x: 0 < x <= 50
        ])
        missing_only = \
            fields.Bool(missing=False, truthy=('yes', 'true'), falsy=('no', 'false'))

    def make_start_requests(self, **kwds):
        query = {}
        if self.args.model is not None:
            query['_cls'] = self.args.model
        if self.args.ns is not None:
            query['ns'] = self.args.ns
        if self.args.missing_only:
            query['source_text'] = {
                '$exists': False,
                '$in': [ None, [] ]
            }
        cursor = _.Page.objects(__raw__=query).only('_id').timeout(False)
        for chunk in chunked(cursor, n=self.args.limit):
            url = self.make_query(
                prop='cirrusdoc',
                pageids='|'.join(str(doc.pk) for doc in chunk),
                **kwds
            )
            yield Request(url)

    def start_requests(self):
        yield from self.make_start_requests()

    def parse(self, response):
        data = super().parse(response)
        pages = list(jmp.search('query.pages', data).values())
        for page in pages:
            if 'missing' in page:
                continue
            try:
                cirrus = page.pop('cirrusdoc')[0]
            except (KeyError, IndexError):
                continue
            source = cirrus['source']
            page.update(
                page_type=cirrus['type'],
                source_text=source['source_text'],
                template=source['template'],
                timestamp_updated=source['timestamp'],
                timestamp_created=source['create_timestamp']
            )
            yield page
