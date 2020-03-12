"""API Spider: page assessments extractor."""
# pylint: disable=no-member
from urllib.parse import urlparse, unquote
from more_itertools import chunked
import jmespath as jmp
from scrapy import Request
from dzeta.schema import Schema, fields
from . import ApiSpider
from ... import _


class ApiPageAssessments(ApiSpider):
    """API spider for extracting page assessment data.

    _Attributes_ section describes available user-provided arguments.
    See _Wikipedia API_ docs_ for more info.

    .. _docs: https://en.wikipedia.org/w/api.php?action=help&modules=query%2Bpageassessments

    Attributes
    ----------
    model : str, optional
        Mongoengine collection model to get page records from.
    ns : int, optional
        Namespace to use. Should be left to the default value of ``0``.
    palimit : int
        Number of pages in one batch. Defaults to ``500``.
    pasubprojects : bool
        If truthy then subprojects data is also collected.
    """
    name = 'api_page_assessments'
    # Spider setings
    custom_settings = {
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 6.0
    }


    class Args(Schema):
        model = fields.Str(required=False)
        ns = fields.Int(missing=0, strict=False)
        palimit = fields.Int(missing=50, strict=False, validate=[
            lambda x: 0 < x <= 50
        ])
        pagelimit = fields.Int(missing=10, strict=False, validate=[
            lambda x: 0 < x <= 20
        ])
        pasubprojects = \
            fields.Bool(missing=True, truthy=('yes', 'true'), falsy=('no', 'false'))
        missing_only = \
            fields.Bool(missing=False, truthy=('yes', 'true'), falsy=('no', 'false'))

    def make_start_requests(self, **kwds):
        query = {}
        if self.args.model is not None:
            query['_cls'] = self.args.model
        if self.args.ns is not None:
            query['ns'] = self.args.ns
        if self.args.missing_only:
            query['$or'] = [
                { 'assessments': { '$exists': False } },
                { 'assessments': None },
                { 'assessments': [] }
            ]

        cursor = _.Page.objects.aggregate(
            { '$match': query },
            { '$project': { '_id': 1 } }
        )
        cursor = list(cursor)
        params = { 'palimit': self.args.palimit }
        if self.args.pasubprojects:
            params['pasubprojects'] = 'true'

        for chunk in chunked(cursor, n=self.args.pagelimit):
            url = self.make_query(
                prop='pageassessments',
                pageids='|'.join(str(doc['_id']) for doc in chunk),
                **{ **params, **kwds }
            )
            yield Request(url)

    def start_requests(self):
        yield from self.make_start_requests()

    def modify_url(self, url, **kwds):
        url = unquote(url)
        parsed = urlparse(url)
        query = {
            k:v for k, v in (
                param.split('=') for param in parsed.query.split('&')
            )
        }
        query.update(**kwds)
        url = self.make_query(**query)
        return unquote(url)

    def parse(self, response):
        data = super().parse(response)
        pages = list(jmp.search('query.pages', data).values())
        pacontinue = jmp.search('continue.pacontinue', data)
        for page in pages:
            if 'missing' in page:
                continue
            page_id = page['pageid']
            title = page['title']
            assessments = page.get('pageassessments', [])
            doc = {
                '_id': page_id,
                'title': title,
                'assessments': assessments
            }
            if pacontinue:
                url = self.modify_url(response.url, pacontinue=pacontinue)
                yield Request(url)
            yield doc
