"""API Spider: page assessments extractor."""
# pylint: disable=no-member
from furl import furl
import jmespath as jmp
from scrapy import Request
from taukit.utils import slice_chunks
from . import ApiSpider
from ... import _


class ApiPageAssessments(ApiSpider):
    """API spider for extracting page assessments."""
    name = 'api_page_assessments'

    _attributes_schema = {
        'palimit': { 'type': 'integer', 'coerce': int, 'default': 50 },
        'pasubprojects': { 'type': 'string', 'default': 'false' },
        'ns': { 'type': 'integer', 'default': 0, 'coerce': int }
    }

    def make_start_request(self, **kwds):
        attrs = self.get_attributes(return_null=False)
        attrs = { k: v for k, v in attrs.items() if k.startswith('pa') }
        url = self.make_query(
            prop='pageassessments',
            **attrs,
            **kwds
        )
        return Request(url)

    def start_requests(self):
        self.get_attributes()
        cursor = _.Page.objects.aggregate(
            { '$match': { 'ns': self.ns } },
            { '$project': { '_id': 1 } },
            allowDiskUse=True
        )
        pageids = [ d['_id'] for d in cursor ]
        for chunk in slice_chunks(pageids, self.palimit):
            yield self.make_start_request(
                pageids='|'.join(map(str, chunk))
            )

    def parse(self, response):
        data = super().parse(response)
        cont = jmp.search('continue.pacontinue', data)
        if cont:
            url = furl(response.url)
            url.add({ 'pacontinue': cont })
            yield Request(url.tostr())
        pages = jmp.search('query.pages', data)
        for p in pages.values():
            page_id = jmp.search('pageid', p)
            assessments = jmp.search('pageassessments', p)
            if not assessments:
                continue
            item = {
                '_id': page_id,
                'assessments': assessments
            }
            yield item
