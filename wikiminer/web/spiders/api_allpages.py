"""API Spider: all pages iterator endpoint."""
import jmespath as jmp
from scrapy import Request
from taukit.utils import slice_chunks, parse_date
from . import ApiSpider


class ApiAllPagesMain(ApiSpider):
    """API spider for all pages iterator endpoint."""
    name = 'api_allpages_main'

    def make_start_request(self, **kwds):
        url = self.make_query(
            list='allpages',
            apnamespace=0,
            aplimit=500,
            **kwds
        )
        return Request(url)

    def start_requests(self):
        yield self.make_start_request()

    def parse(self, response):
        data = super().parse(response)
        allpages = jmp.search('query.allpages', data)
        for chunk in slice_chunks(allpages, 50):
            url = self.make_query(
                prop='cirrusdoc',
                pageids='|'.join(map(str, (i['pageid'] for i in chunk)))
            )
            request = Request(url, callback=self.parse_cirrus)
            yield request
        cont = jmp.search('continue.apcontinue', data)
        if cont:
            request = self.make_start_request(apcontinue=cont)
            yield request

    def _to_date(self, date):
        if date:
            return parse_date(date)
        return None

    def parse_cirrus(self, response):
        data = super().parse(response)
        pages = jmp.search('query.pages', data)
        for page_id in pages:
            p = pages[page_id]
            src = jmp.search('cirrusdoc[0].source', p)
            pages[page_id] = {
                'page_id': page_id,
                'ns': jmp.search('ns', p),
                'title': jmp.search('title', p),
                'type': jmp.search('cirrusdoc[0].type', p),
                'source_text': jmp.search('source_text', src),
                'template': jmp.search('template', src),
                'timestamp': self._to_date(jmp.search('timestamp', src)),
                'create_timestamp': self._to_date(jmp.search('create_timestamp', src)),
                # 'redirect': jmp.search('redirect', src),
                'text_bytes': jmp.search('text_bytes', src),
                'category': jmp.search('category', src),
                'popularity_score': jmp.search('popularity_score', src)
            }
        url = self.make_query(
            prop='pageassessments',
            pageids='|'.join(map(str,  (p for p in pages)))
        )
        request = Request(url, callback=self.parse_assessments)
        request.meta['pages'] = pages
        yield request

    def parse_assessments(self, response):
        data = super().parse(response)
        pages = response.meta['pages']
        for p in data['query']['pages'].values():
            page_id = jmp.search('page_id', p)
            pages[page_id] = jmp.search('pageassessments', p)
        for item in pages.values():
            if item is not None:
                yield item
