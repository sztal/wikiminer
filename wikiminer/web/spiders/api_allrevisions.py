"""API Spider: all revisions iterator endpoint."""
# pylint: disable=no-member
import jmespath as jmp
from scrapy import Request
from dzeta.schema import Schema, fields
from . import ApiSpider


class ApiAllRevision(ApiSpider):
    """API spider for all revisions list iterator endpoint.

    _Attributes_ section describes available user-provided arguments.
    See _Wikipedia API_ docs_ for more info.

    .. _docs: https://en.wikipedia.org/w/api.php?action=help&modules=query%2Ballrevisions

    Attributes
    ----------
    arvstart : str
        Timestamp to begin with.
    arvend : str
        Timestamp to end at.
    arvdir : str
        Enumeration direction.
    arvnamespace int or str
        Namespace number or sequence of numbers divided by ``|`` (as string).
        Defaults to ``'0|1|2|3|4|5'``.
    arvlimit : int
        Pagination size for querying API. Defaults to ``500``.
    arvprop : str
        Revisions props to return. Defaults to
        ``'ids|flags|timestamp|user|size|sha1|contentmodel|comment|tags'``.
    arvslots {'main'}
        Should not be changed.
    """
    name = 'api_allrevisions'

    class Args(Schema):
        arvstart = fields.Str(required=False)
        arvend = fields.Str(required=False)
        arvdir = fields.Str(missing='newer')
        arvnamespace = fields.Str(missing='0|1|2|3|4|5', strict=False)
        arvlimit = fields.Int(missing=500, strict=False)
        arvprop = fields.Str(
            missing='ids|flags|timestamp|user|size|sha1|contentmodel|comment|tags'
        )
        arvslots = fields.Str(missing='main')

    def make_start_request(self, **kwds):
        url = self.make_query(
            list='allrevisions',
            **self.args,
            **kwds
        )
        return Request(url)

    def start_requests(self):
        yield self.make_start_request()

    def parse(self, response):
        data = super().parse(response)
        cont = jmp.search('continue.arvcontinue', data)
        if cont:
            request = self.make_start_request(arvcontinue=cont)
            yield request
        allrevisions = jmp.search('query.allrevisions', data)
        for page in allrevisions:
            for rev in page.get('revisions', []):
                dct = {
                    'rev_id': rev['revid'],
                    'parent_id': rev['parentid'],
                    'page_id': page['pageid'],
                    'ns': page['ns'],
                    'user_name': rev['user'],
                    'size': rev['size'],
                    'sha1': rev.get('sha1'),
                    'contentmodel': rev.get('contentmodel'),
                    'comment': rev.get('comment'),
                    'tags': rev.get('tags', []),
                    'rev_size': None,
                    'minor': 'minor' in rev,
                    'timestamp': rev['timestamp']
                }
                yield dct
