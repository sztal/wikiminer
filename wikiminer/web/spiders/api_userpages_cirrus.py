# pylint: disable=no-member,protected-access
from more_itertools import chunked
import jmespath as jmp
from scrapy import Request
from dzeta.schema import Schema, fields
from . import ApiSpider
from ... import _


class ApiUserpagesCirrus(ApiSpider):
    """API spider for updating userpages cirrus data.

    _Attributes_ section described available user-provided arguments.
    See _Wikipedia API_ docs_ for more info.

    .. _docs: https://en.wikipedia.org/w/api.php?action=help&modules=query%2Bcirrusdoc

    Attributes
    ----------
    model : str, optional
        Mongoengine collection class name to determine userset.
        Do not pass anything to get cirrus for userpages for all users
        defined in the database.
    limit : int
        Number of pages in one batch. Defaults to ``50``.
    missing_only : {'yes', 'true', 'no', 'false'}
        Should only data for pages without cirrus data be fetched.
    """
    name = 'api_userpages_cirrus'
    current_pids = []

    class Args(Schema):
        ns = fields.Int(missing=2, strict=False, validate=[
            lambda x: x in (2, 3)
        ])
        limit = fields.Int(missing=50, strict=False, validate=[
            lambda x: 0 < x <= 50
        ])
        missing_only = \
            fields.Bool(missing=False, truthy=('yes', 'true'), falsy=('no', 'false'))
        apfilterredir = fields.String(missing='nonredirects', validate=[
            lambda x: x in ('nonredirects', 'redirects', 'all')
        ])

    def make_ap_request(self, apprefix, **kwds):
        url = self.make_query(
            list='allpages',
            aplimit=self.args.limit,
            apfilterredir=self.args.apfilterredir,
            apprefix=apprefix,
            apnamespace=self.args.ns,
            **kwds
        )
        return Request(url, callback=self.parse_ap, meta={
            'user_name': apprefix
        })

    def make_cirrus_request(self, user_name, page_ids, **kwds):
        url = self.make_query(
            prop='cirrusdoc',
            pageids='|'.join(str(pid) for pid in page_ids),
            **kwds
        )
        return Request(url, callback=self.parse_cirrus, meta={
            'user_name': user_name
        })

    def parse_ap(self, response):
        data = super().parse(response)

        apcontinue = jmp.search('continue.apcontinue', data)
        user_name = response.meta['user_name']
        if apcontinue:
            request_ap = self.make_ap_request(
                apprefix=user_name,
                apcontinue=apcontinue
            )
            yield request_ap

        pages = jmp.search('query.allpages', data)
        if pages:
            page_ids = [ p['pageid'] for p in pages ]
            request_cirrus = self.make_cirrus_request(user_name, page_ids)
            yield request_cirrus

    def parse_cirrus(self, response):
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
            user_name = response.meta['user_name']
            if user_name.endswith('/'):
                user_name = user_name[:-1]
            page.update(
                user_name=user_name,
                page_type=cirrus['type'],
                source_text=source['source_text'],
                template=source['template'],
                timestamp_updated=source['timestamp'],
                timestamp_created=source['create_timestamp']
            )
            yield page

    def make_start_requests(self, **kwds):
        pipeline = [
            { '$project': {
                '_id': 0,
                'user_name': 1
            } }
        ]
        if self.args.missing_only:
            pipeline += [
                { '$lookup': {
                    'from': _.Page._.get_collection().name,
                    'let': { 'user_name': '$user_name' },
                    'pipeline': [
                        { '$match': {
                            '_cls': _.UserPage._class_name,
                            'ns': self.args.ns,
                            '$expr': { '$eq': [ '$user_name', '$$user_name' ] }
                        } },
                        { '$project': {
                            '_id': 1
                        } }
                    ],
                    'as': 'userpages'
                } },
                { '$addFields': {
                    'n_pages': { '$size': '$userpages' }
                } },
                { '$match': {
                    'n_pages': 0
                } }
            ]
        cursor = _.User.objects.aggregate(*pipeline)
        for doc in cursor:
            request_main = self.make_ap_request(
                apprefix=doc['user_name'],
                apto=doc['user_name']
            )
            request_sub = self.make_ap_request(apprefix=doc['user_name']+'/')
            yield request_main
            yield request_sub

    def start_requests(self):
        yield from self.make_start_requests()
