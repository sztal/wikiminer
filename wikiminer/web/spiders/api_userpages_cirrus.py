# pylint: disable=no-member,protected-access
from more_itertools import chunked
from scrapy import Request
from .api_pages_cirrus import ApiPagesCirrus
from ... import _


class ApiUserpagesCirrus(ApiPagesCirrus):
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

    def make_start_requests(self, **kwds):
        query = {}
        if self.args.model is not None:
            query['_cls'] = self.args.model
        project = { '$project': { 'user_name': 1 } }
        lookup = { '$lookup': {
            'from': _.UserPage._.get_collection().name,
            'let': { 'user_name': '$user_name' },
            'pipeline': [
                { '$match': {
                    '_cls': _.UserPage._class_name,
                    '$expr': {
                        '$eq': [ '$user_name', '$$user_name' ]
                    }
                } },
                { '$project': { '_id': 1 } }
            ],
            'as': 'pages'
        } }
        if self.args.missing_only:
            lookup['$lookup']['pipeline'][0]['$match'].update(
                source_text={ '$exists': False, '$in': [ None, [] ] }
            )
        add_fields = { '$addFields': { 'pages': '$pages._id' } }
        unwind = { '$unwind': '$pages' }

        cursor = _.User.objects.aggregate(
            { '$match': query }, project, lookup, add_fields, unwind,
            allowDiskUse=True
        )
        for chunk in chunked(cursor, n=self.args.limit):
            url = self.make_query(
                prop='cirrusdoc',
                pageids='|'.join(str(doc['pages']) for doc in chunk),
                **kwds
            )
            yield Request(url)
