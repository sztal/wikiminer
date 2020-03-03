"""API Spider: get user data for WikiProject members."""
# pylint: disable=no-member
import re
from more_itertools import chunked
import jmespath as jmp
import requests
from bs4 import BeautifulSoup as bs
from scrapy import Request
from dzeta.schema import Schema, fields
from . import ApiSpider
from ... import _


class ApiWpUsers(ApiSpider):
    """API spider for getting user data for WikiProject members.

    _Attributes_ section describes available user-provided arguments.
    See _Wikipedia API_ docs for more info.

    .. _docs: https://en.wikipedia.org/w/api.php?action=help&modules=query%2Busers

    Attributes
    ----------
    usprop : str
        User properties to include in the output.
    limit : int
        Number of records in one chunk. Defaults to ``50`` (max value).
    """
    name = 'api_wp_users'
    bot_list = \
        'https://en.wikipedia.org/wiki/Wikipedia:List_of_bots_by_number_of_edits'
    unflagged_bot_list = \
        'https://en.wikipedia.org/wiki/Wikipedia:List_of_bots_by_number_of_edits/Unflagged_bots'
    rx_rm = re.compile(r"User( talk)?:", re.IGNORECASE)
    rx_ip = re.compile(r"^((\d{1,3}\.){3}\d{1,3}|([A-Z0-9]{1,4}:){7}[A-Z0-9]{1,4})", re.IGNORECASE)

    class Args(Schema):
        usprop = fields.Str(
            missing='groups|groupmemberships|editcount|gender|rights|registration|emailable'
        )
        limit = fields.Int(missing=50, strict=False, validate=[
            lambda x: 0 < x <= 50
        ])

    def get_bots(self):
        resp = requests.get(self.bot_list)
        html = bs(resp.content, features='html.parser')
        bots = html.select('table.wikitable tr td:nth-of-type(2)')
        bots = set(x.text.strip() for x in bots if x)
        resp = requests.get(self.unflagged_bot_list)
        html = bs(resp.content, 'html.parser')
        _bots = html.select('.mw-parser-output ol li a')
        _bots = set(self.rx_rm.sub(r"", x.text.strip()) for x in _bots if x)
        bots = bots.union(_bots)
        return bots

    def make_start_requests(self, **kwds):
        bots = self.get_bots()
        cursor = _.WikiProjectPage.objects.aggregate(
            { '$match': { '_cls': 'Page.WikiProjectPage' } },
            { '$unwind': '$posts' },
            { '$group': {
                '_id': '$posts.user_name',
                'wp': { '$addToSet': '$wp' }
            } },
            { '$project': {
                '_id': 0,
                'user_name': '$_id',
                'wp': 1
            } },
            allowDiskUse=True
        )
        for chunk in chunked(cursor, n=self.args.limit):
            user_names = [ doc['user_name'] for doc in chunk ]
            url = self.make_query(
                list='users',
                ususers='|'.join(
                    u for u in user_names
                    if u not in bots and not self.rx_ip.match(u)
                ),
                usprop=self.args.usprop,
                **kwds
            )
            yield Request(url, meta={ d['user_name']: d['wp'] for d in chunk })

    def start_requests(self):
        yield from self.make_start_requests()

    def parse(self, response):
        data = super().parse(response)
        wp = response.meta
        users = jmp.search('query.users', data)
        for user in users:
            if 'missing' in user or 'invalid' in user \
            or 'bot' in user.get('groups', []):
                continue
            user['name'] = user['name']
            user['emailable'] = 'emailable' in user
            user['wp'] = wp[user['name']]
            user['gender'] = {
                'male': 'M',
                'female': 'F'
            }.get(user['gender'])
            yield user
