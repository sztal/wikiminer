"""General utility Wikiminer utility functions."""
import re
import requests
from bs4 import BeautifulSoup as bs


BOTS_LIST = \
    'https://en.wikipedia.org/wiki/Wikipedia:List_of_bots_by_number_of_edits'
UNFLAGGED_BOTS_LIST = \
    'https://en.wikipedia.org/wiki/Wikipedia:List_of_bots_by_number_of_edits/Unflagged_bots'

_rx_rm = re.compile(r"User([_ ]talk)?:", re.IGNORECASE)


def get_botlist():
    """Get the current list of bots including unflagged bots."""
    resp = requests.get(BOTS_LIST)
    html = bs(resp.content, features='html.parser')
    bots = html.select('table.wikitable tr td:nth-of-type(2)')
    bots = set(x.text.strip() for x in bots if x)
    resp = requests.get(UNFLAGGED_BOTS_LIST)
    html = bs(resp.content, 'html.parser')
    _bots = html.select('.mw-parser-output ol li a')
    _bots = set(_rx_rm.sub(r"", x.text.strip()) for x in _bots if x)
    bots = bots.union(_bots)
    return list(bots)
