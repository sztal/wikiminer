"""Simple script for monitoring COVID-19 WikiProject."""
import re
from datetime import datetime
import requests
from ..utils import get_botlist



COVID_CIRRUS_URL = \
    'https://en.wikipedia.org/w/api.php?action=query&prop=cirrusdoc&format=json&titles=Wikipedia:WikiProject_COVID-19'
COVID_ASSESSMENTS_URL = \
    'https://en.wikipedia.org/w/api.php?action=query&format=json&list=projectpages&wppprojects=COVID-19&wppassessments=true&wpplimit=500'

_rx_bot = re.compile(r"(^|[\s_\|/#:])?Bot([\s_\|/#:]|$)", re.IGNORECASE)
_rx_user = re.compile(r"^User([ _]talk)?:", re.IGNORECASE)


def get_cirrus():
    data = requests.get(COVID_CIRRUS_URL).json()
    query = data['query']
    pages = query['pages']
    data = pages[list(pages.keys()).pop()]
    doc = data['cirrusdoc'][0]
    return doc

def get_members():
    doc = get_cirrus()
    source = doc['source']
    outlinks = source['outgoing_link']
    members = set(_rx_user.sub(r"", x) for x in outlinks if _rx_user.search(x))
    botlist = set(get_botlist())
    return list(m for m in members if m not in botlist and not _rx_bot.search(m))

def get_assessments():
    more = True
    wppcontinue = None

    while more:
        url = COVID_ASSESSMENTS_URL
        if wppcontinue:
            url += '&wppcontinue='+wppcontinue
        data = requests.get(url).json()
        pa = data['query']['projects']['COVID-19']
        yield from pa
        wppcontinue = data.get('continue', {}).get('wppcontinue')
        more = bool(wppcontinue)

def get_covid_data():
    members = list(get_members())
    assessments = list(get_assessments())
    return {
        'wp': 'COVID-19',
        'datetime': datetime.now(),
        'members': members,
        'assessments': assessments
    }
