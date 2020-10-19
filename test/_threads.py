import re
from pathlib import Path
from wikiminer.parsers.threads import WikiParserThreads
from wikiminer import _

HERE = Path(__file__).absolute().parent
DATA = HERE / 'data'


def show_thread(page_title, topic):
    page_title = page_title.replace('_', ' ')
    topic = topic.replace('_', ' ')
    doc = _.WikiProjectPage.objects.get(title=page_title)
    thread = next(d for d in doc['discussions'] if d['topic'] == topic)
    show(thread)

def show(thread):
    def _show(dtree):
        depth = dtree['depth']
        user = dtree['user_name']
        print("\n"+"  "*depth+f"[{depth}][{user}] "+dtree['content'][:40])
        for dt in dtree['comments']:
            _show(dt)
    dtree = thread['dtree']
    _show(dtree)


# Threads 1:
# https://en.wikipedia.org/wiki/Wikipedia_talk:WikiProject_Anatomy/Open_Tasks/Archive_2
with open(DATA / 'threads-1.txt', 'r') as s:
    x1 = s.read().strip()


P = WikiParserThreads(x1)

page_title = 'Wikipedia_talk:WikiProject_Albums/Archive_24'
topic = '"Sources"_tag_placed_on_album_articles_containing_only_infobox_&_track_listing'

show_thread(
    page_title=page_title,
    topic=topic
)

thread = None
for thread in P.parse_threads():
    show(thread)
