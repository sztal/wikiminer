import re
from pathlib import Path
from wikiminer.parsers.threads import WikiParserThreads
from wikiminer import _

HERE = Path(__file__).absolute().parent
DATA = HERE / 'data'

def case(idx):
    with open(DATA / f"threads-{idx}.txt", 'r') as s:
        return next(WikiParserThreads(s.read().strip()).parse_threads())

def show_thread(url):
    page_title, topic = url.split('#')
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


# Thread 1:
# Wikipedia_talk:WikiProject_Anatomy/Open_Tasks/Archive_2
# Thread 2 (outdent):
# WikiProject_College_football/Archive_16#**Coach_navbox_tenure_years_being_changed**

page_title = 'Wikipedia_talk:WikiProject_Albums/Archive_24'
topic = '"Sources"_tag_placed_on_album_articles_containing_only_infobox_&_track_listing'
url = page_title+'#'+topic

# show_thread(url)

show(case(1))
