import re
from pathlib import Path
from wikiminer.parsers.threads import WikiParserThreads

HERE = Path(__file__).absolute().parent
DATA = HERE / 'data'

# Threads 1:
# https://en.wikipedia.org/wiki/Wikipedia_talk:WikiProject_Anatomy/Open_Tasks/Archive_2
with open(DATA / 'threads-1.txt', 'r') as s:
    x1 = s.read().strip()


P = WikiParserThreads(x1)

for thread in P.parse_threads():
    import ipdb; ipdb.set_trace()
