"""Parser for discussion threads on Wikipedia talk pages."""
import re
from . import WikiParser, WikiParserRX


class WikiParserThreadsRX(WikiParserRX):
    """Container-class with pre-compiled regex objects."""
    thread = re.compile(r"(^\s*)(?==)", re.IGNORECASE | re.MULTILINE)
    title = re.compile(r"=+(?P<title>.*?)=+", re.IGNORECASE)
    signature = re.compile(
        r"(\[\[|\{\{)User([ _]talk)?:.*?(\]\]|\}\}).*?\([A-Z]{3,3}\)",
        re.IGNORECASE
    )
    user = re.compile(
        r"(\[\[|\{\{)User([ _]talk)?:"
        r"(?P<user>[^\|#/\[\]\{\}]*)",
        re.IGNORECASE
    )
    timestamp = re.compile(
        r"(?P<ts>(?<=\s)[\w\d:\.,-/\s]*?)"
        r"[\s\W]*\(UTC\)\s*$",
        re.IGNORECASE
    )
    depth = re.compile(r"^[:\*#]+")
    outdent = re.compile(
        r"^:*\{\{(outdent|od)\|(?P<n>(\d+|:+))\}\}",
        re.IGNORECASE
    )
    nl = re.compile(r"(\n|$)",)


class WikiParserThreads(WikiParser):
    """Parser for threads on Wikipedia talk pages.

    This parser is based on some assumptions about the structure of talk
    pages which are usually correct, but in some cases it may produce
    somewhat distorted results (i.e. multiple posts lumped into one).

    Attributes
    ----------
    source : str
        Source (Wiki code) of a page.
    rx : type
        Container-class with pre-compiled regex objects.
    """
    # Class attributes
    rx = WikiParserThreadsRX

    # ###########################################################################

    def iter_threads(self):
        """Iterate over threads.

        Threads are separated by header denoted by `==`.
        """
        for thread in self.rx.thread.split(self.source):
            thread = thread.strip()
            if thread:
                title = self.rx.title.match(thread)
                if title:
                    thread = thread[title.end():]
                    title = title.group('title').strip()
                if thread:
                    thread = thread.strip()
                    yield title, thread

    def iter_posts(self, thread):
        """Iterate over posts in a thread."""
        # This is uglt
        sigs = list(self.rx.signature.finditer(thread))
        N = len(sigs)
        start = None
        for idx, rx in enumerate(sigs, 1):
            if idx < N:
                end = self.rx.nl.search(thread, rx.end()).start()
            else:
                end = None
            post = thread[slice(start, end)]
            yield rx.group(), post
            start = end

    def parse_threads(self):
        """Parse threads and posts."""
        tid = 0
        for thread in self.iter_threads():
            title, thread = thread
            thread = {
                'topic': title,
                'posts': [
                    self._process_post(sig, post)
                    for sig, post in self.iter_posts(thread)
                ]
            }
            if thread['posts']:
                tid += 1
                thread['tid'] = tid
                yield self._process_thread(thread)

    def _process_post(self, sig, post):
        """Process signatures and posts in a tidy dictionary."""
        user_name = self.rx.user.search(sig)
        if user_name:
            user_name = user_name.group('user')
        timestamp = self.rx.timestamp.search(sig)
        if timestamp:
            timestamp = timestamp.group('ts')
        content = post.strip()
        depth = self._count_depth(content)
        # content = self.rx.depth.sub(r"", content).strip()
        # content = self.rx.outdent.sub(r"", content).strip()
        dct = {
            'user_name': self.sanitize_user_name(user_name),
            'timestamp': self.parse_date(timestamp),
            'depth': depth,
            'content': content
        }
        return dct

    def _process_thread(self, thread):
        # Sanitize depth values
        posts = thread.pop('posts', [])
        for idx, post in enumerate(posts):
            if idx == 0:
                post['depth'] = 0
            else:
                post['depth'] += 1
            post['comments'] = []

        dtree, posts = posts[0], posts[1:]
        stack = [ (dtree, dtree['depth']) ]
        for post in posts:
            depth = post['depth']
            while True:
                parent, parent_depth = stack[-1]
                if depth <= parent_depth:
                    stack.pop()
                else:
                    post['depth'] = parent['depth'] + 1
                    parent['comments'].append(post)
                    stack.append((post, depth))
                    break

        return { **thread, 'dtree': dtree }

    def _count_depth(self, s):
        m = self.rx.depth.match(s)
        depth = len(m.group()) if m else 0
        out = self.rx.outdent.match(s)
        if out:
            n = out.group('n')
            if n.startswith(':'):
                n = len(n)
            else:
                n = int(n)
            depth += n
        return depth
