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
        r"^[:\*#]*\{\{(outdent|od)\d?(\|(\d+|:+))?\}\}"
    )
    # outdent = re.compile(
    #     r"^:*\{\{(outdent|od)(2|\|(?P<n>(\d+|:+)))?\}\}",
    #     re.IGNORECASE
    # )
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
        for idx, match in enumerate(sigs, 1):
            if idx < N:
                end = self.rx.nl.search(thread, match.end()).start()
            else:
                end = None
            post = thread[slice(start, end)]
            yield match.group(), post
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
        user_name = None
        for match in self.rx.user.finditer(sig):
            user_name = match.group('user')
        timestamp = self.rx.timestamp.search(sig)
        if timestamp:
            timestamp = timestamp.group('ts')
        content = post.strip()
        depth, outdent = self._count_depth(content)
        dct = {
            'user_name': self.sanitize_user_name(user_name),
            'timestamp': self.parse_date(timestamp),
            'depth': depth,
            'dots': depth,
            'outdent': outdent,
            'content': content,
            'comments': []
        }
        return dct

    def _process_thread(self, thread):
        posts = thread.pop('posts', [])
        dtree, posts = posts[0], posts[1:]
        dtree['depth'] = dtree['dots'] = 0
        stack = [ dtree ]
        for post in posts:
            post['dots'] += 1
            parent = None
            while parent is None:
                parent = stack[-1]
                if post['outdent']:
                    post['depth'] = parent['depth'] + 1
                elif post['dots'] <= parent['dots']:
                    stack.pop()
                    parent = None
            post['depth'] = parent['depth'] + 1
            parent['comments'].append(post)
            stack.append(post)
        thread = { **thread, 'dtree': dtree }
        self._clean_thread(thread)
        return thread

    def _clean_thread(self, thread):
        def _clean(dtree):
            del dtree['dots']
            del dtree['outdent']
            for dt in dtree['comments']:
                _clean(dt)
        _clean(thread['dtree'])

    def _count_depth(self, s):
        m = self.rx.depth.match(s)
        depth = len(m.group()) if m else 0
        outdent = self.rx.outdent.match(s)
        is_simple_outdent = False
        # if out:
        #     n = out.group('n')
        #     if n:
        #         if n.startswith(':'):
        #             n = len(n)
        #         else:
        #             n = int(n)
        #         depth += n
        #     else:
        #         is_simple_outdent = True
        return depth, outdent
