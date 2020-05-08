"""Parser for discussion threads on Wikipedia talk pages."""
import re
from . import WikiParser, WikiParserRX


class WikiParserThreadsRX(WikiParserRX):
    """Container-class with pre-compiled regex objects."""
    thread = re.compile(r"(^\s*)(?==)", re.IGNORECASE | re.MULTILINE)
    subthreads = re.compile(r"(?<=\(UTC\))$", re.IGNORECASE | re.MULTILINE)
    topic = re.compile(r"^=+(?P<topic>.*?)=+", re.IGNORECASE)
    # Signature
    signature = re.compile(
        r"(\[\[|\{\{)User([ _]talk)?:[^\[\]\{\}]*?(\]\]|\}\})"
        r".*?"
        r"\(UTC\)\s*$",
        re.IGNORECASE | re.MULTILINE | re.DOTALL
    )
    sigstart = re.compile(
        r".*?:(klat[ _])?resU(\[\[|\{\{)",
        re.IGNORECASE | re.MULTILINE | re.DOTALL
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
    # Helpers
    header_start = re.compile(r"^\s*=+", re.IGNORECASE)
    header = re.compile(r"^\s*=+.*?=+", re.IGNORECASE)
    # depth = re.compile(r"^[:\*]+")
    depth = re.compile(r"^:+")
    outdent = re.compile(r"^\{\{(outdent|od\||od2).*?\}\}", re.IGNORECASE)
    tz_trail = re.compile(r"^.*?(?=\)CTU\()", re.IGNORECASE | re.MULTILINE)


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

    ###########################################################################

    def parse_threads(self):
        """Iterate over threads in `source`."""
        for thread in self.rx.thread.split(self.source):
            thread = thread.strip()
            if not thread:
                continue
            thread = self.rx.tz_trail.sub(r"", thread[::-1])[::-1]
            if thread and thread.startswith('='):
                thread = self.parse_thread(thread)
                thread = self.postprocess_thread(thread)
                if thread:
                    yield thread

    def parse_thread(self, thread):
        """Parse single thread source string."""
        thread = self.sanitize(thread)
        topic = self.rx.topic.match(thread)
        if topic:
            topic = topic.group('topic').strip()
        # Parse threads and subthreads
        threads = self._iter_subthreads(thread)
        dct = {
            'topic': topic,
            'threads': [],
        }

        # Start iterating over particular threads
        try:
            thread = next(threads)
        except StopIteration:
            thread = None

        while thread:
            if not dct['threads'] \
            or (thread['depth'] == 0 and not thread.get('outdent')):
                dct['threads'].append(thread)
            thread = self._parse_subthreads(thread, threads)

        return dct

    def postprocess_thread(self, thread):
        """Sanitize thread data and compute derived fields."""
        def postprocess(thread, depth=0):
            thread = self._sanitize_thread(thread)
            thread['depth'] = depth
            subthreads = [
                postprocess(sub, depth=depth+1)
                for sub in thread['subthreads']
            ]
            thread['subthreads'] = [ s for s in subthreads if s ]
            thread = self.compute_fields(thread)
            return thread

        threads = [ postprocess(sub) for sub in thread['threads'] ]
        thread['threads'] = [ t for t in threads if t ]
        return thread

    def compute_fields(self, thread):
        """Compute additional fields on a thread object."""
        content = thread['content']
        sig = self.rx.signature.search(content)

        if sig:
            sig = self.rx.sigstart.match(sig.group()[::-1])
            content = content[:-sig.end()]
            sig = sig.group()[::-1]
            user = self.rx.user.search(sig)
            timestamp = self.rx.timestamp.search(sig)

            if user is not None:
                user = self.sanitize_user_name(user.group('user').strip())
            if timestamp is not None:
                timestamp = timestamp.group('ts').strip()
                timestamp = self.parse_date(timestamp)

            thread.update(
                content=content,
                user_name=user,
                timestamp=timestamp
            )
            thread.pop('outdent', None)
            return thread
        return None

    def _parse_subthreads(self, thread, threads):
        """Parse subthreads in a discussion thread.

        Parameters
        ----------
        thread : dict
            The current thread.
        threads : sequence
            Sequence of threads coming after `thread`.
        """
        try:
            sub = next(threads)
        except StopIteration:
            sub = None

        while sub:
            outdent = bool(self.rx.outdent.match(sub['content']))
            sub['outdent'] = outdent

            if thread['depth'] < sub['depth'] or outdent:
                if outdent:
                    sub['depth'] = 0
                thread['subthreads'].append(sub)
                sub = self._parse_subthreads(sub, threads)
            else:
                return sub

    def _iter_subthreads(self, thread):
        """Iterate over all subthreads in a thread source string.

        Yields
        ------
        int, str
            depth and subthread source string.
        """
        for sub in self.rx.subthreads.split(thread.strip()):
            sub = sub.strip()
            sub = self.rx.header.sub(r"", sub).strip()
            if sub:
                yield {
                    'content': sub,
                    'depth': self._count_depth(sub),
                    'subthreads': []
                }

    def _sanitize_thread(self, thread):
        """Sanitize thread content."""
        content = thread['content'].strip()
        content = self.rx.depth.sub(r"", content).strip()
        content = self.rx.outdent.sub(r"", content).strip()
        thread['content'] = content
        return thread

    def _count_depth(self, s):
        m = self.rx.depth.match(s)
        if m:
            return len(m.group())
        return 0
