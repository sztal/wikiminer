"""Wikipedia parser."""
import re
import html
import unicodedata
import attr
from more_itertools import unique_everseen
from dzeta.utils import parse_date


@attr.s
class WikiParser:
    """Wikipedia parser.

    Attributes
    ----------
    source : src
        Source (Wiki code) of a page.
    """
    # Class attributes
    _rx_msg = re.compile(
        r"(?P<msg>(?<=\n).*)"
        r"(?P<sig>\[\[User( talk)?:.*?(UTC))",
        re.IGNORECASE
    )
    _rx_sig = re.compile(
        r"(?P<username>(?<=:).*?(?=[\/#\|\]]))"
        r".*?"
        r"(?P<ts>(\d\d:\d\d)?,?\s*?(\d\d?\s+?[a-z]+\s+?\d{4}|\d{4}\s+?[a-z]+\s+?\d\d?))"
        r".*?"
        r"(?P<tz>...$)",
        re.IGNORECASE
    )
    _rx_name = re.compile(r"^User( talk)?:\s*", re.IGNORECASE)
    _rx_ws = re.compile(r"[\s_]+", re.IGNORECASE)
    _date_formats = (
        "%H:%M, %d %B %Y",
        "%H:%M, %d %b %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%B %d, %Y at %H:%M:%S"
    )
    _rx_user_code = re.compile(
        r"(\{\{|\[\[|:)User([ _]talk)?[\|:](?P<username>[^\{\}\[\]\|#/]+)",
        re.IGNORECASE
    )


    # Instance attributes
    source = attr.ib(converter=str)

    def _remove_format_control(self, x):
        return "".join(c for c in x if unicodedata.category(c) != 'Cf')

    def _normalize_user_name(self, x):
        s = html.unescape(x.strip())
        s = self._remove_format_control(s)
        s = self._rx_name.sub(r"", s)
        s = (s[:1].upper() + s[1:])
        s = self._rx_ws.sub(r" ", s)
        return s.strip()

    def parse_posts(self, remove_comments=False):
        """Parse posts from the source.

        Parameters
        ----------
        remove_comments : bool
            Should comments be removed.
        """
        source = self.source
        if remove_comments:
            source = re.sub(r"<!--.*?-->", "", source, re.IGNORECASE)
        for match in self._rx_msg.finditer(source):
            msg = match.group('msg')
            sig = self._rx_sig.search(match.group('sig'))
            if not sig:
                continue
            try:
                ts = parse_date(sig.group('ts'), date_formats=self._date_formats)
            except OverflowError:
                continue
            user_name = sig.group('username')
            if not user_name:
                continue
            dct = {
                'user_name': self._normalize_user_name(user_name),
                'timestamp': ts,
                'content': msg
            }
            yield dct

    def parse_user_shortcodes(self):
        """Parse user shortcodes from the source."""
        def _iter():
            for match in self._rx_user_code.finditer(self.source):
                user_name = match.group('username')
                if user_name:
                    user_name = user_name.strip()
                if not user_name:
                    continue
                user_name = self._normalize_user_name(user_name)
                yield user_name

        yield from unique_everseen(_iter())

    def parse_talk_threads(self, remove_comments=True):
        """Parse talk (possibly) nested talk threads.

        Parameters
        ----------
        remove_comments : bool
            Should comments be removed.
        """
        _rx_depth = re.compile(r"^:*")

        def depth(post):
            return len(_rx_depth.match(post['content']).group())

        def get_next_parent(parent, posts):
            while True:
                post = next(posts)
                while depth(post) > depth(parent):
                    parent['replies'].append(post)
                    post = get_next_parent(post, posts)
                else:
                    return post

        def clean_depth_marks(post):
            post['content'] = \
                _rx_depth.sub(r"", post['content']).strip()
            post['replies'] = [ clean_depth_marks(p) for p in post['replies'] ]
            return post

        posts = self.parse_posts(remove_comments=remove_comments)
        posts = map(lambda p: { **p, 'replies': [] }, posts)
        try:
            parent = next(posts)
            while True:
                next_parent = get_next_parent(parent, posts)
                parent = clean_depth_marks(parent)
                yield parent
                parent = next_parent
        except StopIteration:
            return
