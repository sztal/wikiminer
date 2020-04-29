"""Wikipedia parser."""
import re
from datetime import datetime
import html
import unicodedata
from dateutil.parser import parse as parse_date
from more_itertools import unique_everseen
# from dzeta.utils import parse_date


class WikiParserPost:
    """Wikipedia parser.

    Attributes
    ----------
    source : src
        Source (Wiki code) of a page.
    """
    # Class attributes
    _rx_msg = re.compile(
        r"(?P<msg>(?<=\n).*)"
        # r"(?P<sig>[\[\{]{2}User([ _]talk)?:.*?[\]\}]{2}.*?\(...\))"
        r"(?P<sig>"
        r"(\[\[User([ _]talk)?:[^\[\]]*?\]\]"
        r"|\{\{User([ _]talk)?:[^\{\}]*?\}\})"
        r".*?\(...\))"
        ,
        re.IGNORECASE
    )
    _rx_sig = re.compile(
        r"(?P<user>(?<=:).*?(?=[\]\}\|#/\\]))"
        r".*?"
        r"(?P<ts>(\d+:\d+|\d\s\w|\w+\s+\d+,?\s*\d{4})[\s\w\d:,;]*?(?=\(...\)))"
        r"(?P<tz>\(...\))"
        ,
        re.IGNORECASE
    )
    _rx_name = re.compile(r"^User( talk)?:\s*", re.IGNORECASE)
    _rx_ws = re.compile(r"[\s_]+", re.IGNORECASE)
    _date_formats = (
        "%H:%M, %d %B %Y",
        "%H:%M, %d %b %Y",
        "%H:%M, %B %d, %Y",
        "%H:%M, %b %d, %Y",
        "%H:%M %B %d %Y",
        "%H:%M %b %d %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%B %d, %Y at %H:%M:%S",
        "%H:%M:%S, %Y-%m-%d",
        "%H:%M %Z %d %B %Y",
        "%H:%M, %b %d, %Y",
        "%H:%M, %Y %B %d",
        "%d %B %Y %H:%M",
        "%d %b %Y %H:%M",
        "%H:%M, %Y %b %d",
        "%H:%M, %Y %B %d",
        "%B %d, %Y %H:%M",
        "%B %d %Y %H:%M",
        "%b %d, %Y %H:%M",
        "%b %d %Y %H:%M"
    )
    _rx_user_code = re.compile(
        r"(\{\{|\[\[|:)User([ _]talk)?[\|:](?P<user>[^\{\}\[\]\|#/]+)",
        re.IGNORECASE
    )
    _rx_tz = re.compile(r"\(...\)")
    _rx_html_tag = re.compile(r"</?.*?>", re.IGNORECASE)


    def __init__(self, source):
        """Initialization method."""
        self.source = self._remove_html_tags(str(source))

    def _remove_html_tags(self, x):
        return self._rx_html_tag.sub(r"", x)

    def _remove_format_control(self, x):
        return "".join(c for c in x if unicodedata.category(c) != 'Cf')

    def _normalize_user_name(self, x):
        s = html.unescape(x.strip())
        s = self._remove_format_control(s)
        s = self._rx_name.sub(r"", s)
        s = s.capitalize()
        s = self._rx_ws.sub(r" ", s)
        return s.strip()

    def _parse_date(self, dt):
        exc = None
        for fmt in self._date_formats:
            try:
                return datetime.strptime(dt, fmt)
            except ValueError:
                continue
        try:
            dt = parse_date(dt, ignoretz=True)
            return dt
        except (OverflowError, TypeError, ValueError):
            return None

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
            ts = sig.group('ts').strip()
            ts = self._parse_date(ts)
            user_name = sig.group('user')
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
                user_name = match.group('user')
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
        parent = None
        try:
            parent = next(posts)
            while True:
                next_parent = get_next_parent(parent, posts)
                parent = clean_depth_marks(parent)
                yield parent
                parent = next_parent
        except StopIteration:
            if parent:
                yield parent
