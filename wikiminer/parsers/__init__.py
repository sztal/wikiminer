"""Base parser classes and helpers."""
import re
from datetime import datetime
import html
import unicodedata
from dateutil.parser import parse as parse_date


_DATE_FORMATS = (
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


class WikiParserRX:
    """Container-class with pre-compiled regex objects."""
    tag = re.compile(r"</?.*?>", re.IGNORECASE | re.MULTILINE)
    name = re.compile(r"^User( talk)?:\s*", re.IGNORECASE)
    whitespace = re.compile(r"[\s_]+", re.IGNORECASE)
    nbsp = re.compile(r"&nbsp;")


class WikiParser:
    """Wikipedia parser base class.

    Attributes
    ----------
    source : str
        Source (Wiki code) of a page.
    rx : type
        Container-class with pre-compiled regex objects.
    """
    # Class attributes
    rx = WikiParserRX
    _date_formats = _DATE_FORMATS

    ###########################################################################

    def __init__(self, source):
        """Initialization method."""
        self.source = str(source).strip()

    def sanitize(self, x):
        """Sanitize a source string."""
        x = self.rx.tag.sub(r"", x)
        x = self.rx.nbsp.sub(r" ", x)
        return x

    def parse_date(self, ts):
        """Parse date from a timestamp string.

        Parameters
        ----------
        ts : str
            Timestamp string.
        """
        exc = None

        if not ts:
            return None

        for fmt in self._date_formats:
            try:
                return datetime.strptime(ts, fmt)
            except ValueError:
                continue

        try:
            ts = parse_date(ts, ignoretz=True)
            return ts
        except (OverflowError, TypeError, ValueError):
            return None

    def sanitize_user_name(self, x):
        s = html.unescape(x.strip())
        s = self._remove_format_control(s)
        s = self.rx.name.sub(r"", s)
        s = s.capitalize()
        s = self.rx.whitespace.sub(r" ", s)
        return s.strip()

    def _remove_format_control(self, x):
        return "".join(c for c in x if unicodedata.category(c) != 'Cf')
