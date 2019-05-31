"""Spider base classes and mixins."""
import json
from scrapy import Spider
from furl import furl


class ApiSpider(Spider):
    """Wikipedia API spider.

    Attributes
    ----------
    base_url : str
        Base url of the API. Defaults to: ``https://en.wikipedia.org/w/api.php``.
    """
    base_url = 'https://en.wikipedia.org/w/api.php'

    def make_url(self, url=None, **kwds):
        """Make url.

        Parameters
        ----------
        url : str
            URL string. Defaults to `base_url`.
        **kwds :
            URL params.
        """
        url = furl(url) if url is not None else furl(self.base_url)
        url.add(kwds)
        return url.tostr()

    def make_query(self, action='query', frm='json', url=None, **kwds):
        """Query API.

        Parameters
        ----------
        action : str
            Query action. See Wikipedia API documentation for the details.
        frm : str
            Name of the response format. Defaults to ``'json'``.
        url : str
            URL string. Defaults to `base_url`.
        **kwds :
            URL params.
        """
        kwds = { 'action': action, 'format': frm, **kwds }
        url = self.make_url(url=url, **kwds)
        return url

    def parse(self, response):
        """Parse JSON response."""
        data = json.loads(response.body_as_unicode())
        return data
