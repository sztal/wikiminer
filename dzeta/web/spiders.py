"""Base _Dzeta_ spider classes."""
# pylint: disable=no-member
import json
from scrapy import Spider
from ..schema import Schema

_SPIDER_ARGS = '__spider_args__'


class DzetaSpider(Spider):
    """_Dzeta_ webscraping spider.

    Schema for custom arguments can be defined through nested class
    named `Args` which has to subclass :py:class:`dzeta.schema.Schema`.
    This is a simple wrapper around :py:class:`marshmallow.Schema`.

    See Also
    --------
    scrapy : Webscraping framework
    """
    class Args(Schema):
        pass

    @property
    def args(self):
        if hasattr(self, _SPIDER_ARGS):
            return getattr(self, _SPIDER_ARGS)
        schema = self.Args()
        a = schema.load({
            f: getattr(self, f) for f in schema.fields
            if hasattr(self, f)
        })
        setattr(self, _SPIDER_ARGS, a)
        return a

    def parse(self, response):
        raise NotImplementedError


class DzetaAPI(DzetaSpider):
    """_Dzeta_ API spider.

    It assumes that response body is JSON, so it automatically parses it
    properly.

    See Also
    --------
    scrapy : Webscarping framework
    """
    def parse(self, response):
        """Parse JSON response."""
        data = json.loads(response.body_as_unicode())
        return data
