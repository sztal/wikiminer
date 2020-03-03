"""_Dzeta_ custom data types."""
# pylint: disable=multiple-statements,useless-super-delegation
# pylint: disable=arguments-differ,unused-import
from marshmallow import Schema as _Schema, fields, INCLUDE


class SchemaDict(dict):
    """Dict with attribute access that returns ``None`` by default."""
    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            return None


class Schema(_Schema):
    """Simple wrapper around :py:class:`marshmallow.Schema`."""

    def __init__(self, *args, unknown=INCLUDE, **kwds):
        super().__init__(*args, unknown=unknown, **kwds)

    def load(self, obj, *args, **kwds):
        """Simple wrapper around :py:meth:`marshmallow.Schema.load` which
        changes returned object to :py:class:`SchemaDict`.
        """
        return SchemaDict(super().load(obj, *args, **kwds))

    def dump(self, obj, *args, **kwds):
        """Simple wrapper aroud :py:meth:`marshmallow.Schema.load` which
        changes returned object to :py:class:`SchemaDict`.
        """
        return SchemaDict(super().dump(obj, *args, **kwds))
