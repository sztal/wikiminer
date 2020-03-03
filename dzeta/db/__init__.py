"""Database booster."""
import json
from datetime import date, datetime
from ..meta import Interface
from ..schema import Schema, fields


class DBModelInterface(Interface):
    """Database model interface base abstract class.

    It is used to enhance ODM/ORM model classes.
    Typical usecases are adding validation and conversion method,
    additional methods such as helpers for bulk writes etc.

    It also make it possible to define additional schema rules for
    validating and normalizing data. It is useful especially for enhancing
    :py:meth:`from_dict` method to make it more robust.

    Special attribute `__schema_map__` can be used to store a mapping
    from field classes used in a given ODM/ORM to :py:class:`marshmallow.Field`
    classes.

    See Also
    --------
    dzeta.meta.Interface : Interface class

    Attributes
    ----------
    model : type
        Database model object (class).
        This is alias for :py:attr:`dzeta.meta.Interface._`.
    """
    __schema_map__ = {}


    @property
    def model(self):
        return self._

    @property
    def schema(self):
        return self.model.__schema__

    def from_dict(self, dct, only_dict=False, **kwds):
        """Instantiate record from dict.

        Parameters
        ----------
        dct : dict
            Data dictionary.
        only_dict : bool
            Should only normalized `dict` be returned.
        **kwds :
            Passed to :py:meth:`marshmallow.Schema.load`.
        """
        dct = self.schema.load(dct, **kwds)
        if only_dict:
            return dct
        return self.model(**dct)

    def to_dict(self):
        """Dump record to a dict.

        Parameters
        ----------
        fields : iterable of str
            Names of fields to include.
        """
        raise NotImplementedError

    def from_json(self, string, **kwds):
        """Instantiate record from JSON string.

        Parameters
        ----------
        string : str
            Valid JSON string.
        **kwds :
            Passed to :py:meth:`from_dict`.
        """
        dct = json.loads(string)
        return self.from_dict(dct, **kwds)

    def to_json(self):
        """Dump record to a JSON string.

        Parameters
        ----------
        fields : iterable of str
            Names of fields to include.
        **kwds :
            Passed to :py:func:`json.dumps`.
        """
        dct = self.to_dict()
        return self.schema.dumps(dct)

    @classmethod
    def get_schema(cls, model):
        """Get schem `dict` from model class.

        This can be implemented on subclass to allow automatic setting
        of enhanced schemas. The function should return a `dict`
        that can be used in :py:meth:`marshmallow.Schema.from_dict`.
        """
        raise NotImplementedError

    @classmethod
    def set_schema(cls, model):
        """Set schema on model."""
        schema = { k: v for k, v in cls.get_schema(model).items() if v }
        model.__schema__ = Schema.from_dict(schema)()

    @classmethod
    def get_field_meta(cls, field):
        """Get field metadata."""
        return {
            'alias': getattr(field, 'alias', None),
            'converter': getattr(field, 'converter', None)
        }

    @classmethod
    def get_field_schema(cls, field):
        """Get schema definition from model field."""
        meta = cls.get_field_meta(field)
        schemacls = cls.__schema_map__.get(field.__class__, fields.Raw)
        return schemacls(**meta)

    @classmethod
    def inject(cls, instance, init=True, **kwds):
        """Inject reference to the interface instance in the original instance.

        See :py:meth:`dzeta.meta.Interface.inject` for more details.
        """
        instance = super().inject(instance, init=init, **kwds)
        cls.set_schema(instance)
        return instance
