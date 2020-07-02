"""MongoDB booster."""
# pylint: disable=no-member,arguments-differ,protected-access
# pylint: disable=redefined-outer-name
from more_itertools import chunked
from pymongo import UpdateOne
from mongoengine import BooleanField, DateTimeField
from mongoengine import IntField, FloatField
from mongoengine import EmbeddedDocumentField, EmbeddedDocumentListField
from . import DBModelInterface
from ..schema import fields


class MongoModelInterface(DBModelInterface):
    """MongoDB model interface class.

    It is designed to work with :py:class:`pymodm.MongoModel` subclasses.

    See Also
    --------
    dzeta.meta.Interface : Interface class

    Attributes
    ----------
    model : type
        :py:class:`pymodm.MongoModel` model object (class).
        This is alias for :py:attr:`dzeta.meta.Interface._instance`.
    """
    __schema_map__ = {
        BooleanField: fields.Boolean,
        DateTimeField: fields.DateTime,
        IntField: fields.Integer,
        FloatField: fields.Float
    }

    @property
    def pk_field(self):
        return self.model._meta['id_field']
    @property
    def id_field(self):
        return self.pk_field

    def get_collection(self):
        """Get collection object."""
        return self.model._get_collection()

    def to_dict(self):
        """Dump record to a dict.

        Parameters
        ----------
        fields : iterable of str
            Names of fields to include.
            Include all if ``None``.
        """
        return self.to_mongo().to_dict()

    def to_update(self, upsert=True, **kwds):
        """Dump to :py:class:`pymongo.operations.UpdateOne` object.

        Parameters
        ----------
        upsert : bool
            Should upsert mode be used.
        **kwds :
            Passed to :py:class:`pymongo.operations.UpdateOne`.
        """
        dct = self.to_dict()
        return self.dct_to_update(dct, upsert=upsert, **kwds)


    def dct_to_update(self, dct, match=None, upsert=True, **kwds):
        """Dump valid document dictionary to :py:class:`pymongo.UpdateOne` op.

        Parameters
        ----------
        match: dict
            Match query.
        upsert : bool
            Should upsert mode be used.
        **kwds :
            Passed to :py:class:`pymongo.operations.UpdateOne`.
        """
        if hasattr(self.model, '_cls'):
            dct['_cls'] = self.model._class_name
        if match is None:
            match = { self.pk_field: dct.pop(self.pk_field) }
        return UpdateOne(
            filter=match,
            update={ '$set': dct },
            upsert=upsert,
            **kwds
        )

    def bulk_write(self, ops, n=0, **kwds):
        """Execute bulk write operations.

        Parameters
        ----------
        ops : iterable of write ops
            Write ops such as :py:class:`pymongo.operations.UpdateOne`.
        n : int
            Chunke size. If falsy or non-positive then all ops are executed
            in one batch.
        **kwds :
            Passed to :py:meth:`pymongo.collection.Collection.bulk_write`.
        """
        if n and n > 0:
            ops = chunked(ops, n)
        else:
            ops = [ ops ]
        collection = self.get_collection()
        for batch in ops:
            res = collection.bulk_write(list(batch), **kwds)
            yield res.bulk_api_result

    @classmethod
    def get_field_meta(cls, field):
        meta = super().get_field_meta(field)
        meta.update(
            data_key=meta['alias'],
            required=field.required,
            allow_none=field.null
        )
        if not field.required:
            meta.update(
                default=field.default,
                missing=field.default
            )
        if isinstance(field, EmbeddedDocumentField):
            meta['converter'] = field.document_type._.from_dict
        elif isinstance(field, EmbeddedDocumentListField):
            meta['converter'] = lambda doc: [
                field.document_type._.from_dict(x) for x in doc
            ]
        return meta

    @classmethod
    def get_schema(cls, model):
        """Set schema for :py:class:`mongoengine.Document` model."""
        return { k: cls.get_field_schema(v) for k, v in model._fields.items() }
