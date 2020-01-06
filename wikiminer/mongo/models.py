"""Mongoengine models."""
# pylint: disable=no-member,protected-access
from mongoengine import StringField, DateTimeField
from mongoengine import IntField, FloatField
from mongoengine import ListField, DictField
from taukit.db.mongo import Document


COLLECTIONS = ()


def collection_name_to_model(collection):
    for model_name in COLLECTIONS:
        try:
            model = globals()[model_name]
        except KeyError:
            continue
        if model._get_collection_name() == collection:
            return model
    raise NameError(f"no matching model for '{collection}'")


class Page(Document):
    """Page document.

    Attributes
    ----------
    _id : IntField
        Page id. Primary key.
    ns : IntField
        Namespace.
    title : StringField
        Title.
    page_type : StringField
        Page type.
    source_text : StringField
        Wiki source.
    template : ListField(StringField)
        Templates.
    timestamp : DateTimeField
        Timestamp of the last modification.
    create_timestamp : DateTimeField
        Page creation timestamp.
    text_bytes : IntField
        Number of bytes of text.
    category : ListField(StringField)
        Categories a page belongs to.
    popularity_score : FloatField
        Page popularity score.
    assessments : DictField
        Page assessments
    """
    _id = IntField(primary_key=True)
    ns = IntField(required=True)
    title = StringField(required=True)
    page_type = StringField()
    source_text = StringField()
    template = ListField(StringField)
    timestamp = DateTimeField()
    create_timestamp = DateTimeField()
    popularity_score = FloatField()
    assessments = DictField()
    # Settings
    _field_names_map = {
        '_id': [ 'page_id', 'pageid' ],
        'page_type': [ 'type' ]
    }
    meta = {
        'collection': 'wm_pages',
        'indexes': [
            'ns',
            'title',
            'page_type'
        ]
    }
