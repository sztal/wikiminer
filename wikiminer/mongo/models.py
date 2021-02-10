"""Mongoengine models."""
# pylint: disable=no-member,protected-access
from datetime import datetime
from mongoengine import Document as _Document, EmbeddedDocument
from mongoengine import ObjectIdField, BooleanField
from mongoengine import StringField, DateTimeField
from mongoengine import IntField, FloatField
from mongoengine import ListField, DictField, EmbeddedDocumentListField
from dzeta.db.mongo import MongoModelInterface as _Interface


__all__ = [
    'Page',
    'UserPage',
    'CategoryPage',
    'WikiProjectPage',
    'WikiProject',
    'Revision',
    'User'
]

# Abstract classes and interfaces ---------------------------------------------

class Document(_Document):
    """Base document class."""
    _id = ObjectIdField(primary_key=True)
    _touched_at = DateTimeField(default=datetime.now)
    # Settings
    meta = {
        'abstract': True
    }

class MongoModelInterface(_Interface):
    """Modified Mongo model interface class."""
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
        doc = super().from_dict(dct, only_dict=only_dict, **kwds)
        if only_dict and '_touched_at' not in doc:
            doc['_touched_at'] = self._._touched_at.default()
        return doc


# Models ----------------------------------------------------------------------


@MongoModelInterface.inject
class WikiProject(Document):
    """WikiProject document.

    Attributes
    ----------
    _id : ObjectIdField
        Generic primary key.
    name : StringField
        Project name.
    subproject : BooleanField
        Flag indicating whether it is a subproject.
        This may not be 100% veridical as determining
        relationships between WikiProject is not trivial.
    parents : ListField(ReferenceField)
        Possible parent projects.
    """
    _id = ObjectIdField(primary_key=True)
    name = StringField(required=True)
    subproject = BooleanField(default=False)
    parents = ListField(StringField(), null=True)
    watchers = IntField(null=True, default=None)
    # Settings
    meta = {
        'collection': 'wm_wikiprojects',
        'indexes': [
            { 'fields': ['name'], 'unique': True },
            'subproject',
            'watchers'
        ]
    }


@MongoModelInterface.inject
class Post(EmbeddedDocument):
    """User post embedded document.

    Attributes
    ----------
    user_name : StringField
        Username.
    timestamp : DateTimeField
        Post timestamp.
    content : StringField
        Post content.
    """
    user_name = StringField(required=True)
    timestamp = DateTimeField(required=True)
    content = StringField()


@MongoModelInterface.inject
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
    timestamp_updated : DateTimeField
        Timestamp of the last modification.
    timestamp_created : DateTimeField
        Page creation timestamp.
    timestamp_record : DateTimeField
        Timestamp of the creation of database record.
    text_bytes : IntField
        Number of bytes of text.
    category : ListField(StringField)
        Categories a page belongs to.
    popularity_score : FloatField
        Page popularity score.
    assessments : ListField(DictField)
        Page assessments.
    posts : EmbeddedDocumentListField(Post)
        Users' posts on the page.
    topics : EmbeddedDocumentListField(DictField)
        Discussion topics on the page.
        Well-defined only for talk pages.
    users : ListField(StringField)
        Users who posted or left their shortcodes on the page.
    projects : ListField(StringField)
        WikiProjects to which the page belongs.
        Used only by pages in main and main talk.
    discussions : ListField(DictField)
        Discussion stored as discussion trees.
    """
    _id = IntField(primary_key=True, alias='pageid')
    ns = IntField(required=True)
    title = StringField(required=True)
    page_type = StringField()
    source_text = StringField()
    template = ListField(StringField())
    timestamp_updated = DateTimeField()
    timestamp_created = DateTimeField()
    timestamp_record = DateTimeField(default=datetime.utcnow)
    popularity_score = FloatField()
    assessments = ListField(DictField(), default=list)
    posts = EmbeddedDocumentListField(Post, default=list)
    topics = ListField(DictField(), default=list)
    users = ListField(StringField(), default=list)
    projects = ListField(StringField(), default=list)
    discussions = ListField(DictField(), default=list)
    # Settings
    meta = {
        'collection': 'wm_pages',
        'indexes': [
            'ns',
            { 'fields': ['title'], 'unique': True, 'cls': False },
            'page_type'
        ],
        'allow_inheritance': True,
        'index_cls': False,
        'index_drop_dups': True
    }


@MongoModelInterface.inject
class WikiProjectPage(Page):
    """WikiProject page model.

    WikiProject pages are pages in project namespaces (4 and 5)
    which are part of the set of internal pages of a given WP.
    Thus, this does not refer to articles that are in scope of a given
    WikiProject.

    See Also
    --------
    Page : standard page model

    Attributes
    ----------
    wp_raw : StringField
        Raw WP name as extracted from the URL (before dealiasing)
    wp : StringField
        Name of the WikiProject
    """
    ns = IntField(required=True, choices=(4, 5))
    wp_raw = StringField(required=True)
    wp = StringField(default=None, null=True)
    # Settings
    meta = {
        'indexes': [
            '_cls',
            'wp'
        ],
        'index_cls': True
    }


@MongoModelInterface.inject
class UserPage(Page):
    """User page model.

    See Also
    --------
    Page : standard page model

    Attributes
    ----------
    ns : IntField
        Has to be either ``2`` (User) or ``3`` (User talk).
    user_name : StringField
        User name.
    """
    ns = IntField(required=True, choices=(2, 3))
    user_name = StringField(required=True)
    # Settings
    meta = {
        'indexes': [
            '_cls',
            'user_name'
        ],
        'index_cls': True
    }


@MongoModelInterface.inject
class CategoryPage(Page):
    """Category page model.

    Category pages are used only inasmuch they store information
    on members of WikiProjects.

    See Also
    --------
    Page : standard page model.
    """
    ns = IntField(required=True, choices=(14, 15))
    wp_raw = StringField(required=True)
    wp = StringField(default=None, null=True)
    # Settings
    meta = {
        'indexes': [
            '_cls'
        ],
        'index_cls': True
    }


@MongoModelInterface.inject
class Revision(Document):
    """Revision model.

    Attributes
    ----------
    _id : IntField
        Revision id (`rev_id`).
    parent_id : IntField
        Parent revision id.
    page_id : IntField
        Page id.
    ns : IntField
        Namespace id.
    user_name : StringField
        User name of the revision author.
    timestamp : DateTimeField
        Revision timestamp.
    comment : StringField
        Revision comment.
    contentmodel : StringField
        Content model.
    text : StringField
        Page text after the revision.
    size : IntField
        Byte lenght of the page after the revision.
    rev_size : IntField
        Byte length of the revision. May be negative.
    sha1 : StringField
        SHA1 hash of the page after the revision.
    minor : BooleanField
        Flag indicating minor revisions.
    """
    _id = IntField(primary_key=True, alias='rev_id')
    parent_id = IntField(required=True, null=True)
    page_id = IntField(required=True)
    user_name = StringField(required=True, null=True)
    ns = IntField(required=True)
    minor = BooleanField(required=True)
    timestamp = DateTimeField(required=True)
    size = IntField(required=True, min=0)
    rev_size = IntField(null=True, default=None, required=False)
    sha1 = StringField(null=True, default=None)
    comment = StringField(null=True)
    contentmodel = StringField(null=True)
    text = StringField(null=True, default=None)
    tags = ListField(default=[])
    # Settings
    meta = {
        'collection': 'wm_revisions',
        'indexes': [
            'parent_id',
            'page_id',
            'user_name',
            'ns',
            'minor',
            'timestamp',
            'size',
            'rev_size',
            '#sha1'
        ],
        'index_background': True
    }


@MongoModelInterface.inject
class User(Document):
    """User document.

    Attributes
    ----------
    _id : ObjectIdField
        Document id. Primary key.
    user_id : IntField
        User id.
    user_name : StringField
        User name.
    editcount: IntField
        Edit count.
    registration : DateTimeField
        Registration timestamp.
    groups : ListField(StringField)
        Groups a user belongs to.
    rights : ListField(StringField)
        Rights a user has.
    emailable : BooleanField
        Is a user emailable with the API method ``Special:Emailuser``.
    missing : BooleanField
        Is user information missing in the current WP database.
    gender : StringField
        Gender. Has to be 'M', 'F' or null.
    wp : ListField(StringField)
        WikiProject a user is involved in.
    """
    # _id = ObjectIdField(primary_key=True)
    _id = IntField(primary_key=True, alias='userid')
    user_name = StringField(unique=True, required=True, alias='name')
    editcount = IntField(min_value=0, required=True)
    registration = DateTimeField(required=True, null=True, default=None)
    groups = ListField(StringField(), default=[])
    memberships = ListField(default=None, alias='groupmemberships')
    rights = ListField(StringField(), default=[])
    emailable = BooleanField(default=False)
    missing = BooleanField(default=False)
    gender = StringField(choices=('M', 'F'), null=True, default=None)
    wp = ListField(StringField(), default=[])
    # Settings
    meta = {
        'collection': 'wm_users',
        'indexes': [
            'user_name',
            'editcount',
            'registration',
            'emailable',
            'gender'
        ]
    }
