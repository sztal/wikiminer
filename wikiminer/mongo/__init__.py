"""*MongoDB* connector based on *pymongo* and *Mongoengine*

See Also
--------
pymongo
mongoengine
"""
# pylint: disable=wildcard-import
import mongoengine
from .models import *


def init(user, password, host, port, db, authentication_db=None, **kwds):
    """Initilize Mongoengine ODM.

    Parameters
    ----------
    user : str
        Username to authenticate with.
    password : str
        User authentication password.
    host : str
        Host IP address/name.
    port : str
        Port number.
    db : str
        Database name.
    authentication_db : str or None
        Authentication database name.
        Use `db` if ``None``.
    kwds :
        Keyword arguments passed to
        :py:func:`mongoengine.connect`.
    """
    def connect(uri, authentication_source, **kwds):
        """Connect to MongoDB."""
        return mongoengine.connect(
            host=uri,
            authentication_source=authentication_source,
            **kwds
        )

    mongo_uri = 'mongodb://{username}:{password}@{host}:{port}/{db}'
    if not authentication_db:
        authentication_db = db
    uri = mongo_uri.format(
        username=user,
        password=password,
        host=host,
        port=port,
        db=db
    )
    mongo = connect(uri, authentication_db, **kwds)
    return mongo
