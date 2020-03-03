"""*MongoDB* connector based on *pymongo* and *Mongoengine*

See Also
--------
pymongo
mongoengine
"""
# pylint: disable=wildcard-import
from mongoengine import connect
from .models import *


def init(user, password, host, port, db, **kwds):
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
    kwds :
        Passed to :py:func:`pymodm.connection.connect`.
    """
    mongo_uri = 'mongodb://{username}:{password}@{host}:{port}/{db}'
    uri = mongo_uri.format(
        username=user,
        password=password,
        host=host,
        port=port,
        db=db
    )
    connect(host=uri, **kwds)
