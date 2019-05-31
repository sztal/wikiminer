"""Mongoengine models."""
# pylint: disable=no-member,protected-access
from taukit.db.mongo import Document, EmbeddedDocument


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
