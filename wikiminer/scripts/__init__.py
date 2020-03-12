"""Wikiminer scripts.

They are accessible also from the global `_` object in the `scripts`
attribute (aliased with `s` attribute).
"""
# pylint: disable=no-member,protected-access
import re
import json
from collections import defaultdict
import requests
from more_itertools import chunked
from tqdm import tqdm
from pymongo import UpdateMany, UpdateOne
from wikiminer import _
from wikiminer.parsers.wiki import WikiParser


def docs_from_json(path, model, n=5000, update_kws=None, **kwds):
    """Create/update documents from json(lines) file.

    Parameters
    ----------
    path : str
        Path to a file with document per line as a single valid JSON.
    model : interfaced mongoengine collection
        :py:class:`mongoengine.Document` with
        :py:class:`dzeta.db.mongo.MongoModelInterface`.
    n : int
        Batch size for updating.
        Full batch if falsy or non-positive.
    update_kws : dict, optional
        Keyword parameters passed to
        :py:meth:`dzeta.db.mongo.MongoModelInterface.to_update`.
    **kwds :
        Passed to :py:meth:`dzeta.db.mongo.MongoModelInterface.bulk_write`.
    """
    update_kws = update_kws or {}

    def make_update_op(line):
        dct = model._.from_json(line, only_dict=True, partial=True)
        op = model._.dct_to_update(dct, **update_kws)
        return op

    with open(path, 'r') as f:
        ops = map(make_update_op, f)
        for info in model._.bulk_write(ops, n=n, **kwds):
            info.pop('upserted', None)
            print(info)


def make_wp_pages(n=5000, update_kws=None, **kwds):
    """Update `Page` documents and create `WikiProjectPage` subcollection.

    Parameters
    ----------
    n : int
        Batch size for updating.
        Full batch if falsy or non-positive.
    update_kws : dict, optional
        Keyword parameters passed to
        :py:meth:`dzeta.db.mongo.MongoModelInterface.to_update`.
    **kwds :
        Passed to :py:meth:`dzeta.db.mongo.MongoModelInterface.bulk_write`.
    """
    update_kws = update_kws or {}
    rx_wp = re.compile(r"^Wikipedia( talk)?:\s*?WikiProject[^/]+", re.IGNORECASE)
    rx_ne = re.compile(r"Signpost|Editorial team", re.IGNORECASE)
    rx_ex = re.compile(r"^Wikipedia( talk)?:\s*?Wiki\s*?Projects?:?\s*?([^/]*?)\s*?(/|$).*", re.IGNORECASE)
    rx_rm = re.compile(r"^(WikiPedia:)?WikiProjects?:?\s*([^/]+)\s*(/|$)", re.IGNORECASE)
    # Aggregation stages
    match = { '$match': {
        'ns': { '$in': [4, 5] },
        '$and': [
            { 'title': rx_wp },
            { 'title': { '$not': rx_ne } }
        ]
    } }
    project = { '$project': {
        '_id': 1,
        'title': 1,
        '_cls': 1
    } }

    def make_update_op(doc):
        doc['wp_raw'] = rx_ex.sub(r"\2", doc['title']).strip()
        doc['_cls'] = _.WikiProjectPage._class_name
        op = _.WikiProjectPage._.dct_to_update(doc)
        return op

    cursor = _.Page.objects.aggregate(match, project)
    ops = map(make_update_op, cursor)
    for info in _.WikiProjectPage._.bulk_write(ops, n=n, **kwds):
        info.pop('inserted', None)
        print(info)
    # Correct WP
    print("Correcting WP names ...")
    base_url = "https://en.wikipedia.org/w/api.php?action=query&prop=cirrusdoc&titles={titles}&format=json"
    # Set ops
    ops = []
    # Correction loop
    for names in tqdm(list(chunked(_.WikiProjectPage.objects.distinct('wp_raw'), n=50))):
        titles = "|".join("Wikipedia:WikiProject "+name for name in names)
        url = base_url.format(titles=titles)
        response = requests.get(url)
        body = response.json()
        pages = body['query']['pages']
        for pid, data in pages.items():
            pid = int(pid)
            wp_title_raw = wp_title = rx_rm.match(data['title']).group(2)
            try:
                cirrus = data.pop('cirrusdoc').pop()
            except (IndexError, KeyError):
                cirrus = None
            if pid < 0 or cirrus is None:
                op = {
                    'set___cls': _.Page._class_name,
                    'unset__wp': '',
                    'unset__wp_raw': ''
                }
            else:
                cirrus_pid = int(cirrus['id'])
                if cirrus_pid != pid:
                    m = rx_rm.match(cirrus['source']['title'])
                    if m is None:
                        op = {
                            'set___cls': _.Page._class_name,
                            'unset__wp': '',
                            'unset__wp_raw': ''
                        }
                    else:
                        wp_title = m.group(2)
                op = { 'set__wp': wp_title }
            flt = { '_cls': _.WikiProjectPage._class_name, 'wp_raw': wp_title_raw }
            _.WikiProjectPage.objects(**flt).update(**op)
    # Second correction loop (what the FUCK MONGODB!!)
    cursor = _.WikiProjectPage.objects.aggregate(
        { '$match': { 'wp': { '$exists': False } } },
        { '$sortByCount': '$wp_raw' },
        allowDiskUse=True
    )
    print("Second correction loop ...")
    for doc in tqdm(cursor):
        _.WikiProjectPage.objects(wp_raw=doc['_id']).update(set__wp=doc['_id'])


def make_user_pages(n=10000, update_kws=None, **kwds):
    """Detect and convert page documents corresponding to user pages.

    Parameters
    ----------
    n : int
        Batch size for updating.
        Full batch if falsy or non-positive.
    update_kws : dict, optional
        Keyword parameters passed to
        :py:meth:`dzeta.db.mongo.MongoModelInterface.to_update`.
    **kwds :
        Passed to :py:meth:`dzeta.db.mongo.MongoModelInterface.bulk_write`.
    """
    update_kws = update_kws or {}
    # Reset user pages
    _.UserPage.objects.update(set___cls=_.Page._class_name)
    cursor = _.Page.objects.aggregate(
        { '$match': {
            'ns': { '$in': [2, 3] }
        } },
        { '$project': {
            'title': 1,
            'start': { '$add': [
                { '$indexOfCP': [ '$title', ':' ] },
                1
            ] },
            'end': { '$indexOfCP': [ '$title', '/' ] }
        } },
        { '$addFields': {
            'end': { '$cond': [
                { '$gte': [ '$end', 0 ] },
                '$end',
                { '$strLenCP': '$title' },
            ] }
        } },
        { '$project': {
            'user_name': { '$substrCP': [
                '$title', '$start', { '$subtract': [ '$end', '$start' ] }
            ] }
        } },
        allowDiskUse=True
    )
    # pylint: disable=unnecessary-lambda
    ops = map(lambda d: _.UserPage._.dct_to_update(d, **update_kws), cursor)
    for info in _.UserPage._.bulk_write(ops, n=n, **kwds):
        info.pop('upserted', None)
        print(info)


def parse_posts(model, cursor, n=5000, update_kws=None, **kwds):
    """Parse posts from pages' content and update them in the databse.

    Parameters
    ----------
    model : interfaced mongoengine collection
        :py:class:`mongoengine.Document` with
        :py:class:`dzeta.db.mongo.MongoModelInterface`.
    cursor : pymongo.command_cursor.CommandCursor
        Cursor for iterating over documents.
        Documents must contain `_id` and `source_text` fields.
    n : int
        Batch size for updating.
        Full batch if falsy or non-positive.
    update_kws : dict, optional
        Keyword parameters passed to
        :py:meth:`dzeta.db.mongo.MongoModelInterface.to_update`.
    **kwds :
        Passed to :py:meth:dzeta.db.mongo.MongoModelInterface.bulk_write`.
    """
    update_kws = update_kws or {}
    counter = 0

    def make_update_op(doc):
        parser = WikiParser(doc.get('source_text', ''))
        nonlocal counter
        counter += 1
        _id = doc['_id']
        print(f"\rItem {counter}|id={_id}", end="")
        posts = list(parser.parse_posts())
        dct = {
            '_id': _id,
            'posts': posts
        }
        op = model._.dct_to_update(dct, **update_kws)
        return op

    ops = filter(None, map(make_update_op, cursor))
    for info in model._.bulk_write(ops, n=n, **kwds):
        info.pop('upserted', None)
        print(info)


def get_direct_communication(filepath=None, **kwds):
    """Get direct communication per user from userpages.

    Parameters
    ----------
    filepath : str, optional
        Filepath for saving results as JSON lines.
        A cursor is returned if not provided.
    **kwds :
        Additional options for the aggregation pipeline.
    """
    pipeline = []
    pipeline.append({ '$project': {
        '_id': 0,
        'user_id': '$_id',
        'user_name': 1,
        'emailable': 1,
        'gender': 1,
        'wp': 1,
        'groups': 1
    } })
    pipeline.append({ '$lookup': {
        'from': _.UserPage._.get_collection().name,
        'let': { 'user_name': '$user_name' },
        'pipeline': [
            { '$match': { '$expr': {
                '$and': [
                    { '$eq': [ '$_cls', _.UserPage._class_name ] },
                    { '$in': [ '$ns', [2, 3] ] },
                    { '$eq': [ '$user_name', '$$user_name' ] }
                ]
            } } },
            { '$project': {
                'posts.content': 0
            } },
            { '$project': {
                '_id': 0,
                'page_id': '$_id',
                'page': '$title',
                'user_name': 1,
                'ns': 1,
                'posts': 1
            } }
        ],
        'as': 'userpage'
    } })

    cursor = _.User.objects.aggregate(
        *pipeline,
        **{ 'allowDiskUse': True, **kwds }
    )

    if filepath:
        with open(filepath, 'x') as handle:
            for doc in tqdm(cursor):
                handle.write(json.dumps(doc, default=str)+"\n")
    return cursor


def make_page_assessments(filepath, n=10000, reset=True, **kwds):
    """Define page assessments objects on page documents.

    Parameters
    ----------
    filepath : str
        Path to a file with page assessments stored as JSON lines.
        One document per line.
    n : int
        Batch size for bulk updating.
    reset: bool
        Should current ``assessments`` objects be reset
        before updating.
    **kwds :
        Passed to :py:meth:dzeta.db.mongo.MongoModelInterface.bulk_write`.
    """
    if reset:
        print("Resetting 'assessments' objects ...")
        _.Page.objects(ns=0).update(set__assessments={}, multi=True)


    print("Finding paged documents ...")
    paged = defaultdict(lambda: 0)
    with open(filepath, 'r') as stream:
        for line in tqdm(stream):
            doc = json.loads(line)
            paged[doc['_id']] += 1

    print("Updating documents ...")
    paged = { k: {} for k, v in paged.items() if v > 1 }

    def iter_docs():
        with open(filepath, 'r') as stream:
            for line in stream:
                doc = json.loads(line)
                if doc['_id'] in paged:
                    if doc['assessments']:
                        paged[doc['_id']].update(**doc['assessments'])
                else:
                    yield {
                        '_id': doc['_id'],
                        'assessments': doc['assessments']
                    }
        for k, v in paged.items():
            yield { '_id': k, 'assessments': v or {} }

    ops = map(
        lambda doc: _.Page._.dct_to_update(doc, upsert=False),
        iter_docs()
    )
    for info in _.Page._.bulk_write(ops, n=n, **kwds):
        info.pop('upserted', None)
        print(info)


def get_page_assessments(filepath=None, **kwds):
    """Get page assessment data.

    Parameters
    ----------
    filepath : str, optional
        Filepath for saving results as JSON lines.
        A cursor is returned if not provided.
    **kwds :
        Additional options for the aggregation pipeline
    """
    cursor = _.Page.objects.aggregate(
        { '$match': {
            '_cls': _.Page._class_name,
            'ns': 0,
            '$and': [
                { 'assessments': { '$exists': True } },
                { 'assessments': { '$nin': [ [], None ] } }
            ]
        } },
        { '$project': {
            '_id': 1,
            'title': 1,
            'ns': 1,
            'assessments': { '$objectToArray': '$assessments' }
        } },
        { '$unwind': '$assessments' },
        { '$addFields': {
            'wp': { '$split': [ '$assessments.k', '/' ] },
            'class': '$assessments.v.class',
            'importance': '$assessments.v.importance',
            'page_id': '$_id',
        } },
        { '$unwind': '$wp' },
        { '$project': {
            '_id': 0,
            'assessments': 0
        } },
        **kwds
    )

    rx = re.compile(r"^WikiProject", re.IGNORECASE)
    def correct_wp(doc):
        doc['wp'] = rx.sub("", doc['wp']).strip()
        return doc

    cursor = map(correct_wp, cursor)

    if filepath:
        with open(filepath, 'x') as handle:
            columns = ['page_id', 'title', 'ns', 'wp', 'class', 'importance']
            handle.write("\t".join(columns)+"\n")
            for doc in tqdm(cursor):
                line = "\t".join(str(doc[col]) for col in columns)+"\n"
                handle.write(line)
    return cursor
