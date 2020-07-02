"""Wikiminer scripts.

They are accessible also from the global `_` object in the `scripts`
attribute (aliased with `s` attribute).
"""
# pylint: disable=no-member,protected-access
import re
import json
from collections import defaultdict
from collections.abc import Sequence
from itertools import chain
import requests
from bs4 import BeautifulSoup as bs
from more_itertools import chunked
from tqdm import tqdm
from pymongo import UpdateMany, UpdateOne
from wikiminer import _
from wikiminer.parsers.wiki import WikiParserPost
from wikiminer.parsers.threads import WikiParserThreads


def cursor_jl(cursor, filepath):
    """Dump cursor to JSON lines."""
    with open(filepath, 'x') as f:
        for doc in tqdm(cursor):
            f.write(json.dumps(doc, default=str)+"\n")


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
    rx_ex = re.compile(r"^Wikipedia( talik)?:\s*?Wiki\s*?Projects?:?\s*?([^/]*?)\s*?(/|$).*", re.IGNORECASE)
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


def make_wp_member_category_pages(n=5000, update_kws=None, **kwds):
    """Detect and flag category pages with WP member lists.

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
    rx_wp = re.compile(
        r"^Category:(Wikipedia:)?\s*Wiki\s*Project(?P<wp>.*?)(member|participant)",
        re.IGNORECASE
    )
    _.CategoryPage.objects.update(_cls=_.Page._class_name)
    _.Page.objects(
        ns=14,
        title=re.compile(r"WikiProject.*(member|participant)", re.IGNORECASE)
    ).update(_cls=_.CategoryPage._class_name)

    def make_update_op(doc):
        doc['wp_raw'] = rx_wp.search(doc['title']).group('wp').strip()
        op = _.CategoryPage._.dct_to_update(doc)
        return op

    cursor = _.CategoryPage.objects.aggregate(
        { '$match': { 'ns': 14 } },
        { '$project': {
            '_id': 1,
            'title': 1
        } }
    )
    ops = map(make_update_op, cursor)
    for info in _.CategoryPage._.bulk_write(ops, n=n, **kwds):
        info.pop('upserted', None)
        print(info)

    cursor = _.CategoryPage.objects.aggregate(
        { '$match': {
            '_cls': _.CategoryPage._class_name,
            'ns': 14
        } },
        { '$lookup': {
            'from': _.CategoryPage._.get_collection().name,
            'let': { 'wp': '$wp_raw' },
            'pipeline': [
                { '$match': {
                    '_cls': _.WikiProjectPage._class_name,
                    'ns': { '$in': [ 4, 5 ] }
                } },
                { '$group': {
                    '_id': { 'wp': '$wp', 'wp_raw': '$wp_raw' }
                } },
                { '$project': {
                    '_id': 0,
                    'wp': '$_id.wp',
                    'wp_raw': '$_id.wp_raw'
                } },
                { '$match': {
                    '$expr': { '$or': [
                        { '$eq': [ '$wp', '$$wp' ] },
                        { '$eq': [ '$wp_raw', '$$wp' ] }
                    ] }
                } },
                { '$project': {
                    '_id': 1,
                    'wp': 1
                } }
            ],
            'as': 'wp_page'
        } },
        { '$project': {
            'wp': { '$arrayElemAt': [ '$wp_page', 0 ] }
        } },
        { '$project': {
            'wp': '$wp.wp'
        } }
    )

    ops = map(lambda d: _.CategoryPage._.dct_to_update(d), cursor)
    for info in _.CategoryPage._.bulk_write(ops, n=n, **kwds):
        info.pop('upserted', None)
        print(info)


def make_page_wp_labels(n=5000, update_kws=None, **kwds):
    """Add WP labels to pages (main and main talk) based on page assessments.

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

    cursor = _.Page.objects.aggregate(
        { '$match': {
            'ns': 0,
            '$and': [
                { 'assessments': { '$nin': [ {}, [], None ] } },
                { 'assessments': { '$exists': True } }
            ]
        } },
        { '$project': {
            '_id': 1,
            'title': 1,
            'assessments': { '$objectToArray': '$assessments' }
        } },
        { '$project': {
            '_id': 1,
            'title': 1,
            'projects': '$assessments.k'
        } }
    )

    def make_update_ops(doc):
        talk = {
            'title': 'Talk:'+doc.pop('title'),
            'projects': doc['projects'][:]
        }
        yield _.Page._.dct_to_update(doc, **update_kws)
        match = { 'title': talk.pop('title') }
        yield _.Page._.dct_to_update(talk, match, **update_kws)

    ops = chain.from_iterable(map(make_update_ops, cursor))
    for info in _.Page._.bulk_write(ops, n=n, **kwds):
        info.pop('upserted', None)
        print(info)
    _.Page.objects(ns__exists=False).delete()


def parse_posts(cursor, model, n=5000, update_kws=None, **kwds):
    """Parse posts from pages' content and update them in the databse.

    Parameters
    ----------
    cursor : pymongo.command_cursor.CommandCursor
        Cursor for iterating over documents.
        Documents must contain `_id` and `source_text` fields.
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
        Passed to :py:meth:dzeta.db.mongo.MongoModelInterface.bulk_write`.
    """
    update_kws = update_kws or {}

    def make_update_op(doc, pbar):
        pbar.set_postfix({ '_id': str(doc['_id']) })
        parser = WikiParserPost(doc.get('source_text', ''))
        _id = doc['_id']
        posts = list(parser.parse_posts())
        dct = {
            '_id': _id,
            'posts': posts
        }
        op = model._.dct_to_update(dct, **update_kws)
        return op

    cursor = tqdm(cursor.batch_size(n))
    ops = filter(None, map(
        lambda x: make_update_op(x, cursor),
        cursor
    ))
    for info in model._.bulk_write(ops, n=n, **kwds):
        info.pop('upserted', None)
        print(info)


def parse_talk_threads(cursor, model, n=5000, update_kws=None, **kwds):
    """Parse talk threads from pages' content and update them in the database.

    Parameters
    ----------
    cursor : pymong.command_cursor.CommandCursor
        Cursor for iterating over documents.
        Documents must contain `_id` and `source_text` fields.
    model : interfaced mongoengine collection
        :py:class:`mongoengine.Document` with
        :py:class:`dzeta.db.mongo.MongoModelInterface`.
    n : int
        Batch size for updating.
        Full batch if falsy or non-positive.
    update_kwds : dict, optional
        Keyword parameters passed to
        :py:meth:`dzeta.db.mongo.MongoModelInterface.bulk_write`.
    """
    update_kws = update_kws or {}

    def make_update_op(doc, pbar):
        pbar.set_postfix({ '_id': str(doc['_id']) })
        parser = WikiParserThreads(doc.get('source_text', ''))
        _id = doc['_id']
        topics = list(parser.parse_threads())
        dct = {
            '_id': _id,
            'topics': topics
        }
        op = model._.dct_to_update(dct, **update_kws)
        return op

    cursor = tqdm(cursor)
    ops = filter(None, map(
        lambda x: make_update_op(x, cursor),
        cursor
    ))
    for info in model._.bulk_write(ops, n=n, **kwds):
        info.pop('upserted', None)
        print(info)


def parse_users(cursor, model, n=5000, update_kws=None, **kwds):
    """Parse user shortcodes from pages' code and update them in the database.

    Parameters
    ----------
    cursor : pymongo.command_cursor.CommandCursor
        Cursor for iterating over documents.
        Documents must contain `_id` and `source_text` fields.
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
        Passed to :py:meth:dzeta.db.mongo.MongoModelInterface.bulk_write`.
    """
    update_kws = update_kws or {}

    def make_update_op(doc):
        parser = WikiParserPost(doc.get('source_text', ''))
        users = list(parser.parse_user_shortcodes())
        dct = dict(_id=doc['_id'], users=users)
        op = model._.dct_to_update(dct, **update_kws)
        return op

    ops = tqdm(filter(None, map(make_update_op, cursor)))
    for info in model._.bulk_write(ops, n=n, **kwds):
        info.pop('upserted', None)
        print(info)


def parse_category_users(cursor, model, n=5000, update_kws=None, **kwds):
    """Parse user shortcodes from member category pages.

    Parameters
    ----------
    cursor : pymongo.command_cursor.CommandCursor
        Cursor for iterating over documents.
        Documents must contain `_id` and `source_text` fields.
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
        Passed to :py:meth:dzeta.db.mongo.MongoModelInterface.bulk_write`.
    """
    update_kws = update_kws or {}
    base_url = 'https://en.wikipedia.org/wiki/'
    rx_start = re.compile(r"^User([ _]talk)?:", re.IGNORECASE)
    rx_user = re.compile(r"^User([ _]talk)?:(?P<user>.*?)(/|#|$)")
    selector = '#mw-pages .mw-content-ltr li > a'

    def make_update_op(doc):
        resp = requests.get(base_url+doc['title'])
        html = bs(resp.content, features='html.parser')
        users =  [
            rx_user.search(x.text).group('user')
            for x in html.select(selector)
            if rx_start.search(x.text)
        ]
        users = list(set(users))
        dct = dict(_id=doc['_id'], users=users)
        op = model._.dct_to_update(dct, **update_kws)
        return op

    ops = tqdm(filter(None, map(make_update_op, cursor)))
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
        'edit_count': '$editcount',
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


def get_talk_threads(filepath, model, ns, match=None, sequences=False,
                     sanitize_content=True, **kwds):
    """Get talk threads from WP pages.

    Parameters
    ----------
    filepath : str
        Filepath to save data at. Format is JSON lines.
    ns : int or tuple of int
        Namespaces to use (``4`` and/or ``5``).
        By default only talk is gathered.
    match: dict, optional
        Additional `$match` stage conditions.
    sequences : bool
        Should sequences be returned instead of raw threads.
        Sequences rearrange and duplicate data so for every path
        from a root to a leaf the full path is returned.
    sanitize_content: bool
        Should content be sanitize for NLP/Linguistic processing, i.e.
        with LIWC.
    **kwds :
        Additional options for the aggregation pipeline.
    """
    def sanitize(text):
        text = re.sub(r"\{\{.*?\}\}", r"", text)
        text = re.sub(r"\[\[.*?:(.*?)(\|.*?)?\]\]", r"\1", text)
        text = re.sub(r" +", r" ", text)
        text = re.sub(r"[\n\t]+", r"    ", text)
        text = re.sub(r"-+\s*$", r"", text)
        return text.strip()

    def unwind_threads(cursor):
        def unwind_subthreads(thread, idx, tid):
            subthreads = thread.pop('subthreads', None) or []
            if thread and sanitize_content:
                thread['content'] = sanitize(thread['content'])
            thread = {
                **doc,
                'tid': tid,
                'idx': str(idx),
                **thread
            }
            yield thread
            for i, sub in enumerate(subthreads, 1):
                yield from unwind_subthreads(sub, idx=f"{idx}.{i}", tid=tid)
        tid = 0
        pid = None
        for doc in cursor:
            if pid is not None and pid != doc['page_id']:
                tid = 0
            tid += 1
            post = doc.pop('post')
            yield from unwind_subthreads(post, 0, tid=tid)
            pid = doc['page_id']

    def unwind_sequences(threads):
        raise NotImplementedError("see how 'unwind_threads' is implemented now")
        path = []
        threads = map(lambda dct: {
            'page_id': dct['page_id'],
            'topic': dct['topic'],
            'idx': dct['idx']
        }, threads)
        for thread in threads:
            same_topic = bool(path) and \
                thread['page_id'] == path[-1]['page_id'] and \
                thread['topic'] == path[-1]['topic']

            if not same_topic or not thread['idx'].startswith(path[-1]['idx']):
                yield from path
                if same_topic:
                    path = [
                        t for t in path
                        if thread['idx'].startswith(t['idx'])
                    ]
                else:
                    path = []
            path.append(thread)

        yield from path

    if not isinstance(ns, Sequence):
        ns = (ns,)
    cursor = model.objects.aggregate(
        { '$match': {
            '_cls': model._class_name,
            'ns': { '$in': ns },
            **(match or {})
        } },
        { '$unwind': '$topics' },
        { '$addFields': {
            'topic': '$topics.topic',
            'post': { '$arrayElemAt': [ '$topics.threads', 0 ] },
            'threads': { '$slice': [ '$topics.threads', 1, 999999999 ] }
        } },
        { '$project': {
            '_id': 0,
            'page_id': '$_id',
            'ns': 1,
            'wp': 1,
            'title': 1,
            'topic': 1,
            'post': {
                'content': '$post.content',
                'depth': '$post.depth',
                'user_name': '$post.user_name',
                'timestamp': '$post.timestamp',
                'subthreads': { '$concatArrays': [
                    '$post.subthreads',
                    '$threads'
                ] }
            }
        } },
        **kwds
    )

    cursor = unwind_threads(cursor)
    if sequences:
        cursor = unwind_sequences(cursor)
    with open(filepath, 'x') as f:
        for doc in tqdm(cursor):
            f.write(json.dumps(doc, default=str)+"\n")


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
