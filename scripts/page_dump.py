"""Dump _Page_ collection to JSON lines."""
# pylint: disable=no-member
import os
import json
from tqdm import tqdm
from taukit.serializers import JSONEncoder
from wikiminer import _


HERE = os.path.dirname(__file__)
FILEPATH = os.path.join(HERE, 'page_dump.jl')
N = _.Page.objects.count()


cursor = _.Page.objects.aggregate(
    { '$project': {
        '_id': 0,
        'page_id': '$_id',
        'ns': 1,
        'title': 1,
        'assessments': { '$objectToArray': '$assessments' }
    } },
    { '$addFields': {
        'assessments': { '$map': {
            'input': '$assessments',
            'as': 'a',
            'in': {
                'k': '$$a.k',
                'class': '$$a.v.class',
                'importance': '$$a.v.importance'
            }
        } }
    } },
    { '$unwind': '$assessments' },
    { '$addFields': {
        'wp': '$assessments.k',
        'wp_class': '$assessments.class',
        'wp_importance': '$assessments.importance'
    } },
    { '$project': {
        'assessments': 0
    } },
    allowDiskUse=True
)

if os.path.exists(FILEPATH):
    os.remove(FILEPATH)

with open(FILEPATH, 'a') as f:
    with tqdm(total=N) as pbar:
        for doc in cursor:
            f.write(json.dumps(doc, cls=JSONEncoder).strip()+"\n")
            pbar.update(1)
