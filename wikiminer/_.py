"""Main objects namespace."""
# pylint: disable=wildcard-import,unused-import,unused-wildcard-import,no-member
import os
from configparser import ExtendedInterpolation
from dzeta.config import Config
from . import mongo
from . import scripts
from .mongo.models import *


MODE = os.environ.get('RUNTIME_MODE', 'DEV')
MODULE_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.split(MODULE_DIR)[0]

# Aliases
s = scripts


cfg = Config(interpolation=ExtendedInterpolation())
cfg.read(os.path.join(ROOT_DIR, 'wikiminer.cfg'))

mongo.init(
    user=cfg.getenvvar(MODE, 'mongo_user'),
    password=cfg.getenvvar(MODE, 'mongo_pass'),
    host=cfg.getenvvar(MODE, 'mongo_host'),
    port=cfg.getenvvar(MODE, 'mongo_port'),
    db=cfg.getenvvar(MODE, 'mongo_db')
)
