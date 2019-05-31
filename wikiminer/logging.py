"""Logging setup."""
from taukit.utils import make_path
from taukit.logging import formatter_default, message_handler
from taukit.logging import rotating_file_handler, timed_rotating_file_handler
from ._ import cfg, MODE, ROOT_DIR


LOG_ROOT_DIR = make_path(ROOT_DIR, cfg.get(MODE, 'log_root_dir'))
LOG_LEVEL = cfg.getenvvar(MODE, 'log_level', fallback='DEBUG')

ROTATING_FILE_HANDLER = {
    **rotating_file_handler,
    'level': LOG_LEVEL
}
TIMED_ROTATING_FILE_HANDLER = {
    **timed_rotating_file_handler,
    'level': LOG_LEVEL
}

MESSAGE_HANDLER = {
    **message_handler,
    'level': 'DEBUG'
}

LOG_SETTINGS = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': formatter_default
    },
    'handlers': {
        'main': {
            **TIMED_ROTATING_FILE_HANDLER,
            'filename': make_path(LOG_ROOT_DIR, 'wikiminer', 'wikiminer.log')
        },
        'error': {
            **ROTATING_FILE_HANDLER,
            'level': 'ERROR',
            'filename': make_path(LOG_ROOT_DIR, 'wikiminer', 'wikiminer-error.log')
        },
        'debug': {
            **ROTATING_FILE_HANDLER,
            'level': 'DEBUG',
            'filename': make_path(LOG_ROOT_DIR, 'wikiminer', 'wikiminer-debug.log')
        },
        'message': MESSAGE_HANDLER
    },
    'loggers': {
        'wikiminer': {
            'propagate': False,
            'handlers': ['main', 'error', 'debug', 'message'],
            'level': LOG_LEVEL
        }
    }
}
