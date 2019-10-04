import logging.config

from .avro_from_sql import *
from .cli import *
from .constants import *
from .kafka import *
from .tracked_tables import *

log_level = os.getenv('LOG_LEVEL', 'DEBUG').upper()

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        __name__: {
            'handlers': ['console'],
            'level': log_level,
            'propagate': True,
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': log_level,
            'formatter': 'simple',
        },
    },
    'formatters': {
        'simple': {
            'datefmt': '%Y-%m-%d %H:%M:%S',
            'format': '(%(threadName)s) %(asctime)s %(levelname)-8s [%(name)s:%(lineno)s] %(message)s',
        },
    },
})
