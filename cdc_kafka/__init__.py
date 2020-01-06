import logging.config
import os

import sentry_sdk

sentry_sdk.init()

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

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
            'format': '%(asctime)s %(levelname)-8s [%(name)s:%(lineno)s] %(message)s',
        },
    },
})
