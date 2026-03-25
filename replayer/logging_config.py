import logging.config
import os

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        'replayer': {
            'handlers': ['console'],
            'level': log_level,
            'propagate': False,
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
            'format': '%(asctime)s %(levelname)-8s (%(processName)s) [%(name)s:%(lineno)s] %(message)s',
        },
    },
})


def get_logger(name: str) -> logging.Logger:
    """Get a logger for the given module name.

    Use as: logger = get_logger(__name__)
    """
    return logging.getLogger(name)
