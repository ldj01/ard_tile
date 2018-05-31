"""Utilities for interacting with host system."""

import sys
import logging


logger = logging.getLogger('ARD_determine_segments_framework')


class L2pgsLoggingFilter(logging.Filter):
    """Set subsystem name via logging filter, for later log parsing."""

    def filter(self, record):
        """Set subsystem for log parsing."""
        record.subsystem = 'ARDFramework'
        return True


class L2pgsExceptionFormatter(logging.Formatter):
    """Custom Exception formatter."""

    def formatException(self, exc_info):
        """Initialize exception formatting."""
        result = super(L2pgsExceptionFormatter, self).formatException(exc_info)
        return repr(result)

    def format(self, record):
        """Remove all new-lines from an exception traceback."""
        msg = super(L2pgsExceptionFormatter, self).format(record)
        if record.exc_text:
            msg = msg.replace('\n', ' ')
            msg = msg.replace('\\n', ' ')
        return msg


def setup_logger(level='INFO', stream='stdout'):
    """Initialize the message logging components."""
    global logger

    # Setup the logging level
    logging_level = getattr(logging, level.upper())

    handler = logging.StreamHandler(getattr(sys, stream.lower()))
    formatter = L2pgsExceptionFormatter(fmt=('%(asctime)s.%(msecs)03d'
                                             ' %(subsystem)s'
                                             ' %(levelname)-8s'
                                             ' %(message)s'),
                                        datefmt='%Y-%m-%dT%H:%M:%S')

    handler.setFormatter(formatter)
    handler.addFilter(L2pgsLoggingFilter())

    logger.setLevel(logging_level)
    logger.addHandler(handler)
