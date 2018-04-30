""" Utilities for interacting with host system """

import sys
import logging


logger = None # global logger instance set by util.setup_logger()


class L2PGS_LoggingFilter(logging.Filter):
    """ Set subsystem name via logging filter, for later log parsing """
    def filter(self, record):
        record.subsystem = 'ARDFramework'

        return True


class L2PGS_ExceptionFormatter(logging.Formatter):
    """ Custom Exception formatter """
    def formatException(self, exc_info):
        result = super(L2PGS_ExceptionFormatter, self).formatException(exc_info)
        return repr(result)

    def format(self, record):
        """ Removes all new-lines from an exception traceback """
        s = super(L2PGS_ExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', ' ')
            s = s.replace('\\n', ' ')
        return s


def setup_logging(level='INFO', stream='stdout'):
    """ Initialize the message logging components. """
    global logger

    # Setup the logging level
    logging_level = getattr(logging, level.upper())

    handler = logging.StreamHandler(getattr(sys, stream.lower()))
    formatter = L2PGS_ExceptionFormatter(fmt=('%(asctime)s.%(msecs)03d'
                                              ' %(subsystem)s'
                                              ' %(levelname)-8s'
                                              ' %(message)s'),
                                         datefmt='%Y-%m-%dT%H:%M:%S')

    handler.setFormatter(formatter)
    handler.addFilter(L2PGS_LoggingFilter())

    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.addHandler(handler)
