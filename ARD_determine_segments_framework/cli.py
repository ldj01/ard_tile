#! /usr/bin/env python
""" Commandline interface """

from argparse import ArgumentParser

import config
import framework
from util import setup_logger


def parse_cli():
    """ Parse commandline arguments

    Returns:
        dict: key/value pairs of user supplied arguments
    """
    parser = ArgumentParser(description='Launch the ARD Clip framework.')

    # Optional arguments
    parser.add_argument('-c', '--conf', dest='config_file',
                        help='configuration file')
    parser.add_argument('--disable-credentials', action='store_true',
                        dest='disable_creds', default=False,
                        help='flag to disable Mesos credential use')
    return vars(parser.parse_args())


if __name__ == '__main__':
    opts = config.read_config(**parse_cli())
    setup_logger('debug' if opts.debug else 'info')
    framework.run_forever(opts)
