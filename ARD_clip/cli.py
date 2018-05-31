#! /usr/bin/env python
"""Commandline entrypoint expected for Mesos tasks."""
import json
from argparse import ArgumentParser

import config
from util import setup_logger, logger
from ARD_Clip import process_segments


def parse_cli():
    """Parse supplied command line arguments."""
    parser = ArgumentParser()
    parser.add_argument(action="store", dest='segment',
                        type=json.loads, metavar='JSON')
    parser.add_argument(action="store", dest='output_path',
                        type=str, metavar='PATH')
    parser.add_argument('-c', '--config', action="store", dest='config_file',
                        required=False, type=str, metavar='PATH')
    return vars(parser.parse_args())


if __name__ == '__main__':
    setup_logger()

    args = parse_cli()
    conf = config.read_config(args['config_file'])
    setup_logger(level='debug' if conf.debug else 'info')

    logger.info('******************Start************')
    logger.info('             DB connection: %s', conf.connstr)
    logger.info("             Version: %s", conf.version)
    logger.info("             Debug: %s", conf.debug)
    logger.info('segment: %s', args['segment'])
    logger.info('output path: %s', args['output_path'])

    process_segments(args['segment'], args['output_path'], conf)

    logger.info('..................Normal End............')
