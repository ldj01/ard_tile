#! /usr/bin/env python

import json
from argparse import ArgumentParser

import config
from util import setup_logger, logger
from ARD_Clip import process_segments


def parse_cli():
    """ Command line argument parsing """

    parser = ArgumentParser()
    parser.add_argument(action="store", dest='segment', required=True,
                        type=json.loads, metavar='JSON')
    parser.add_argument(action="store", dest='output_path', required=True,
                        type=str, metavar='PATH')
    parser.add_argument('-c', '--config', action="store", dest='config_file', required=False,
                        type=str, metavar='PATH')

    return vars(parser.parse_args())


if __name__ == '__main__':
    setup_logger()

    args = parse_cli()
    conf = config.read_config(args['config_file'])
    setup_logger(level='debug' if conf.debug else 'info')

    logger.info('******************Start************')
    logger.info('             DB connection: {0}'.format(conf.connstr))
    logger.info("             Version: {0}".format(conf.version))
    logger.info("             Debug: {0}".format(conf.debug))
    logger.info('segment: {0}'.format(args['segment']))
    logger.info('output path: {0}'.format(args['output_path']))

    process_segments(args['segment'], args['output_path'], conf)

    logger.info('..................Normal End............')
