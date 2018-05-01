""" Reads values from config file """

import os
import sys
import ConfigParser

from util import logger


class objdict(dict):
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError("No such attribute: " + name)


def read_config(config_file=None):
    """ Read the configuration information """

    config = ConfigParser.ConfigParser()
    options = objdict()

    # Set the configuration values.
    if config_file is None:
        config_file = os.path.expanduser('~/ARD_Clip.conf')

    if len(config.read(config_file)) == 0:
        logger.error(("Error opening config file {0}.")
                        .format(config_file))
        sys.exit(1)

    section = 'SectionOne'
    if not config.has_section(section):
        logger.error(("Error: {0} section not in config file.")
                        .format(section))
        sys.exit(1)

    if config.has_option(section, 'dbconnect'):
        options.connstr = config.get(section, 'dbconnect')
    if config.has_option(section, 'version'):
        options.version = config.get(section, 'version')
    if config.has_option(section, 'hsmstage'):
        options.hsmstage = config.getboolean(section, 'hsmstage')
    if config.has_option(section, 'soap_envelope_template'):
        options.soap_envelope = config.get(section, 'soap_envelope_template')
    if config.has_option(section, 'debug'):
        options.debug = config.getboolean(section, 'debug')
    if config.has_option(section, 'products'):
        options.products = config.get(section, 'products').split(',')
    else:
        raise ValueError('Configuration is missing "products" to process')

    return options
