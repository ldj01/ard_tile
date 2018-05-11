""" Reads values from config file """

import os
import sys
import ConfigParser

import yaml

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
        options.version = config.getint(section, 'version')
    if config.has_option(section, 'collection'):
        options.collection = config.getint(section, 'collection')
    if config.has_option(section, 'hsmstage'):
        options.hsmstage = config.getboolean(section, 'hsmstage')
    if config.has_option(section, 'soap_envelope_template'):
        options.soap_envelope = config.get(section, 'soap_envelope_template')
    if config.has_option(section, 'debug'):
        options.debug = config.getboolean(section, 'debug')
    if config.has_option(section, 'workdir'):
        options.workdir = config.get(section, 'workdir')
    if config.has_option(section, 'minscenespertile'):
        options.minscenespertile = config.getint(section, 'minscenespertile')
    if config.has_option(section, 'maxscenespertile'):
        options.maxscenespertile = config.getint(section, 'maxscenespertile')
    if config.has_option(section, 'products'):
        options.products = config.get(section, 'products').split(',')
    else:
        raise ValueError('Configuration is missing "products" to process')

    return options


def read_processing_config(sensor, filename=None):
    """ Read ARD Tile data processing options

    Args:
        sensor (str): Top-level name for current options
        filename (str): path to yaml configuration

    Returns:
        dict: options subset by specified sensor
    """
    if filename is None:
        filename = os.getenv('ARD_YAML_PATH')
    if not os.path.exists(filename):
        raise IOError("Cannot open processing configuration %s" % filename)
    conf = yaml.load(open(filename))
    if sensor not in conf:
        raise ValueError('Sensor not found: %s' % sensor)
    return conf[sensor]


def datatype_searches(conf, band_name):
    """ Fetch list of data of a common format (nodata, bytesize/bytetype, ...)

    Args:
        conf (dict): raw structure from `read_processing_config`
        band_name (str): product listed under 'datatype'

    Returns:
        str,int: datatype for given band_name

    Example:
        >>> datatype_searches(conf, 'toa_band1')
        1
    """
    return [d for d, l in conf['datatype'].items() if band_name in l].pop()


def determine_output_products(conf, product):
    """ Get the output name mapping for a given product

    Args:
        conf (dict): raw structure from `read_processing_config`
        product (str): product listed under 'package'

    Returns:
        dict: output input-to-output name mapping

    Example:
        >>> determine_output_products(conf, 'TA')
        {'toa_band1': 'TAB1', ...}
    """
    if product not in conf['package']:
        raise ValueError('Product not found: %s' % product)
    return {k:v for k,v in conf['rename'].items()
            if v in conf['package'][product]}
