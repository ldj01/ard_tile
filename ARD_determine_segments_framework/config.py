""" Read configuration from CLI or config file """

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


def read_config(config_file=None, disable_creds=False):
    """ Read the configuration information """

    config = ConfigParser.ConfigParser()
    options = objdict()


    # Set the configuration values.
    if config_file is None:
        config_file = os.path.expanduser('~/ARD_determine_segments.conf')

    # Set the configuration values.
    #config_file = 'ARD_determine_segments.conf'
    options.config_file = config_file
    options.disable_creds = disable_creds

    if len(config.read(config_file)) == 0:
        logger.error(("Error opening config file {0}.")
                        .format(config_file))
        sys.exit(1)

    section = 'ard'
    if not config.has_section(section):
        logger.error(("Error: {0} section not in config file.")
                        .format(section))
        sys.exit(1)


    options.l2_db_con = config.get(section, 'dbconnect')
    options.minscenesperseg = config.getint(section, 'minscenespersegment')
    options.master = config.get(section, 'zookeeper')
    options.docker_pkg = config.get(section, 'docker_pkg')
    options.segment_query = config.get(section, 'segment_query')
    options.satellite = config.get(section, 'satellite')

    if not disable_creds:
        section = 'mesos'
        if not config.has_section(section):
            logger.error(("Error: {0} section not in config file.")
                        .format(section))
            sys.exit(1)
        options.mesos_principal = config.get(section, 'principal')
        options.mesos_secret = config.get(section, 'secret')
        options.mesos_role = config.get(section, 'role')
    else:
        options.mesos_principal = ""
        options.mesos_secret = ""
        options.mesos_role = ""

    section = 'pgs_framework'
    if not config.has_section(section):
        logger.error(("Error: {0} section not in config file.")
                        .format(section))
        sys.exit(1)
    options.framework_user = config.get(section, 'framework_user')
    options.input_method = config.get(section, 'input_method')
    if options.input_method == 'http':
        options.input_url = config.get(section, 'input_url')
    options.max_orders = config.getint(section, 'max_orders')
    options.max_jobs = config.getint(section, 'max_jobs')
    options.max_failed_jobs = config.getint(section, 'max_failed_jobs')
    options.max_retries = config.getint(section, 'max_retries')
    options.retry_interval = config.getint(section, 'retry_interval')
    options.cpus = config.getint(section, 'req_cpus')
    options.memory = config.getint(section, 'req_mem_mb')
    options.disk = config.getint(section, 'req_disk_mb')

    section = 'ardclip'
    if not config.has_section(section):
        logger.error(("Error: {0} section not in config file.")
                        .format(section))
        sys.exit(1)
    options.confdir = config.get(section, 'ard_conf_dir')
    options.indir = config.get(section, 'base_input_dir')
    options.outdir = config.get(section, 'base_output_dir')
    options.auxdir = config.get(section, 'base_aux_dir')
    options.container_name = config.get(section, 'container')
    options.container_user = config.get(section, 'internal_user_id')
    options.container_group = config.get(section, 'internal_group_id')

    return options
