#! /usr/bin/env python


"""
File: ardtile.py

Purpose: Provide command line interface to the ARDTILE system for the operators.
"""


import os
import sys
import logging
import ConfigParser
import json
import urlparse
from argparse import ArgumentParser, ArgumentTypeError, RawTextHelpFormatter
from collections import OrderedDict, namedtuple
from time import sleep
from datetime import datetime

import smtplib 
from email.mime.text import MIMEText 

import requests
import kazoo.client
import cx_Oracle


# Specify the software version
SYSTEM_VERSION = 'ARDTILE_1.0.0'

# Specify the ARD tables where we will get info from
DB_ARD_SCENES = 'ARD_PROCESSED_SCENES'
DB_ARD_TILES = 'ARD_COMPLETED_TILES'

# Specify both DB tables used for updates
L8_DB_TABLE = 'l1_albers_inventory'
L457_DB_TABLE = 'l1_albers_inventory@inv_l2_bridge_link'


logger = None


class ARDTILESystemError(Exception):
    """General system error"""
    pass


class ARDTILEMissingConfigError(ARDTILESystemError):
    """Specific to configuration errors"""
    pass


class ARDTILE_LoggingFilter(logging.Filter):
    """Forces 'ARDTILE' to be provided in the 'subsystem' tag of the log format
       string
    """

    def filter(self, record):
        """Provide the string for the 'subsystem' tag"""

        record.subsystem = 'ARDTILE'

        return True


class ARDTILE_ExceptionFormatter(logging.Formatter):
    """Modifies how exceptions are formatted
    """

    def formatException(self, exc_info):
        """Specifies how to format the exception text"""

        result = super(ARDTILE_ExceptionFormatter,
                       self).formatException(exc_info)

        return repr(result)

    def format(self, record):
        """Specifies how to format the message text if it is an exception"""

        s = super(ARDTILE_ExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', ' ')
            s = s.replace('\\n', ' ')

        return s


def setup_logging(args):
    """Configures the logging/reporting

    Args:
        args <args>: Command line arguments
    """

    global logger

    # Setup the logging level
    logging_level = logging.INFO
    if args.debug:
        logging_level = logging.DEBUG

    handler = logging.StreamHandler(sys.stdout)
    formatter = ARDTILE_ExceptionFormatter(fmt=('%(asctime)s.%(msecs)03d'
                                              ' %(subsystem)s'
                                              ' %(levelname)-8s'
                                              ' %(message)s'),
                                         datefmt='%Y-%m-%dT%H:%M:%S')

    handler.setFormatter(formatter)
    handler.addFilter(ARDTILE_LoggingFilter())

    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.addHandler(handler)

    # Disable annoying INFO messages from the requests module
    logging.getLogger('requests').setLevel(logging.WARNING)
    # Disable annoying INFO messages from the kazoo module
    logging.getLogger("kazoo").setLevel(logging.WARNING)


def determine_mesos_leader(args, cfg):
    """Use zookeeper to determine the Mesos leader

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    parsed_url = urlparse.urlparse(cfg.zookeeper, scheme='zk',
                                   allow_fragments=False)

    zk_hosts = parsed_url.netloc
    zk_path = '/mesos'

    zk = kazoo.client.KazooClient(hosts=zk_hosts)
    zk.start()

    try:
        nodes = None
        if zk.exists(zk_path):
            nodes = [node for node in zk.get_children(zk_path)
                     if node.startswith('json.info')]

        if nodes:
            nodes.sort()
            node_path = os.path.join(zk_path, nodes[0])
            (data, stat) = zk.get(node_path)

            data = json.loads(data)

            if args.debug:
                print(json.dumps(data, indent=4))
            return '{}:{}'.format(data['address']['hostname'],
                                  data['address']['port'])

    finally:
        zk.stop()

    raise ARDTILESystemError('Unable to determine Mesos Leader'
                           ' - Contact System Administrator')


def determine_marathon_leader(cfg):
    """Use zookeeper to determine the Marathon leader

    Args:
        cfg <ConfigInfo>: Configuration from the config files
    """

    parsed_url = urlparse.urlparse(cfg.zookeeper, scheme='zk',
                                   allow_fragments=False)

    zk_hosts = parsed_url.netloc
    # This is the zookeeper path to the marathon leader information
    zk_path = 'marathon/leader'

    zk = kazoo.client.KazooClient(hosts=zk_hosts)
    zk.start()

    try:
        children = zk.get_children(zk_path)
        data = zk.get('{}/{}'.format(zk_path, children[-1]))
        return data[0]

    finally:
        zk.stop()

    raise ARDTILESystemError('Unable to determine Marathon Leader'
                           ' - Contact System Administrator')


def get_marathon_task_cfg(args, cfg, task):
    """Get the Marathon configuration for the specified task

    Args:
        cfg <ConfigInfo>: Configuration from the config files
        task <TaskInfo>: TaskInfo configuration from the config files
                         for the specific task we are looking for
    """

    marathon_leader = determine_marathon_leader(cfg)

    data = None
    with requests.Session() as session:
        session.mount('http://', requests.adapters.HTTPAdapter(max_retries=1))
        session.mount('https://', requests.adapters.HTTPAdapter(max_retries=1))
        session.verify = cfg.ssl_certs

        url = 'https://{0}/v2/apps/{1}/{2}'.format(marathon_leader,
                                                   cfg.marathon.group,
                                                   task.name)
        logger.debug('Trying {}'.format(url))

        req = None
        try:
            req = session.get(url=url, auth=(cfg.marathon.user,
                                             cfg.marathon.password))

            if not req.ok:
                req.raise_for_status()

            data = json.loads(req.content)

        except Exception:
            msg = ('Unable to determine Task configuration for ({})'
                   .format(task.name))
            if args.debug:
                logger.exception(msg)
            else:
                logger.warn(msg)

        finally:
            if req is not None:
                req.close()

    return data


def update_marathon_task_cfg(args, cfg, name, state):
    """Update the specified Marathon task's configuration

    Args:
        cfg <ConfigInfo>: Configuration from the config files
        name <str>: Task name to update
        state <dict>: The state to put to the tasks endpoint
    """

    marathon_leader = determine_marathon_leader(cfg)

    with requests.Session() as session:
        session.mount('http://', requests.adapters.HTTPAdapter(max_retries=1))
        session.mount('https://', requests.adapters.HTTPAdapter(max_retries=1))
        session.verify = cfg.ssl_certs

        url = 'https://{0}/v2/apps/{1}/{2}'.format(marathon_leader,
                                                   cfg.marathon.group, name)
        logger.debug('Trying {}'.format(url))

        req = None
        try:
            req = session.put(url=url, json=state,
                              auth=(cfg.marathon.user, cfg.marathon.password))

            if not req.ok:
                req.raise_for_status()

        except Exception:
            msg = ('Unable to register/update {} Task configuration in'
                   ' Marathon'.format(name))
            if args.debug:
                logger.exception(msg)
            else:
                logger.error(msg)

        finally:
            if req is not None:
                req.close()


def specified_marathon_tasks(args, cfg):
    """Returns a list of the specified Marathon tasks

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    if args.task != 'all':
        return [f for f in cfg.marathon_tasks if args.task in f.name]
    else:
        return [f for f in cfg.marathon_tasks]


def register_marathon_tasks(args, cfg):
    """Register the tasks into Marathon

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    conf_mount = {'containerPath': os.path.join(cfg.base_config_path,
                                                'ARD_determine_segments.conf'),
                  'hostPath': cfg.config_file,
                  'mode': 'RO' }

    for task in specified_marathon_tasks(args, cfg):
        task_txt = None
        with open(task.cfg_file, 'r') as task_fd:
            task_txt = task_fd.read()

        task_cfg = json.loads(task_txt)
        task_cfg['id'] = '/{}'.format(task.name)
        task_cfg['user'] = '{}'.format(task.user)

        task_cfg['container']['volumes'].append(conf_mount)

        print('Enabling {} Task'.format(task.name))
        update_marathon_task_cfg(args, cfg, task.name, task_cfg)


def deregister_marathon_tasks(args, cfg):
    """De-Register the task from Marathon

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    marathon_leader = determine_marathon_leader(cfg)

    with requests.Session() as session:
        session.mount('http://', requests.adapters.HTTPAdapter(max_retries=1))
        session.mount('https://', requests.adapters.HTTPAdapter(max_retries=1))
        session.verify = cfg.ssl_certs

        for task in specified_marathon_tasks(args, cfg):
            url = 'https://{0}/v2/apps/{1}/{2}'.format(marathon_leader,
                                                       cfg.marathon.group,
                                                       task.name)
            logger.debug('Trying {}'.format(url))

            req = None
            try:
                print('Disabling {} Task'.format(task.name))
                req = session.delete(url=url, auth=(cfg.marathon.user,
                                                    cfg.marathon.password))

                if not req.ok:
                    req.raise_for_status()

            except Exception:
                msg = ('Unable to deregister {} Task from Marathon'
                       .format(task.name))
                if args.debug:
                    logger.exception(msg)
                else:
                    logger.error(msg)

            finally:
                if req is not None:
                    req.close()


def start_marathon_task(args, cfg, task, task_cfg):
    """Start the specified task in Marathon

    Args:
        cfg <ConfigInfo>: Configuration from the config files
        task <TaskInfo>: Task configuration for specific task
        task_cfg <dict>: Marathon task configuration
    """

    if task_cfg is not None and task_cfg['app']['tasksRunning'] >= 1:
        # Task is currently running.... tell the user
        print('{} Task has already been started'.format(task.name))

    elif task_cfg is not None and task_cfg['app']['tasksRunning'] == 0:
        # Update the running instances to 1 effectivly telling Marathon
        # to start it
        print('Starting {} Task'.format(task.name))
        update_marathon_task_cfg(args, cfg, task.name,
                                 {'instances': int(task.instances)})

    else:
        # Task not registered in Marathon.... tell the user
        print('{} Task has not been enabled'.format(task.name))


def start_marathon_tasks(args, cfg):
    """Start the tasks in Marathon

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    for task in specified_marathon_tasks(args, cfg):

        task_cfg = get_marathon_task_cfg(args, cfg, task)
        start_marathon_task(args, cfg, task, task_cfg)


def stop_marathon_task(args, cfg, task, task_cfg):
    """Stop the specified task in Marathon

    Args:
        cfg <ConfigInfo>: Configuration from the config files
        task <TaskInfo>: Task configuration for specific task
        task_cfg <dict>: Marathon task configuration
    """

    if task_cfg is not None and task_cfg['app']['tasksRunning'] >= 1:
        # Update the running instances to 0 effectivly stopping it in
        # Marathon
        update_marathon_task_cfg(args, cfg, task.name, {'instances': 0})
        print('Stopping {} Task'.format(task.name))

    elif task_cfg is not None and task_cfg['app']['tasksRunning'] == 0:
        # Task is currently not running.... tell the user
        print('{} Task has already been stopped'.format(task.name))

    else:
        # Task not registered in Marathon.... tell the user
        print('{} Task has not been enabled'.format(task.name))


def stop_marathon_tasks(args, cfg):
    """Stop the tasks in Marathon

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    for task in specified_marathon_tasks(args, cfg):

        task_cfg = get_marathon_task_cfg(args, cfg, task)
        stop_marathon_task(args, cfg, task, task_cfg)


HOURS_IN_DAY = 24.0
MINUTES_IN_DAY = 1440.0


def argument_validation_last(s):
    """Validates and returns a values in days for the 'last' command line
       argument

    Args:
        s <str>: The command line value to verify

    Returns:
        <int>: Validated value from the command line as days
    """

    if s.lower().endswith('day'):
        return float(s.lower().replace('day', ''))
    elif s.lower().endswith('days'):
        return float(s.lower().replace('days', ''))
    elif s.lower().endswith('hr'):
        return float(s.lower().replace('hr', '')) / HOURS_IN_DAY
    elif s.lower().endswith('hrs'):
        return float(s.lower().replace('hrs', '')) / HOURS_IN_DAY
    elif s.lower().endswith('min'):
        return float(s.lower().replace('min', '')) / MINUTES_IN_DAY
    elif s.lower().endswith('mins'):
        return float(s.lower().replace('mins', '')) / MINUTES_IN_DAY
    else:
        raise ArgumentTypeError('Invalid time format: Examples: 1min(s),'
                                ' 2hr(s), 3day(s)')


SECONDS_IN_MINUTE = 60.0


def argument_validation_watch(s):
    """Validates and returns a values in seconds for the 'watch' command line
       argument

    Args:
        s <str>: The command line value to verify

    Returns:
        <float>: Validated value from the command line as seconds
    """

    if s.lower().endswith('min'):
        return float(s.lower().replace('min', '')) * SECONDS_IN_MINUTE
    elif s.lower().endswith('mins'):
        return float(s.lower().replace('mins', '')) * SECONDS_IN_MINUTE
    elif s.lower().endswith('sec'):
        return float(s.lower().replace('sec', ''))
    elif s.lower().endswith('secs'):
        return float(s.lower().replace('secs', ''))
    else:
        raise ArgumentTypeError('Invalid time format:'
                                ' Examples: 3sec(s), 2min(s)')


def argument_validation_last_days(s):
    """Validates and returns a values in seconds for the 'watch' command line
       argument

    Args:
        s <str>: The command line value to verify

    Returns:
        <int>: Validated value from the command line as days
    """

    if s.lower().endswith('day'):
        return int(s.lower().replace('day', ''))
    elif s.lower().endswith('days'):
        return int(s.lower().replace('days', ''))
    else:
        raise ArgumentTypeError('Invalid time format: Example: 3day(s)')


def get_command_line_arguments():
    """Setup and return the command line options

    Returns:
        <args>: Command line arguments
    """

    description = ('Provides command line interaction with the ARDTILE system.')
    parser = ArgumentParser(description=description)

    parser.add_argument('--version',
                        action='version',
                        version=SYSTEM_VERSION)

    parser.add_argument('--debug',
                        action='store_true',
                        dest='debug',
                        default=False,
                        help='display error information')

    subparsers = parser.add_subparsers(dest='subcommand')

    # ---------------------------------
    description = 'Provides system control'
    sub_p = subparsers.add_parser('systemctl',
                                  description=description,
                                  help=description)

    sub_p.add_argument('--task',
                       action='store',
                       dest='task',
                       default='all',
                       choices=['all'],
                       help='specify the Marathon task')

    group = sub_p.add_mutually_exclusive_group(required=True)

    group.add_argument('--enable',
                       action='store_true',
                       dest='enable',
                       default=False,
                       help='enable ARDTILE processing')

    group.add_argument('--disable',
                       action='store_true',
                       dest='disable',
                       default=False,
                       help='disable ARDTILE processing')

    group.add_argument('--start',
                       action='store_true',
                       dest='start',
                       default=False,
                       help='start ARDTILE processing')

    group.add_argument('--stop',
                       action='store_true',
                       dest='stop',
                       default=False,
                       help='stop ARDTILE processing')

    # ---------------------------------
    description = 'Displays the status of the system'
    sub_p = subparsers.add_parser('status',
                                  description=description,
                                  formatter_class=RawTextHelpFormatter,
                                  help=description)

    sub_p.add_argument('--all',
                       action='store_true',
                       dest='all',
                       default=False,
                       help='display all information')

    sub_p.add_argument('--tasks',
                       action='store_true',
                       dest='tasks',
                       default=False,
                       help='display Marathon task information')

    sub_p.add_argument('--processing',
                       action='store_true',
                       dest='processing',
                       default=False,
                       help='display processing information')

    sub_p.add_argument('--ready',
                       action='store_true',
                       dest='ready',
                       default=False,
                       help='display ready information')

    sub_p.add_argument('--reinit',
                       action='store_true',
                       dest='reinit',
                       default=False,
                       help='display reinitialized information')

    sub_p.add_argument('--remaining',
                       action='store_true',
                       dest='remaining',
                       default=False,
                       help='display remaining information')

    sub_p.add_argument('--success',
                       action='store_true',
                       dest='success',
                       default=False,
                       help='display scene success information')

    sub_p.add_argument('--tsuccess',
                       action='store_true',
                       dest='tsuccess',
                       default=False,
                       help='display tile success information')

    sub_p.add_argument('--last',
                       action='store',
                       dest='last',
                       metavar='TIME',
                       type=argument_validation_last,
                       help='display information for the last'
                            '\nExamples: 1min(s), 2hr(s), 3day(s)')

    sub_p.add_argument('--watch',
                       action='store',
                       dest='watch',
                       metavar='TIME',
                       type=argument_validation_watch,
                       help='continually display information'
                            '\nExamples: 3sec(s), 2min(s)')

    # ---------------------------------
    description = 'Generates detailed reports for the system'
    sub_p = subparsers.add_parser('report',
                                  description=description,
                                  formatter_class=RawTextHelpFormatter,
                                  help=description)

    sub_p.add_argument('--emails',
                       action='store',
                       dest='emails',
                       metavar='EMAIL_LIST',
                       help='list of emails to send the report to'
                            '\nExample: e1@junk.org,e2@junk.org')

    sub_p.add_argument('--last',
                       action='store',
                       dest='last',
                       metavar='TIME',
                       type=argument_validation_last_days,
                       help='display information for the last days'
                            '\nExample: 3day(s)')

    # ---------------------------------
#    description = 'Allows changing a Landsat Product IDs state'
#    sub_p = subparsers.add_parser('productctl',
#                                  description=description,
#                                  help=description)

#    sub_p.add_argument('--product-id',
#                       action='store',
#                       dest='product_id',
#                       required=True,
#                       metavar='PRODUCT_ID',
#                       help='specifies the Landsat Product ID to modify')

#    group = sub_p.add_mutually_exclusive_group(required=True)

#    group.add_argument('--ready',
#                       action='store_true',
#                       dest='ready',
#                       default=False,
#                       help='sets the specified PRODUCT_ID to ready state')

#    group.add_argument('--hold',
#                       action='store_true',
#                       dest='hold',
#                       default=False,
#                       help='sets the specified PRODUCT_ID to hold state')

#    group.add_argument('--error',
#                       action='store_true',
#                       dest='error',
#                       default=False,
#                       help='sets the specified PRODUCT_ID to error state')

    # ---------------------------------
    description = 'Lists the Product IDs in a specific status'
    sub_p = subparsers.add_parser('list',
                                  description=description,
                                  formatter_class=RawTextHelpFormatter,
                                  help=description)

    group = sub_p.add_mutually_exclusive_group(required=True)

    group.add_argument('--processing',
                       action='store_true',
                       dest='processing',
                       default=False,
                       help='display Product IDs in the PROCESSING state')

    group.add_argument('--ready',
                       action='store_true',
                       dest='ready',
                       default=False,
                       help='display Product IDs in the READY state')

    group.add_argument('--reinit',
                       action='store_true',
                       dest='reinit',
                       default=False,
                       help='display Product IDs in the REINITLZD state')

    # ---------------------------------
    description = 'Displays the Mesos and Marathon leaders'
    sub_p = subparsers.add_parser('leaders',
                                  description=description,
                                  formatter_class=RawTextHelpFormatter,
                                  help=description)

    args = parser.parse_args()

    return args


def systemctl(args, cfg):
    """Control whether the system is enabled and running

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    if args.enable:
        register_marathon_tasks(args, cfg)

    elif args.disable:
        deregister_marathon_tasks(args, cfg)

    elif args.start:
        start_marathon_tasks(args, cfg)

    elif args.stop:
        stop_marathon_tasks(args, cfg)


TASK_COLORS = {
    'No': '\x1b[1m\x1b[38;5;124mNo\x1b[0m',
    'Yes': '\x1b[38;5;47mYes\x1b[0m',
    'SHUTDOWN': '\x1b[1m\x1b[38;5;124mSHUTDOWN\x1b[0m',
    'TASK_LOST': '\xb1[1m\x1b[38;5;124mTASK_LOST\x1b[0m',
    'TASK_KILLED': '\xb1[1m\x1b[38;5;124mTASK_KILLED\x1b[0m',
    'TASK_ERROR': '\xb1[1m\x1b[38;5;124mTASK_ERROR\x1b[0m',
    'TASK_FAILED': '\xb1[1m\x1b[38;5;124mTASK_FAILED\x1b[0m',
    'TASK_RUNNING': '\x1b[38;5;47mTASK_RUNNING\x1b[0m',
    'TASK_FINISHED': '\x1b[38;5;47mTASK_FINISHED\x1b[0m',
    'TASK_STARTING': '\x1b[38;5;228mTASK_STARTING\x1b[0m',
    'TASK_STAGING': '\x1b[38;5;228mTASK_STAGING\x1b[0m'
}
STATUS_FMT = '{0:>10}: {1:>11}'
STATUS_FMT_RED = '\x1b[1m\x1b[38;5;124m{}\x1b[0m'.format(STATUS_FMT)
STATUS_FMT_GREEN = '\x1b[38;5;47m{}\x1b[0m'.format(STATUS_FMT)
STATUS_DATE_GREEN = '\x1b[38;5;47m{:>60}\x1b[0m'
HEADING_BLUE = '\x1b[38;5;21m{}\x1b[0m'


def status(args, cfg):
    """Present a quick status of the system
    """

    results = OrderedDict()
    find_all = False

    # Determine if we need to report all of the statuses or just specific
    # ones
    if args.all or (not args.success and
                    not args.ready and
                    not args.reinit and
                    not args.processing and
                    not args.remaining and
                    not args.tsuccess and
                    not args.tasks):
        find_all = True

    try:
        done = False
        while not done:
            # Output the current time for operator logging
            print('-'*60)
            print(STATUS_DATE_GREEN.format(str(datetime.now())))
            print('-'*60)
            if find_all or args.success or args.ready or args.reinit or args.processing or args.remaining:
                print(HEADING_BLUE.format('Product ID STATUS'))

            db_con = cx_Oracle.connect(cfg.ard_db_connect)
            try:
                db_cur = db_con.cursor()
                try:
                    base_stmt = ("select count(*)"
                                 " from {0}"
                                 " where {1} processing_state = '{2}'")

                    date_range = ""
                    date_range2 = ""
                    if args.last:
                        # Confine results to the last ???? (days, hrs, or mins)
                        date_range = ("DATE_PROCESSED > sysdate-{} and"
                                      .format(args.last))
                        date_range2 = ("DATE_COMPLETED > sysdate-{} and"
                                      .format(args.last))

                    if find_all or args.success:
                        select_stmt = base_stmt.format(DB_ARD_SCENES, date_range,
                                                       'COMPLETE')
                        db_cur.execute(select_stmt)
                        value = int(db_cur.fetchone()[0])
                        print(STATUS_FMT.format('SUCCESS', value))

                    if find_all or args.ready:
                        select_stmt = base_stmt.format(DB_ARD_SCENES, date_range,
                                                       'INQUEUE')
                        db_cur.execute(select_stmt)
                        value = int(db_cur.fetchone()[0])
                        print(STATUS_FMT.format('READY', value))

                    if find_all or args.processing:
                        select_stmt = base_stmt.format(DB_ARD_SCENES, date_range,
                                                       'INWORK')
                        db_cur.execute(select_stmt)
                        value = int(db_cur.fetchone()[0])
                        print(STATUS_FMT.format('PROCESSING', value))

                    if find_all or args.reinit:
                        select_stmt = base_stmt.format(DB_ARD_SCENES, date_range,
                                                       'BLANK')
                        db_cur.execute(select_stmt)
                        value = int(db_cur.fetchone()[0])
                        print(STATUS_FMT.format('REINITLZD', value))

                    if find_all or args.remaining:
                        select_stmt = "select count(*) from ARD_UNPROCESSED_SCENES_V"

                        beginloc = cfg.segment_query.find("1001")
                        endloc = cfg.segment_query.lower().find("order")
                        if beginloc > -1 and endloc > -1:
                           where_clause = cfg.segment_query[beginloc+8:endloc]
                           select_stmt = select_stmt + " where " + where_clause

                        logger.debug(select_stmt)

                        db_cur.execute(select_stmt)
                        value = int(db_cur.fetchone()[0])
                        print(STATUS_FMT.format('REMAINING', value))

                    if find_all or args.tsuccess:
                        print
                        print(HEADING_BLUE.format('TILE STATUS'))
                        select_stmt = base_stmt.format(DB_ARD_TILES, date_range2,
                                                       'SUCCESS')
                        db_cur.execute(select_stmt)
                        value = int(db_cur.fetchone()[0])
                        print(STATUS_FMT.format('SUCCESS', value))

#                    select_stmt = base_stmt.format(DB_ARD_TILES, date_range2,
#                                                       'NOT NEEDED')
#                    db_cur.execute(select_stmt)
#                    value = int(db_cur.fetchone()[0])
#                    print(STATUS_FMT.format('NOT NEEDED', value))

                finally:
                    db_cur.close()

            finally:
                db_con.close()

            # Task section
            if find_all or args.tasks:

                for task in cfg.marathon_tasks:
                    task_cfg = get_marathon_task_cfg(args, cfg, task)

                    task_id = 'NA'
                    task_state = 'SHUTDOWN'
                    task_host = 'NA'

                    if (task_cfg is not None and
                            len(task_cfg['app']['tasks']) > 0):

                        task_id = task_cfg['app']['tasks'][0]['id']
                        task_state = task_cfg['app']['tasks'][0]['state']
                        task_host = task_cfg['app']['tasks'][0]['host']

                    task_enabled = 'No'
                    if task_cfg is not None:
                        task_enabled = 'Yes'

                    print
                    print ('Marathon Task {0} Status:\n'
                           '  Task Enabled => {1}\n'
                           '    Mesos Task => {2}\n'
                           '         State => {3}\n'
                           '          Host => {4}'
                           .format(task.name, TASK_COLORS[task_enabled],
                                   task_id, TASK_COLORS[task_state],
                                   task_host))

            if args.watch > 0:
                done = False
                sleep(args.watch)
            else:
                done = True

    except KeyboardInterrupt:
        pass


def build_report(args, cfg):
    """Build a report to be emailed
    """
    report = ""

    # Output the current time for operator logging
    report += '-'*60 + '\n'
    report += 'Scenes and Tiles Processed in the last ' + str(args.last) + ' day(s)\n'
    report += '-'*60 + '\n\n'


    report += 'SCENE STATUS' + '\n'
    report += '------------' + '\n'

    db_con = cx_Oracle.connect(cfg.ard_db_connect)
    try:
        db_cur = db_con.cursor()
        try:
            base_stmt = ("select count(*)"
                         " from {0}"
                         " where {1} processing_state = '{2}'")

            date_range = ""
            date_range2 = ""
            if args.last:
                # Confine results to the last ???? (days, hrs, or mins)
                date_range = ("DATE_PROCESSED > sysdate-{} and"
                              .format(args.last))
                date_range2 = ("DATE_COMPLETED > sysdate-{} and"
                              .format(args.last))

            select_stmt = base_stmt.format(DB_ARD_SCENES, date_range,
                                           'COMPLETE')
            db_cur.execute(select_stmt)
            value = int(db_cur.fetchone()[0])
            report += 'SUCCESS:  ' + str(value) + '\n'

            select_stmt = base_stmt.format(DB_ARD_SCENES, date_range,
                                           'INQUEUE')
            db_cur.execute(select_stmt)
            value = int(db_cur.fetchone()[0])
            report += 'READY:  ' + str(value) + '\n'

            select_stmt = base_stmt.format(DB_ARD_SCENES, date_range,
                                           'INWORK')
            db_cur.execute(select_stmt)
            value = int(db_cur.fetchone()[0])
            report += 'PROCESSING:  ' + str(value) + '\n'

            select_stmt = base_stmt.format(DB_ARD_SCENES, date_range,
                                           'BLANK')
            db_cur.execute(select_stmt)
            value = int(db_cur.fetchone()[0])
            report += 'REINITLZD:  ' + str(value) + '\n'

            select_stmt = "select count(*) from ARD_UNPROCESSED_SCENES_V"

            beginloc = cfg.segment_query.find("1001")
            endloc = cfg.segment_query.lower().find("order")
            if beginloc > -1 and endloc > -1:
               where_clause = cfg.segment_query[beginloc+8:endloc]
               select_stmt = select_stmt + " where " + where_clause

            logger.debug(select_stmt)

            db_cur.execute(select_stmt)
            value = int(db_cur.fetchone()[0])
            report += 'REMAINING:  ' + str(value) + '\n\n'

            print
            report += 'TILE STATUS' + '\n'
            report += '-----------' + '\n'
            select_stmt = base_stmt.format(DB_ARD_TILES, date_range2,
                                           'SUCCESS')
            db_cur.execute(select_stmt)
            value = int(db_cur.fetchone()[0])
            report += 'SUCCESS:  ' + str(value) + '\n'
        finally:
            db_cur.close()

    finally:
        db_con.close()

    report += '-'*60 + '\n\n'

    return report

def report(args, cfg):
    """Generate reports

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """
    if not args.emails:
        print "You must enter at least one email address to send the report to"
        return

    if not args.last:
        args.last = 1   # 1 = 1 day

    print "Creating report"
    report = build_report(args, cfg)
    print "Finished creating report"

    msg = MIMEText(report)

    fromAddr = 'ardtile@usgs.gov'

    toAddr = args.emails.split(",")
    msg['Subject'] = 'Daily ARD Report'
    msg['From'] = fromAddr

    print "Sending report to " + args.emails
    # Send the message via our own SMTP server
    SMTP_host = "smtp.usgs.gov"
    s = smtplib.SMTP(SMTP_host)
    msg['To'] = args.emails
    s.sendmail(fromAddr, toAddr, msg.as_string())

    s.quit() 
    print "Report has been sent"

def get_db_table(db_cur, product_id):
    """Return the table and view to use based on what the product_id is

    Args:
        db_cur <cursor>: Database cursor to use
        product_id <str>: Landsat product ID

    Returns:
        <str>: Database table to use
    """

    select_stmt = ("select satellite"
                   " from {0}"
                   " where landsat_product_id_albers = '{1}'"
                   .format(DB_ARD_SCENES, product_id))

    db_cur.execute(select_stmt)
    value = int(db_cur.fetchone()[0])

    if value == 8:
        return L8_DB_TABLE
    else:
        return L457_DB_TABLE


def productctl(args, cfg):
    """Modification of a Landsat Product IDs state

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    db_con = cx_Oracle.connect(cfg.ard_db_connect)
    try:
        db_cur = db_con.cursor()
        try:

            db_table = get_db_table(db_cur, args.product_id)

            update_tmp = ("update {0}"
                          " set processing_state = {1}{2}"
                          " where landsat_product_id_albers = '{3}'"
                              " and processing_state not in ({4})")

            state = "'ERROR'"
            retries = ''
            not_states = "'PROCESSING'"
            if args.ready:
                state = "'READY'"
                retries = ', processing_retries = 0'
            elif args.hold:
                state = "'HOLD'"

            update_stmt = update_tmp.format(db_table, state, retries,
                                            args.product_id, not_states)

            logger.debug(update_stmt)
            db_cur.execute(update_stmt)

            if db_cur.rowcount != 1:
                logger.warn('No database rows updated')

            db_con.commit()

        finally:
            db_cur.close()

    finally:
        db_con.close()


def list_ids(args, cfg):
    """Lists the Product IDs in the specified status

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    db_con = cx_Oracle.connect(cfg.ard_db_connect)
    try:
        db_cur = db_con.cursor()
        try:

            l47_select = ("select scene_id "
                          " from {0}"
                          " where processing_state = '{1}'")

            if args.processing:
                select_stmt = l47_select.format(DB_ARD_SCENES, 'INWORK')
                db_cur.execute(select_stmt)

            elif args.ready:
                select_stmt = l47_select.format(DB_ARD_SCENES, 'INQUEUE')
                db_cur.execute(select_stmt)

            else:
                select_stmt = l47_select.format(DB_ARD_SCENES, 'BLANK')
                db_cur.execute(select_stmt)

            found = False
            for result in db_cur.fetchall():
                dd = [str(x) for x in result]
                print('  '.join(dd))
                found = True

            if not found:
                print("None in requested status")

        finally:
            db_cur.close()

    finally:
        db_con.close()


def leaders(args, cfg):
    """Displays the Mesos and/or Marathon leaders

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    mesos_leader = determine_mesos_leader(args, cfg)
    marathon_leader = determine_marathon_leader(cfg)

    print 'Mesos Leader: https://{}'.format(mesos_leader)
    print 'Marathon Leader: https://{}'.format(marathon_leader)


# Task information structure
TaskInfo = namedtuple('TaskInfo', ('name',
                                   'user',
                                   'cfg_file',
                                   'instances'))

# Marathon information structure
MarathonInfo = namedtuple('MarathonInfo', ('user',
                                           'password',
                                           'group'))

# Configuration information structure
ConfigInfo = namedtuple('ConfigInfo', ('ard_mode',
                                       'base_config_path',
                                       'config_file',
                                       'ard_db_connect',
                                       'zookeeper',
                                       'ssl_certs',
                                       'segment_query',
                                       'marathon',
                                       'marathon_tasks'))


def read_configuration():
    """Reads configuration from the config file and returns it

    Returns:
        <ConfigInfo>: Populated with configuration read from the config file
    """

    # A dev directory should never exist in it, st, and ops configurations
    ard_mode = os.environ.get('ARDTILE_MODE', 'dev')

    base_config_path = '/usr/local/usgs/ard_tile'

    config_file = os.path.join(base_config_path, ard_mode, 'ARD_determine_segments.conf')

    if not os.path.isfile(config_file):
        raise ARDTILEMissingConfigError('Missing {}'.format(config_file))

    cfg = ConfigParser.ConfigParser()
    cfg.read(config_file)

    ard_section = 'ard'
    marathon_section = 'marathon'
    pgs_task_section = 'pgs_task'

    marathon_group = '-'.join([cfg.get(marathon_section, 'group'), ard_mode])
    marathon_info = MarathonInfo(user=cfg.get(marathon_section, 'user'),
                                 password=cfg.get(marathon_section, 'password'),
                                 group=marathon_group)

    pgs_cfg_path = os.path.join(base_config_path,
                                cfg.get(pgs_task_section, 'cfg_file'))

    pgs_task = TaskInfo(name=cfg.get(pgs_task_section, 'name'),
                        user=cfg.get(pgs_task_section, 'user'),
                        cfg_file=pgs_cfg_path,
                        instances=cfg.get(pgs_task_section, 'instances'))

    return ConfigInfo(ard_mode=ard_mode,
                      base_config_path=base_config_path,
                      config_file=config_file,
                      ard_db_connect=cfg.get(ard_section, 'dbconnect'),
                      zookeeper=cfg.get(ard_section, 'zookeeper'),
                      segment_query=cfg.get(ard_section, 'segment_query'),
                      ssl_certs=cfg.get(ard_section, 'ssl_certs'),
                      marathon=marathon_info,
                      marathon_tasks=[pgs_task])


def main():
    """Main processing, provides setup
    """

    # Determine the command and arguments
    args = get_command_line_arguments()

    setup_logging(args)

    cfg = read_configuration()

    # Setup the mapping
    cac = dict()
    cac['systemctl'] = systemctl
    cac['report'] = report
    cac['status'] = status
    #cac['productctl'] = productctl
    cac['list'] = list_ids
    cac['leaders'] = leaders

    # Call the specified subcommand
    cac[args.subcommand](args, cfg)


if __name__ == '__main__':

    try:
        main()
    except Exception:
        if logger is not None:
            logger.exception('EXCEPTION ')
