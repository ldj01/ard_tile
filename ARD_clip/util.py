""" Utilities for interacting with host system """

import os
import sys
import glob
import stat
import shlex
import shutil
import hashlib
import logging
import tarfile
import datetime
import subprocess


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


def setup_logger(level='INFO', stream='stdout'):
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


def watch_stdout(cmd):
    """ Combine stdout/stderr, read output in real time, return execution results

    Args:
        cmd (list): command and arguments to execute

    Returns:
        dict: exit status code and text output stream from stdout
    """
    logging.debug('Execute: %s', cmd)
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)
    output = []
    for line in iter(process.stdout.readline, b''):
        logging.debug(line.strip())
        output.append(line.strip())
        if process.poll() is not None:
            break
    process.stdout.close()
    process.wait()
    return {
        'cmd': cmd,
        'status': process.returncode,
        'output': output
    }


def execute_cmd(cmd, workdir=None):
    """ Execute a system command line call, raise error on non-zero exit codes

    Args:
        cmd (str): The command line to execute.
        workdir (str): path of directory to perform work in

    Returns:
        dict: exit status code and text output stream from stdout
    """
    current_dir = os.getcwd()
    if os.path.isdir(workdir or ''):
        os.chdir(workdir)

    cparts = cmd
    if not isinstance(cmd, (tuple, list)):
        cparts = shlex.split(cmd)
    results = watch_stdout(cparts)

    message = None
    if results['status'] < 0:
        message = 'Application terminated by signal [{}]'.format(cmd)

    if results['status'] != 0:
        message = 'Application failed to execute [{}]'.format(cmd)

    if os.WEXITSTATUS(results['status']) != 0:
        message = ('Application [{}] returned error code [{}]'
                   .format(cmd, os.WEXITSTATUS(results['status'])))

    os.chdir(current_dir)
    if message is not None:
        logging.error('%s Stdout/Stderr is: %s', message, results['output'])

    return results


def make_file_group_writeable(filename):
    """  Make file group wrietable """
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IWGRP)


def checksum_md5(filename):
    """ Calculate the MD5 hex digest of input filename

    Args:
        filename (str): path to file to digest

    Returns:
        str: md5 checksum
    """
    return hashlib.md5(open(filename, 'rb').read()).hexdigest()


def process_checksums(indir='.', filext="*.tar", outdir='.'):
    """ Create md5 checksum files for all matching file types

    Args:
        indir (str): path to find files
        filext (str): file extension glob to search for
        outdir (str): path to write output files

    Returns:
        str: status of operation [SUCCESS, ERROR]
    """
    fullnames = glob.glob(os.path.join(indir, filext))
    for fullname in fullnames:
        basename = os.path.basename(fullname)
        md5name = os.path.join(outdir, basename.replace('.tar', '.md5'))
        md5hash = checksum_md5(fullname)
        with open(md5name) as fid:
            fid.write(' '.join([md5hash, basename]))
        make_file_group_writeable(md5name)


def get_production_timestamp():
    """ Create a string containing the current UTC date/time in ISO 8601 format """
    return datetime.datetime.utcnow().strftime(r'%Y-%m-%dT%H:%M:%SZ')


def tar_archive(output_filename, files):
    """ Combine files as single-layer tar archive

    Args:
        output_filename (str): path to write tar archive
        files (list): full paths to files to add to archive
    """

    if len(files) < 1:
        raise ValueError("No files to archive, cannot create %s" % output_filename)

    with tarfile.open(output_filename, "w") as tar:
        for filename in files:
            tar.add(filename, arcname=os.path.basename(filename))

    make_file_group_writeable(output_filename)


def untar_archive(filename, rootdir='.'):
    """ Extracts a tar.gz file into directory location, using basename as new folder name

    Args:
        filename (str): path to tar.gz archive
        rootdir (str): path to base directory to extract files

    Returns:
        str: path to new created directory
    """
    directory = os.path.join(rootdir, os.path.basename(filename).split('.')[0])
    if make_dirs(directory):
        logger.info('Unpacking tar: %s', filename)
        tar = tarfile.open(filename, 'r:gz')
        tar.extractall(directory)
        tar.close()
        logger.info('End unpacking tar')
    return directory


def make_dirs(directory):
    """ Create a directory if it does not already exist """
    if not os.path.isdir(directory):
        logger.info('Create new directory: %s', directory)
        os.makedirs(directory)
        return True
    else:
        logger.debug('Directory already exists: %s', directory)
        return False
