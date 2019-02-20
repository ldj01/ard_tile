"""Utilities for interacting with host system."""

import os
import sys
import glob
import stat
import shlex
import shutil
import hashlib
import logging
import tarfile
import subprocess


logger = logging.getLogger()


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

    logger.handlers = []
    logger.setLevel(logging_level)
    logger.addHandler(handler)


def watch_stdout(cmd):
    """Read combined stdout/stderr in real time and return results.

    Args:
        cmd (list): command and arguments to execute

    Returns:
        dict: exit status code and text output stream from stdout

    """
    logging.debug('Execute: %s', cmd)
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, bufsize=0)
    output = []
    for line in iter(process.stdout.readline, b''):
        logging.debug(line.strip())
        output.append(line.strip())
    return_code = process.wait()
    return {
        'cmd': cmd,
        'status': return_code,
        'output': output,
    }


def execute_cmd(cmd, workdir=None):
    """Execute a system command line call, raise error on non-zero exit

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
    """ Make permissions on file group wrietable."""
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IWGRP)


def checksum_md5(filename):
    """Calculate the MD5 hex digest of input filename.

    Args:
        filename (str): path to file to digest

    Returns:
        str: md5 checksum

    """
    return hashlib.md5(open(filename, 'rb').read()).hexdigest()


def process_checksums(globext="*.tar"):
    """Create md5 checksum files for all matching file types.

    Args:
        globext (str): file glob to search for

    Returns:
        str: status of operation [SUCCESS, ERROR]

    """
    fullnames = glob.glob(globext)
    for fullname in fullnames:
        md5name = fullname.replace('.tar', '.md5')
        md5hash = checksum_md5(fullname)
        with open(md5name, 'w') as fid:
            fid.write(' '.join([md5hash, os.path.basename(fullname)]))
        make_file_group_writeable(md5name)


def tar_archive(output_filename, files):
    """Combine files as single-layer tar archive.

    Args:
        output_filename (str): path to write tar archive
        files (list): full paths to files to add to archive

    """
    if not files:
        raise ValueError("No files to archive, cannot create %s" %
                         output_filename)

    with tarfile.open(output_filename, "w") as tar:
        for filename in files:
            tar.add(filename, arcname=os.path.basename(filename))

    make_file_group_writeable(output_filename)


def untar_archive(filename, directory='.'):
    """Extract into directory using basename as new folder name.

    Args:
        filename (str): path to tar.gz archive
        directory (str): path to base directory to extract files

    Returns:
        str: path to new created directory

    """
    if make_dirs(directory):
        logger.info('Unpacking tar: %s', filename)
        tar = tarfile.open(filename, 'r:gz')
        tar.extractall(directory)
        tar.close()
        logger.info('End unpacking tar')
    return directory


def make_dirs(directory):
    """Create a directory if it does not already exist."""
    if not os.path.isdir(directory):
        logger.info('Create new directory: %s', directory)
        os.makedirs(directory)
        return True
    else:
        logger.debug('Directory already exists: %s', directory)
        return False


def ffind(*paths):
    """Find first file match by combining glob searches.

    Args:
        paths (list): list of filepath parts to combine as a glob

    Returns:
        str: the first result match from the glob

    Example:
        >>> ffind('/usr', 'bin', '*sh')
        '/usr/bin/bash'

    """
    search = os.path.join(*paths)
    logger.debug('Find files: %s', search)
    return glob.glob(search).pop()


def remove(*paths):
    """Remove mutiple paths, either files or directories.

    Args:
        paths (list): the paths to attempt to remove

    Returns:
        None

    Examples:
        >>> remove('local.txt', '/path/to/folder', '/other/folder')
        None

    """
    for path in paths:
        logger.debug('Remove %s', path)
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
