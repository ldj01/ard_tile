#!/usr/bin/python
#
#  determine_segments.py
#
#  This program determines consecutive scenes to be tiled
#

import sys
import os, time
import signal
import cx_Oracle
import datetime
from pprint import pprint
import subprocess
from subprocess import Popen
import ConfigParser
from argparse import ArgumentParser
import glob
import logging
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
import string
import random
from threading import Thread


SUCCESS = 0
ERROR = 1


# Mesos framework interface
class ArdTileScheduler(mesos.interface.Scheduler):

    def __init__(self, implicitAcknowledgements, executor):
        self.implicitAcknowledgements = implicitAcknowledgements
        self.executor = executor
        self.jobs = []
        self.logdirs = {}
        self.prod_ids = {}
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.tasksFailed = 0

    def registered(self, driver, frameworkId, masterInfo):
        logger.info(("Registered with framework ID {0}")
                    .format(frameworkId.value))

    def resourceOffers(self, driver, offers):
        # Check whether the system needs to shut down based on the following
        # criteria:
        # - An interrupt or termination signal has been received.
        # - The maximum allowed number of jobs have failed.
        # - Retrieval of new jobs has failed.
        # - Database connection has failed (also in statusUpdate()).
        if not shutdown.flag and self.tasksFailed == conf.max_failed_jobs:
            logger.error("Error:  Max allowable failures reached.  "
                         "Shutting down.")
            shutdown.shutdown(0, 0)

        # If a shutdown request has been issued or the job queue is empty,
        # there's nothing more to do.
        if shutdown.flag or not self.jobs:
            for offer in offers:
                driver.declineOffer(offer.id)
            return

        # Connect to the database.
        try:
            con = cx_Oracle.connect(l2_db_con)
            con.close()
        except:
            logger.error("Error:  Unable to connect to the database.")
            shutdown.shutdown(0, 0)
            for offer in offers:
                driver.declineOffer(offer.id)
            return

        for offer in offers:
            if (not self.jobs or
                self.tasksLaunched - self.tasksFinished == conf.max_jobs or
                shutdown.flag):
                driver.declineOffer(offer.id)
                continue
            offerCpus = 0
            offerMem = 0
            offerDisk = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value
                elif resource.name == "disk":
                    offerDisk += resource.scalar.value

            # If the offer resources aren't sufficient, decline the offer.
            if (offerCpus < self.jobs[0].cpus or
                offerDisk < self.jobs[0].disk or
                offerMem < self.jobs[0].mem):
                driver.declineOffer(offer.id)
                continue

            # The offer is good, so see how many tasks we can stuff into it.
            tasks = []
            jobids = []
            while (self.jobs and
                   self.tasksLaunched - self.tasksFinished < conf.max_jobs and
                   len(tasks) < 4 and  # max jobs per offer
                   offerCpus >= self.jobs[0].cpus and
                   offerDisk >= self.jobs[0].disk and
                   offerMem >= self.jobs[0].mem):
                job = self.jobs.pop(0)
                tasks.append(job.makeTask(offer))
                jobids.append("'{0}'".format(job.job_id))
                job.submitted = True
                self.logdirs[job.job_id] = job.logdir
                self.prod_ids[job.job_id] = job.prod_id
                self.tasksLaunched += 1
                offerCpus -= job.cpus
                offerDisk -= job.disk
                offerMem -= job.mem

            if tasks:
                driver.launchTasks(offer.id, tasks)

            else:
                driver.declineOffer(offer.id)


    def statusUpdate(self, driver, update):
        task_id = update.task_id.value

        logger.info(("Task {0} is in state {1}. (agent {2})")
                    .format(task_id, mesos_pb2.TaskState.Name(update.state),
                            update.slave_id.value))

        if (update.state == mesos_pb2.TASK_FINISHED or
            update.state == mesos_pb2.TASK_LOST or
            update.state == mesos_pb2.TASK_KILLED or
            update.state == mesos_pb2.TASK_FAILED):
            # Watch for duplicate updates.  If this is a duplicate, just get
            # out of this function.  We know we've been here before if the
            # product ID for this task is no longer available.
            if task_id not in self.prod_ids:
                if not self.implicitAcknowledgements:
                    driver.acknowledgeStatusUpdate(update)
                return

            self.tasksFinished += 1


            del self.logdirs[task_id]
            del self.prod_ids[task_id]

            # If a shutdown request has been issued and all running tasks have
            # completed, shut down.
            if shutdown.flag and self.tasksFinished == self.tasksLaunched:
                driver.stop()

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        logger.info("Received framework message")


# ARD Tile task object
class Job():

    def __init__(self):
        self.cpus = 1
        self.disk = 100
        self.mem = 128
        self.command = ""
        self.submitted = False
        self.logdir = ""
        self.prod_id = ""
        self.job_id = ""

    def makeTask(self, offer):

        # Create the container object
        container = mesos_pb2.ContainerInfo()
        container.type = 1  # mesos_pb2.ContainerInfo.Type.DOCKER

        # Create container volumes
        output_volume = container.volumes.add()
        output_volume.host_path = conf.outdir
        output_volume.container_path = conf.outdir
        output_volume.mode = 1  # mesos_pb2.Volume.Mode.RW

        input_volume = container.volumes.add()
        input_volume.host_path = conf.indir
        input_volume.container_path = conf.indir
        input_volume.mode = 2  # mesos_pb2.Volume.Mode.RO

        config_volume = container.volumes.add()
        config_volume.host_path = conf.confdir
        #config_volume.container_path = conf.confdir
        config_volume.container_path = '/mnt/mesos/sandbox/ARD_Clip.conf'
        config_volume.mode = 2  # mesos_pb2.Volume.Mode.RO

        localtime_volume = container.volumes.add()
        localtime_volume.host_path = '/etc/localtime'
        localtime_volume.container_path = '/etc/localtime'
        localtime_volume.mode = 2  # mesos_pb2.Volume.Mode.RO

        # Specify container Docker image
        docker = mesos_pb2.ContainerInfo.DockerInfo()
        docker.image = conf.container_name
        docker.network = 2  # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
        docker.force_pull_image = False

        user_param = docker.parameters.add()
        user_param.key = 'user'
        user_param.value = '{0}:{1}'.format(conf.container_user,
                                            conf.container_group)

        workdir_param = docker.parameters.add()
        workdir_param.key = 'workdir'
        workdir_param.value = '/mnt/mesos/sandbox'

        container.docker.MergeFrom(docker)

        # Create the task object
        task = mesos_pb2.TaskInfo()
        task.task_id.value = self.job_id
        task.slave_id.value = offer.slave_id.value
        task.name = 'ARD Clip ' + self.job_id

        # Add the container
        task.container.MergeFrom(container)

        # Define the command line to execute in the Docker container
        command = mesos_pb2.CommandInfo()
        command.value = self.command

        # Set the user to run the task as.
        command.user = conf.framework_user

        # Add the docker uri for logging into the remote repository
        if conf.docker_pkg:
            command.uris.add().value = conf.docker_pkg

        # The MergeFrom allows to create an object then to use this object
        # in an other one. Here we use the new CommandInfo object and specify
        # to use this instance for the parameter task.command.
        task.command.MergeFrom(command)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = self.cpus

        disk = task.resources.add()
        disk.name = "disk"
        disk.type = mesos_pb2.Value.SCALAR
        disk.scalar.value = self.disk

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mem

        # Return the object
        return task


# Framework shutdown control
class Shutdown():
    def __init__(self):
        self.flag = False
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        self.flag = True
        logger.info("Shutdown requested.")

logger = None


# Message logging filter
class L2PGS_LoggingFilter(logging.Filter):
    def filter(self, record):
        record.subsystem = 'ARDClip'

        return True


# Exception formatter
class L2PGS_ExceptionFormatter(logging.Formatter):
    def formatException(self, exc_info):
        result = super(L2PGS_ExceptionFormatter, self).formatException(exc_info)
        return repr(result)

    def format(self, record):
        s = super(L2PGS_ExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', ' ')
            s = s.replace('\\n', ' ')
        return s


# Initialize the message logging components.
def setup_logging():

    global logger

    # Setup the logging level
    logging_level = logging.INFO

    handler = logging.StreamHandler(sys.stdout)
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

    logging.getLogger('requests').setLevel(logging.WARNING)



# Configuration items
class Config():

    def __init__(self):
        self.config = ConfigParser.ConfigParser()
    
    # Read one configuration item from a specified section of the configuration
    # file.
    #
    # type = 0 (string), 1 (integer)
    def readConfItem(self, section, keyword, type):
        if not self.config.has_option(section, keyword):
            logger.error(("Error: {} configuration option missing.")
                         .format(keyword))
            sys.exit(1)
        elif type == 0:   # string
            return self.config.get(section, keyword)
        else:  # integer
            return self.config.getint(section, keyword)


    # Read the configuration information.
    def readConfig(self, argv):
        parser = ArgumentParser(description='Launch the ARD Clip framework.')

        # Optional arguments
        parser.add_argument('-c', '--conf', dest='config_file',
                            help='configuration file')
        parser.add_argument('--disable-credentials', action='store_true',
                            dest='disable_creds', default=False,
                            help='flag to disable Mesos credential use')

        # Read the command-line arguments.
        args = parser.parse_args()

        # Set the configuration values.
        if args.config_file is not None:
            config_file = args.config_file
        else:
            config_file = '/home/doerr/ARD_determine_segments.conf'

        # Set the configuration values.
        #config_file = 'ARD_determine_segments.conf'

        if len(self.config.read(config_file)) == 0:
            logger.error(("Error opening config file {0}.")
                         .format(config_file))
            sys.exit(1)
        
        section = 'ard'
        if not self.config.has_section(section):
            logger.error(("Error: {0} section not in config file.")
                         .format(section))
            sys.exit(1)
        l2_db_con = self.readConfItem(section, 'dbconnect', 0)
        minscenesperseg = self.readConfItem(section, 'minscenespersegment', 1)
        master = self.readConfItem(section, 'zookeeper', 0)
        self.docker_pkg = self.readConfItem(section, 'docker_pkg', 0)
        self.segment_query = self.readConfItem(section, 'segment_query', 0)
        self.satellite = self.readConfItem(section, 'satellite', 0)

        if not args.disable_creds:
            section = 'mesos'
            if not self.config.has_section(section):
               logger.error(("Error: {0} section not in config file.")
                         .format(section))
               sys.exit(1)
            mesos_principal = self.readConfItem(section, 'principal', 0)
            mesos_secret = self.readConfItem(section, 'secret', 0)
            mesos_role = self.readConfItem(section, 'role', 0)
        else:
            mesos_principal = ""
            mesos_secret = ""
            mesos_role = ""

        section = 'pgs_framework'
        if not self.config.has_section(section):
            logger.error(("Error: {0} section not in config file.")
                         .format(section))
            sys.exit(1)
        self.framework_user = self.readConfItem(section, 'framework_user', 0)
        self.input_method = self.readConfItem(section, 'input_method', 0)
        if self.input_method == 'http':
            self.input_url = self.readConfItem(section, 'input_url', 0)
        self.max_orders = self.readConfItem(section, 'max_orders', 1)
        self.max_jobs = self.readConfItem(section, 'max_jobs', 1)
        self.max_failed_jobs = self.readConfItem(section, 'max_failed_jobs', 1)
        self.max_retries = self.readConfItem(section, 'max_retries', 1)
        self.retry_interval = self.readConfItem(section, 'retry_interval', 1)
        self.cpus = self.readConfItem(section, 'req_cpus', 1)
        self.memory = self.readConfItem(section, 'req_mem_mb', 1)
        self.disk = self.readConfItem(section, 'req_disk_mb', 1)

        section = 'ardclip'
        if not self.config.has_section(section):
            logger.error(("Error: {0} section not in config file.")
                         .format(section))
            sys.exit(1)
        self.confdir = self.readConfItem(section, 'ard_conf_dir', 0)
        self.indir = self.readConfItem(section, 'base_input_dir', 0)
        self.outdir = self.readConfItem(section, 'base_output_dir', 0)
        self.container_name = self.readConfItem(section, 'container', 0)
        self.container_user = self.readConfItem(section, 'internal_user_id', 0)
        self.container_group = self.readConfItem(section, 'internal_group_id', 0)

        return (master, l2_db_con, args.disable_creds,
                mesos_principal, mesos_secret, mesos_role, minscenesperseg)


def id_generator(size=6):
   return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))

def determineSegments(jobs):

   SQL = conf.segment_query
   #SQL="select trunc(nvl(b.DATE_ACQUIRED,c.DATE_ACQUIRED)) DATE_ACQUIRED, nvl(b.WRS_PATH,c.WRS_PATH) WRS_PATH, nvl(b.WRS_ROW,c.WRS_ROW) WRS_ROW, a.L2_LOCATION || '/' || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,3) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,4) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,6) || regexp_substr(a.LANDSAT_PRODUCT_ID, '[^_]+',1,7) || '-SC*.tar.gz' file_loc, a.LANDSAT_PRODUCT_ID LANDSAT_PRODUCT_ID from l2_albers_inventory a left join etm_scene_inventory@inv_l2_bridge_link b on a.landsat_scene_id = b.landsat_scene_id and b.vcid = 1 left join tm_scene_inventory@inv_l2_bridge_link c on a.landsat_scene_id = c.landsat_scene_id where a.LANDSAT_PRODUCT_ID not in (select scene_id from ARD_PROCESSED_SCENES) order by DATE_ACQUIRED, WRS_PATH, WRS_ROW"
 
   logger.info('Segment query: {0}'.format(SQL))


   try:
      connection = cx_Oracle.connect(l2_db_con)
   except:
      logger.error("Unable to connect to the database.")
      return ERROR
 
   cursor = connection.cursor()
   cursor.execute(SQL)
   scenes_to_process = cursor.fetchall()

   logger.info("Number of scenes returned from query: {0}".format(len(scenes_to_process)))
   #logger.info("Complete scene list: {0}".format(scenes_to_process))

   # iterate through scenes to process list building lists that contain
   # consecutive WRS_ROWS and storing that list in segments_list
   if len(scenes_to_process) > 0:
      previous_wrs_row = scenes_to_process[0][2] -1
      previous_row = ()
      consec_wrs_row_list = []
      segments_list = []
      for r in scenes_to_process:
          if r == previous_row:
             previous_row = r
             continue
          previous_row = r

          # Get complete file name
          tarFileName = glob.glob(r[3])
          if len(tarFileName) > 0:
             # create new tuple with datetime object converted to string
             new_tuple = r[0].strftime('%Y-%m-%d'), r[1], r[2], tarFileName[0], r[4]
             current_wrs_row = r[2]
             if current_wrs_row-1 != previous_wrs_row:
             # broken row number so save consec_wrs_row_list and nullify
                segments_list.append(consec_wrs_row_list)
                consec_wrs_row_list = []

             # add record to consec_wrs_row_list list
             consec_wrs_row_list.append(new_tuple)
             previous_wrs_row = current_wrs_row

      # add last consec_wrs_row_list to segments_list
      segments_list.append(consec_wrs_row_list)

      logger.info("Number of segments found: {0}".format(len(segments_list)))

      # order segments_list by length of consec_wrs_row_list
      segments_list.sort(reverse=True,key=len)


      completed_scene_list = []
      processed_scenes_insert = "insert into ARD_PROCESSED_SCENES (scene_id,file_location) values (:1,:2)"
      segmentAboveThreshold = False
      # Start segment processing
      # 1. Loop through segments_list and pass segment (consec scene list) to 
      #    external program.
      for segment in segments_list:
         segment_length = len(segment)
         if segment_length >= minscenesperseg:
            segmentAboveThreshold = True
            logger.info("Segment length: {0}".format(len(segment)))
            logger.info("Segment: {0}".format(segment))
            for scene_record in segment:
               # Build list of scenes here to be used in SQL Insert statement
               #print scene_record[4]
               row = (scene_record[4], scene_record[3])
               completed_scene_list.append(row)

            # Build the Docker command.
            cmd = ['ARD_Clip_L457.py']
            if conf.satellite == 'L8':
               cmd = ['ARD_Clip_L8.py']
            cmd.extend(['"' + str(segment) + '"', conf.outdir + "/lta_incoming"])

            # Compile the job information.
            job = Job()
            job_id = id_generator(10)
            job.cpus = conf.cpus
            job.disk = conf.disk
            job.mem = conf.memory
            job.command = ' '.join(cmd)
######           job.logdir = ('{0}').format(conf.outdir)
            job.job_id = job_id
            jobs.append(job)

      if not segmentAboveThreshold:
         logger.info("No segments found that meet the {0} scenes per segment minimum".format(minscenesperseg))

      # Insert scene list into ARD_PROCESSED_SCENES table

      if len(completed_scene_list) > 0:
         logger.info("Scenes inserted into ARD_PROCESSED_SCENES table: {0}".format(completed_scene_list))
         cursor.bindarraysize = len(completed_scene_list)
         cursor.prepare(processed_scenes_insert)
         cursor.executemany(None, completed_scene_list)
         connection.commit()
   else:
      logger.info("There are no scenes ready to process.")

   cursor.close()
   connection.close()
   return SUCCESS

# Main processing block
if __name__ == "__main__":

    setup_logging()

    l2_db_con = 'L2_BRIDGE/L2b123@lsdsscant.cr.usgs.gov:1521/crdev'
    minscenesperseg = 3

    # Read the configuration information and command-line arguments.
    conf = Config()
    master, l2_db_con, disable_creds, mesos_principal, mesos_secret, \
        mesos_role, minscenesperseg = conf.readConfig(sys.argv)

    logger.info('******************Start************')
    logger.info('             DB connection: {0}'.format(l2_db_con))
    logger.info("             MinSenesPerSeg: {0}".format(minscenesperseg))


    # Establish framework, executor, and authentication credentials
    # information.
    framework = mesos_pb2.FrameworkInfo()
    framework.user = conf.framework_user
    framework.name = "ARD Tile Framework"
    framework.principal = mesos_principal
    framework.role = mesos_role

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "default"
    executor.name = "ARD Tile executor"

    implicitAcknowledgements = 1
    mesosScheduler = ArdTileScheduler(implicitAcknowledgements, executor)

    if not disable_creds:
        logger.info("             MESOS creds ENABLED")

        credential = mesos_pb2.Credential()
        credential.principal = mesos_principal
        credential.secret = mesos_secret
        driver = mesos.native.MesosSchedulerDriver(mesosScheduler, framework,
                                                   master,
                                                   implicitAcknowledgements,
                                                   credential)
    else:
        logger.info("             MESOS creds disabled")
        driver = mesos.native.MesosSchedulerDriver(mesosScheduler, framework,
                                                   master,
                                                   implicitAcknowledgements)

    shutdown = Shutdown()

    # driver.run() blocks, so run it in a separate thread.
    def run_driver_async():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)

    framework_thread = Thread(target = run_driver_async, args = ())
    framework_thread.start()

    while framework_thread.is_alive():
        # If a shutdown has been requested, suppress offers and wait for the
        # framework thread to complete.
        if shutdown.flag:
            driver.suppressOffers()
            while framework_thread.is_alive():
                time.sleep(5)
            break

        # If the job queue is empty, get work.
        if (not mesosScheduler.jobs and determineSegments(mesosScheduler.jobs) == ERROR):
            driver.stop(True)
            sys.exit(1)

        # If there's no new work to be done or the max number of jobs are
        # already running, suppress offers and wait for some jobs to finish.
        if (not mesosScheduler.jobs or
            mesosScheduler.tasksLaunched - mesosScheduler.tasksFinished ==
                                                               conf.max_jobs):
            driver.suppressOffers()
            while (mesosScheduler.tasksLaunched -
                   mesosScheduler.tasksFinished == conf.max_jobs):
                time.sleep(20)
            while not mesosScheduler.jobs:
                if determineSegments(mesosScheduler.jobs) == ERROR:
                    driver.stop(True)
                    sys.exit(1)
                time.sleep(20)
                
            driver.reviveOffers()
   
    logger.info('*******************End*******************')
