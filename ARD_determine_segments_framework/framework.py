""" ARD Tile Scheduler Mesos framework interface """

import os
import sys
import time
import json
import signal
from threading import Thread

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from util import logger
import db
from determine_segments import determine_segments


SUCCESS = 0
ERROR = 1


class ArdTileScheduler(mesos.interface.Scheduler):

    def __init__(self, implicitAcknowledgements, executor, conf):
        self.implicitAcknowledgements = implicitAcknowledgements
        self.executor = executor
        self.jobs = []
        self.logdirs = {}
        self.prod_ids = {}
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.tasksFailed = 0
        self.ard_config = conf

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
        if not shutdown.flag and self.tasksFailed == self.ard_config.max_failed_jobs:
            logger.error("Error:  Max allowable failures reached.  "
                         "Shutting down.")
            shutdown.shutdown(0, 0)

        # If a shutdown request has been issued or the job queue is empty,
        # there's nothing more to do.
        if shutdown.flag or not self.jobs:
            for offer in offers:
                driver.declineOffer(offer.id)
            return

        for offer in offers:
            if (not self.jobs or
                self.tasksLaunched - self.tasksFinished == self.ard_config.max_jobs or
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
                   self.tasksLaunched - self.tasksFinished < self.ard_config.max_jobs and
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

    def makeTask(self, offer, conf):

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

        localtime_volume = container.volumes.add()
        localtime_volume.host_path = '/etc/localtime'
        localtime_volume.container_path = '/etc/localtime'
        localtime_volume.mode = 2  # mesos_pb2.Volume.Mode.RO

        aux_volume = container.volumes.add()
        aux_volume.host_path = conf.auxdir
        aux_volume.container_path = conf.auxdir
        aux_volume.mode = 2  # mesos_pb2.Volume.Mode.RO

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
        task.name = 'ARD Clip ' + self.job_id.replace('-', ' ')

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

        command.uris.add().value = conf.confdir

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


class Shutdown():
    """ Framework shutdown control """
    def __init__(self):
        self.flag = False
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        self.flag = True
        logger.info("Shutdown requested.")


def format_job_id(segment):
    # job_id format: <sat>_<path>_<lowest_wrs_row>_<highest_wrs_row>_<acq_date>
    # example:        LE07-047-027-030-2016-09-28
    return ("{SATELLITE}-{WRS_PATH:03s}-{WRS_ROW:03s}-{WRS_ROW_H:03s}-{DATE_ACQUIRED:%Y-%m-%d}"
            .format(WRS_ROW_H=segment[-1]['WRS_ROW'], **segment[0]))


def queue_segments(jobs, conf, connection):
    """ Make a new Mesos job for every segment """
    try:
        segmentAboveThreshold = False
        for segment in determine_segments(**conf):
            completed_scene_list = []
            segment_length = len(segment)
            if segment_length >= conf.minscenesperseg:
                segmentAboveThreshold = True
                logger.info("Segment length: %d", len(segment))
                logger.info("Segment: %s", segment)
                for scene_record in segment:
                    # Build list of scenes here to be used in SQL Insert statement
                    row = (scene_record['LANDSAT_PRODUCT_ID'], scene_record['FILE_LOC'])
                    completed_scene_list.append(row)

                    # set 'BLANK' to 'INQUEUE' processing status
                    db.set_scene_to_inqueue(connection, scene_record['LANDSAT_PRODUCT_ID'])
                logger.info("Scenes inserted into ARD_PROCESSED_SCENES table: %s",
                            completed_scene_list)
                db.processed_scenes(connection, completed_scene_list)

                # Build the Docker entrypoint command.
                cmd = ['ARD_Clip_L457.py']
                if segment[0]['SATELITE'] == 'LC08':
                    cmd = ['ARD_Clip_L8.py']

                subdirdest = {'4': 'tm', '5': 'tm', '7': 'etm', '8': 'oli_tirs'}
                final_output = os.path.join(conf.outdir, "lta_incoming",
                                            subdirdest[segment[0]['SATELITE']], 'ARD_Tile')
                cmd.extend(['"' + json.dumps(segment) + '"', final_output ])

                # Compile the job information.
                job = Job()
                job.cpus = conf.cpus
                job.disk = conf.disk
                job.mem = conf.memory
                job.command = ' '.join(cmd)
                job.job_id = format_job_id(segment)
                jobs.append(job)

        if not segmentAboveThreshold:
            logger.info("No segments found that meet the %d scenes per segment minimum",
                        conf.minscenesperseg)

        return SUCCESS

    except:
        logger.exception('Unable to fetch segments!')
        return ERROR


def run_forever(conf):

    logger.info('******************Start************')
    logger.debug('DB connection: {0}'.format(conf.l2_db_con))
    logger.debug("Minimum Senes Per Seg: {0}".format(conf.minscenesperseg))
    logger.debug('Segment query: {0}'.format(conf.segment_query))

    global shutdown
    connection = db.connection(conf.l2_db_con)
    db.reset_records(connection)

    # Establish framework, executor, and authentication credentials information.
    framework = mesos_pb2.FrameworkInfo()
    framework.user = conf.framework_user
    framework.name = "ARD Tile Framework"
    framework.principal = conf.mesos_principal
    framework.role = conf.mesos_role

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "default"
    executor.name = "ARD Tile executor"

    implicitAcknowledgements = 1
    mesosScheduler = ArdTileScheduler(implicitAcknowledgements, executor, conf)

    if not conf.disable_creds:
        logger.info("             MESOS creds ENABLED")

        credential = mesos_pb2.Credential()
        credential.principal = conf.mesos_principal
        credential.secret = conf.mesos_secret
        driver = mesos.native.MesosSchedulerDriver(mesosScheduler, framework,
                                                   conf.master,
                                                   implicitAcknowledgements,
                                                   credential)
    else:
        logger.info("             MESOS creds disabled")
        driver = mesos.native.MesosSchedulerDriver(mesosScheduler, framework,
                                                   conf.master,
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
        if (not mesosScheduler.jobs and queue_segments(mesosScheduler.jobs, conf, connection) == ERROR):
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
                if queue_segments(mesosScheduler.jobs, conf, connection) == ERROR:
                    driver.stop(True)
                    sys.exit(1)
                time.sleep(20)

            driver.reviveOffers()
