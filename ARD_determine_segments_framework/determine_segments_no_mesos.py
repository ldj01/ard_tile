#!/usr/bin/python
#
#  determine_segments.py
#
#  This program determines consecutive scenes to be tiled
#

import sys
import os, time
import cx_Oracle
import datetime
from pprint import pprint
import ConfigParser
from argparse import ArgumentParser
import glob
import logging
import string
import random


SUCCESS = 0
ERROR = 1




logger = None


# Message logging filter
class L2PGS_LoggingFilter(logging.Filter):
    def filter(self, record):
        record.subsystem = 'ARDSegmentFinder'

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



def determineSegments():

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






    determineSegments()
   
    logger.info('*******************End*******************')
