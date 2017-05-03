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

from operator import itemgetter
from itertools import groupby

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
   logger.info("Complete scene list: {0}".format(scenes_to_process))

   # iterate through scenes to process list building lists that contain
   # consecutive WRS_ROWS and storing that list in segments_list
   if len(scenes_to_process) > 0:
      previous_sat = scenes_to_process[0][5]
      previous_acq_date = scenes_to_process[0][0].strftime('%Y-%m-%d')
      previous_wrs_path = scenes_to_process[0][1]
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
          #tarFileName = glob.glob(r[3])
          #if len(tarFileName) > 0:
             # create new tuple with datetime object converted to string
          #new_tuple = r[0].strftime('%Y-%m-%d'), r[1], r[2], r[4]
          new_tuple = r[0].strftime('%Y-%m-%d'), r[1], r[2], r[3], r[4]

############
          current_sat = r[5]
          current_acq_date = r[0].strftime('%Y-%m-%d')
          current_wrs_path = r[1]
          current_wrs_row = r[2]
          logger.info("current_wrs_row-1: {0}, previous_wrs_row: {1}".format(current_wrs_row-1,previous_wrs_row))

          if current_sat != previous_sat:
          # break in satellite so save consec_wrs_row_list and nullify
             logger.info("diff satellite: {0}, {1}".format(current_sat,previous_sat))
             result = segmentCheck(str(consec_wrs_row_list),0)
             if result == "OK":
                segments_list.append(consec_wrs_row_list)
             else:
                logger.info("SAT Bad segment: {0}, {1}".format(result,consec_wrs_row_list))
             consec_wrs_row_list = []

          elif current_acq_date != previous_acq_date:
          # broken acquistion date so save consec_wrs_row_list and nullify
             logger.info("diff acq date: {0}, {1}".format(current_acq_date,previous_acq_date))
             result = segmentCheck(str(consec_wrs_row_list),0)
             if result == "OK":
                segments_list.append(consec_wrs_row_list)
             else:
                logger.info("ACQ Bad segment: {0}, {1}".format(result,consec_wrs_row_list))
             consec_wrs_row_list = []

          elif current_wrs_path != previous_wrs_path:
             logger.info("diff path: {0}, {1}".format(current_wrs_path,previous_wrs_path))
             # broken path number so save consec_wrs_row_list and nullify
             result = segmentCheck(str(consec_wrs_row_list),0)
             if result == "OK":
                segments_list.append(consec_wrs_row_list)
             else:
                logger.info("PATH Bad segment: {0}, {1}".format(result,consec_wrs_row_list))
             consec_wrs_row_list = []

          elif current_wrs_row-1 != previous_wrs_row:
             logger.info("diff row: {0}, {1}".format(current_wrs_row-1,previous_wrs_row))
             # broken row number so save consec_wrs_row_list and nullify
             result = segmentCheck(str(consec_wrs_row_list),0)
             if result == "OK":
                segments_list.append(consec_wrs_row_list)
             else:
                logger.info("ROW Bad segment: {0}, {1}".format(result,consec_wrs_row_list))
             consec_wrs_row_list = []

############

          # add record to consec_wrs_row_list list
          consec_wrs_row_list.append(new_tuple)
          previous_sat = current_sat
          previous_acq_date = current_acq_date
          previous_wrs_path = current_wrs_path
          previous_wrs_row = current_wrs_row

      # add last consec_wrs_row_list to segments_list
      segments_list.append(consec_wrs_row_list)

      logger.info("Number of segments found: {0}".format(len(segments_list)))

      # order segments_list by length of consec_wrs_row_list
      segments_list.sort(reverse=True,key=len)
      logger.info("segments list: {0}".format(segments_list))


      completed_scene_list = []
      processed_scenes_insert = "insert /*+ ignore_row_on_dupkey_index(ARD_PROCESSED_SCENES, SCENE_ID_PK) */ into ARD_PROCESSED_SCENES (scene_id,file_location) values (:1,:2)"
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
               row = (scene_record[3], scene_record[4])
               completed_scene_list.append(row)


      if not segmentAboveThreshold:
         logger.info("No segments found that meet the {0} scenes per segment minimum".format(minscenesperseg))

      # Insert scene list into ARD_PROCESSED_SCENES table

#      if len(completed_scene_list) > 0:
#         logger.info("Scenes inserted into ARD_PROCESSED_SCENES table: {0}".format(completed_scene_list))
#         cursor.bindarraysize = len(completed_scene_list)
#         cursor.prepare(processed_scenes_insert)
#         cursor.executemany(None, completed_scene_list)
#         connection.commit()
   else:
      logger.info("There are no scenes ready to process.")

   cursor.close()
   connection.close()
   return SUCCESS

def determineSegments2():

   SQL = conf.segment_query
 
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
   logger.info("Complete scene list: {0}".format(scenes_to_process))

   # iterate through scenes pulling out only info that we need
   # i.e. acq_date, path, row, file location and landsat product id
   reformatted_list = []
   if len(scenes_to_process) > 0:
      previous_row = ()
      for r in scenes_to_process:
          if r == previous_row:
             previous_row = r
             continue
          previous_row = r

          # Get complete file name
          #tarFileName = glob.glob(r[3])
          #if len(tarFileName) > 0:
             # create new tuple with datetime object converted to string
          new_tuple = r[0].strftime('%Y-%m-%d'), r[1], r[2], r[3], r[4]


          # add record to reformatted_list list
          reformatted_list.append(new_tuple)

      temp_segments_list = []
      for k, g in groupby(reformatted_list, lambda (x):x[4][:4]+x[4][17:25]+x[4][10:13]):
        #print list(g)
        temp_segments_list.append(list(g))

      #print temp_segments_list
      segments_list = []
      for segment in temp_segments_list:
         for k, g in groupby(enumerate(segment), lambda (i,x):i-x[2]):
            segments_list.append(map(itemgetter(1), g))

      #print segments_list

      logger.info("Number of segments found: {0}".format(len(segments_list)))

      # order segments_list by length of consec_wrs_row_list
      segments_list.sort(reverse=True,key=len)
      logger.info("segments list: {0}".format(segments_list))


      completed_scene_list = []
      processed_scenes_insert = "insert /*+ ignore_row_on_dupkey_index(ARD_PROCESSED_SCENES, SCENE_ID_PK) */ into ARD_PROCESSED_SCENES (scene_id,file_location) values (:1,:2)"
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
               row = (scene_record[3], scene_record[4])
               completed_scene_list.append(row)


      if not segmentAboveThreshold:
         logger.info("No segments found that meet the {0} scenes per segment minimum".format(minscenesperseg))

      # Insert scene list into ARD_PROCESSED_SCENES table

#      if len(completed_scene_list) > 0:
#         logger.info("Scenes inserted into ARD_PROCESSED_SCENES table: {0}".format(completed_scene_list))
#         cursor.bindarraysize = len(completed_scene_list)
#         cursor.prepare(processed_scenes_insert)
#         cursor.executemany(None, completed_scene_list)
#         connection.commit()
   else:
      logger.info("There are no scenes ready to process.")

   cursor.close()
   connection.close()
   return SUCCESS
def segmentCheck(fullSegment, knt):

    result = ''

                  # Gather information about the first scene
                  
    paren1 = fullSegment.find("(")
    
    date1start = fullSegment.find("'", paren1)
    date1end = fullSegment.find("'", date1start+1)
    firstDate = fullSegment[date1start+1:date1end]

    path1start = fullSegment.find(",", date1end)
    path1end = fullSegment.find(",", path1start+1)
    firstPathStr = fullSegment[path1start+2:path1end]
    firstPath = int(firstPathStr)

    sat1start = fullSegment.find(".gz", paren1)
    sat1end = fullSegment.find("'", sat1start+1)
    firstSat = fullSegment[sat1start+7:sat1start+11]

                   # Check each following scene against the first
    paren1 = fullSegment.find("(",sat1end)
                   
    while (paren1 > -1):
        paren2 = fullSegment.find(")", paren1)
        sceneInfo = fullSegment[paren1+1:paren2]

        dateStart = sceneInfo.find("('", sat1end)
        dateEnd = sceneInfo.find("'", dateStart+3)
        nextDate = sceneInfo[dateStart+2:dateEnd]

        pathStart = sceneInfo.find(",", dateEnd)
        pathEnd = sceneInfo.find(",", pathStart+1)
        pathStr = sceneInfo[pathStart+2:pathEnd]
        path = int(pathStr)

        satStart = sceneInfo.find(".gz", pathEnd)
        satEnd = sceneInfo.find("'", satStart+1)
        Sat = sceneInfo[satStart+7:satStart+11]


        #if (knt == 1):
        #    print 'pos = ' + str(date1start)
        #    print 'pos = ' + str(date1end)
        #    print 'sceneInfo = ' + sceneInfo
        #    print 'firstDate = ' + firstDate
        #    print 'firstPathStr = ' + firstPathStr
        #    print 'firstSat = ' + firstSat

        #    print 'nextDate = ' + nextDate
        #    print 'nextPath = ' + str(path)
        #    print 'nextSat = ' + Sat

        if not (firstDate == nextDate):
            result = "Bad Dates: " + firstDate + "/" + nextDate
    
        if not (firstPath == path):
            result = result + "  Bad Paths: " + str(firstPath) + "/" + str(path)

        if not (firstSat == Sat):
            result = result + "  Bad Satellites: " + firstSat + "/" + Sat
        
        paren1 = fullSegment.find("(", paren2)

    if (result == ''):
        return 'OK'
        
    return result

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



    #data = [2, 3, 4, 5, 2, 3, 4, 5, 6, 7]
    data = [(datetime.datetime(2013, 3, 29, 0, 0), 22, 32, '/hsm/lsat1/collection01/oli_tirs/A1_L2/2013/22/32/LC080220322013032901A1-SC*.tar.gz', 'LC08_L2TP_022032_20130329_20170310_01_A1', 'LC08'), (datetime.datetime(2013, 3, 29, 0, 0), 22, 34, '/hsm/lsat1/collection01/oli_tirs/A1_L2/2013/22/33/LC080220332013032901A1-SC*.tar.gz', 'LC08_L2TP_022033_20130329_20170310_01_A1', 'LC08'), (datetime.datetime(2013, 3, 29, 0, 0), 22, 32, '/hsm/lsat1/collection01/etm/A1_L2/2013/22/32/LE070220322013032901A1-SC*.tar.gz', 'LE07_L2TP_022032_20130329_20161103_01_A1', 'LE07'), (datetime.datetime(2013, 3, 29, 0, 0), 22, 33, '/hsm/lsat1/collection01/etm/A1_L2/2013/22/33/LE070220332013032901A1-SC*.tar.gz', 'LE07_L2TP_022033_20130329_20161103_01_A1', 'LE07')]
#    data = [(datetime.datetime(2013, 3, 29, 0, 0), 22, 32, '/hsm/lsat1/collection01/oli_tirs/A1_L2/2013/22/32/LC080220322013032901A1-SC*.tar.gz', 'LC08_L2TP_022032_20130329_20170310_01_A1', 'LC08'), (datetime.datetime(2013, 3, 29, 0, 0), 22, 33, '/hsm/lsat1/collection01/etm/A1_L2/2013/22/33/LE070220332013032901A1-SC*.tar.gz', 'LE07_L2TP_022033_20130329_20161103_01_A1', 'LE07')]
#    print data[0][4][:4]
#    print data[0][4][17:25]
#    print data[0][4][10:13]
#    segments_list = []
#    for k, g in groupby(data, lambda (x):x[4][:4]+x[4][17:25]+x[4][10:13]):
        #print list(g)
#        segments_list.append(list(g))
        #print map(itemgetter(1), g)

    #print segments_list
#    segments_list2 = []
#    for segment in segments_list:
#       for k, g in groupby(enumerate(segment), lambda (i,x):i-x[2]):
#          segments_list2.append(map(itemgetter(1), g))

#    print segments_list2
#    determineSegments()
    determineSegments2()
   
    logger.info('*******************End*******************')
