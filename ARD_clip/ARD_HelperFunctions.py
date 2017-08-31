# ==========================================================================
#
# ARD HelperFunctions
#
#  13 Mar 2017 - Initial Version
#
#
# ==========================================================================
import datetime
import os
import logging
import sys
from osgeo import gdal, osr, ogr
import numpy as np
import ConfigParser
import ast
import urllib2
import stat
import cx_Oracle


# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Create a string containing the current UTC date/time
#
def getProductionDateTime():

    prodTime =  str(datetime.datetime.utcnow())
    prodTime = prodTime.replace(' ', 'T')
    dotPos = prodTime.find('.')
    if (dotPos <= 0):
        prodTime = prodTime + 'Z'
    else:
        prodTime = prodTime[:dotPos] + 'Z'

    return prodTime

    
# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Given a tile identifier (Horizontal, Vertical), return the Albers 
#                     coordinates
#
def getTileFootprintCoords(curTile, tileFootprints):

    returnString = ''
    for curTuple in tileFootprints:
        if (curTuple[0] == curTile):
            returnString = str(curTuple[1][0]) + ' ' + str(curTuple[1][1]) + ' ' + \
                           str(curTuple[1][2]) + ' ' + str(curTuple[1][3]) + ' '
    return returnString
     
# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Bands are renamed from Bridge version to ARD version.  This function
#                      returns the correct ARD suffix only
#
def getARDName(L2filename, filenameCrosswalk):

    for curTuple in filenameCrosswalk:
        if (curTuple[0] == L2filename):
            return curTuple[1]

    return 'ERROR'
    
# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Reads an existing L2 metadata file and returns it as a big, long string
#
def makeMetadataString(metaName):

    if (not os.path.isfile(metaName)):
        return 'ERROR - File does not exist'
    else:
        try:
            infile = open(metaName, 'r')
            metaLines = infile.read()
            infile.close()
        except:
            return 'ERROR - Opening or closing metadata file'

    return metaLines

# ----------------------------------------------------------------------------------------------
#
#   Purpose:  A file containing a histogram of values in the pixel_qa band has been
#                     generated.  Open the file and find the count of the specific values for 
#                     each of the various categories.  These counts will be used for calculating
#                     the % cloud cover, % snow/ice, etc... that will be shown in EarthExplorer.
#
def parseHistFile(histFilename):

    if (not os.path.isfile(histFilename)):
        return ('ERROR - File does not exist',)
    else:
        try:
            infile = open(histFilename, 'r')
            histLines = infile.read()
            infile.close()
        except:
            return ('ERROR - Opening or closing hist.json file',)

    bucketsLoc = histLines.find('buckets from')
    colonLoc = histLines.find(':', bucketsLoc + 1)

                                                                # Create an array with the number of occurrences
                                                                # of all 256 values
    histArray = []
    headLoc = histLines.find('  ', colonLoc) + 2
    while (len(histArray) <= 255):
        tailLoc = histLines.find(' ', headLoc)
        histArray.append(histLines[headLoc:tailLoc])
        headLoc = tailLoc + 1
    
    countClear = 0
    countWater = 0
    countSnow = 0
    countShadow = 0
    countCloud = 0
    countFill = 0
    
    binNumber = 1
    while (binNumber <= 255):
        if (long(histArray[binNumber]) > 0):

            #print 'bin #=' + str(binNumber) + '  histArray[binNumber] = ' + str(histArray[binNumber])
            binStart = binNumber
                                                                # ignore cloud confidence bit
            if (binStart >= 128):
                binStart = binStart - 128
                                                                # ignore cloud confidence bit
            if (binStart >= 64):
                binStart = binStart - 64

            if (binStart >= 32):
                countCloud = countCloud + long(histArray[binNumber])
                binStart = binStart - 32

            if (binStart >= 16):
                countSnow = countSnow + long(histArray[binNumber])
                binStart = binStart - 16

            if (binStart >= 8):
                countShadow = countShadow + long(histArray[binNumber])
                binStart = binStart - 8

            if (binStart >= 4):
                countWater = countWater + long(histArray[binNumber])
                binStart = binStart - 4

            if (binStart >= 2):
                countClear = countClear + long(histArray[binNumber])
                binStart = binStart - 2

            if (binStart >= 1):
                countFill = countFill + long(histArray[binNumber])

        binNumber = binNumber + 1

    return (countFill, countClear, countWater, countSnow, countShadow, countCloud)


# ----------------------------------------------------------------------------------------------
#
#   Purpose:  A file containing a histogram of values in the lineage band has been
#                     generated.  Open the file and find the highest value.
#
def parseSceneHistFile(sceneFilename):

    if (not os.path.isfile(sceneFilename)):
        return ('ERROR - File does not exist',)
    else:
        try:
            infile = open(sceneFilename, 'r')
            sceneHistLines = infile.read()
            infile.close()
        except:
            return ('ERROR - Opening or closing scenes.json file',)

    bucketsLoc = sceneHistLines.find('buckets from')
    colonLoc = sceneHistLines.find(':', bucketsLoc + 1)

    histArray = []
    headLoc = sceneHistLines.find('  ', colonLoc) + 2
    while (len(histArray) <= 255):
        tailLoc = sceneHistLines.find(' ', headLoc)
        histArray.append(sceneHistLines[headLoc:tailLoc])
        headLoc = tailLoc + 1
    
    count1 = long(histArray[1])
    count2 = long(histArray[2])
    count3 = long(histArray[3])

    if (count3 > 0):
        return 3
    elif (count2 > 0):
        return 2
    elif (count1 > 0):
        return 1
    else:
        return 0

# ----------------------------------------------------------------------------------------------
#
# Message logging filter
class L2PGS_LoggingFilter(logging.Filter):
    def filter(self, record):
        record.subsystem = 'ARDTile'

        return True

# ----------------------------------------------------------------------------------------------
#
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

# ----------------------------------------------------------------------------------------------
#
# Initialize the message logging components.
def setup_logging():


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

    return logger

# ==========================================================================

# ----------------------------------------------------------------------------------------------
#
# return counts for each cover type
def sum_counts(unique_values, count_list, target_bit):
    # determine if each unique value contains target bit
    bit_bool = unique_values & 1 << target_bit > 0

    # sum counts from all values containing target bit
    final_count = np.sum(np.array(count_list)[bit_bool])

    return final_count

# ----------------------------------------------------------------------------------------------
#
def raster_value_count(raster_in, landsat_8=False):


    # open raster, read first band as array
    ds = gdal.Open(raster_in)
    band_arr = np.array(ds.GetRasterBand(1).ReadAsArray())

    # get unique values from array
    uni, counts = np.unique(band_arr, return_counts=True)

    # count bits
    countFill = sum_counts(uni, counts, 0)
    countClear = sum_counts(uni, counts, 1)
    countWater = sum_counts(uni, counts, 2)
    countShadow = sum_counts(uni, counts, 3)
    countSnow = sum_counts(uni, counts, 4)
    countCloud = sum_counts(uni, counts, 5)

    # get high-conf cirrus and terrain occlusion for L8
    if landsat_8:
        countCirrus = sum_counts(uni, counts, 9)
        countTerrain = sum_counts(uni, counts, 10)

        return (countFill, countClear, countWater, countShadow, countSnow,
                countCloud, countCirrus, countTerrain)

    else:  # else return for L4-7
        return (countFill, countClear, countWater, countSnow, countShadow, 
                countCloud)



# -----------------------------------------------------------------------------
#
#   Purpose:  This function finds all the tile ids that intersect the input
#             landsat product id.  It also creates a dictionary of tile ids
#             that contain all the required path/rows that contribute to that
#             tile.
#   Inputs:   db connection, landsatProdID, region, wrsPath
#   Example input:  db_connector
#                   'LC08_L2TP_032028_20130802_20170309_01_A1'
#                   'CU'
#                   32
#                   18
#   Returns:  a list of tiles, a dictionary of tiles containing lists of path/rows.
#   Example output:
#     [('002', '002'), ('003', '002'), ('004', '002')]
#     {'004002063': [('063', '046'), ('063', '047')],
#      '002002063': [('063', '046'), ('063', '047')],
#      '003002063': [('063', '046'), ('063', '047')]}
#
def getTilesAndScenesLists(connection, landsatProdID, region, wrsPath, wrsRow, logger, acqdate, satellite):

    # We need to get 2 consecutive wrsRows north and 2 consecutive wrsRows south
    # of input scene to account for possible 3 scene tile.
    northRow = wrsRow - 1
    northnorthRow = wrsRow - 2
    southRow = wrsRow + 1
    southsouthRow = wrsRow + 2
    northPathRow = '{0:03d}{1:03d}'.format(wrsPath,northRow)
    northnorthPathRow = '{0:03d}{1:03d}'.format(wrsPath,northnorthRow)
    southPathRow = '{0:03d}{1:03d}'.format(wrsPath,southRow)
    southsouthPathRow = '{0:03d}{1:03d}'.format(wrsPath,southsouthRow)


    if satellite == 'LC08':
        minRowLimit = wrsRow - 2
        maxRowLimit = wrsRow + 2

        ls_prod_id_sql = "select distinct LANDSAT_PRODUCT_ID from \
                          inventory.LMD_SCENE where \
                          trunc(DATE_ACQUIRED) = to_date(:1,'YYYY-MM-DD') \
                          and LANDSAT_PRODUCT_ID is not null \
                          and WRS_ROW >= :2 and WRS_ROW <= :3 \
                          and WRS_PATH = :4"
        ls_prod_id_tuple = (acqdate,minRowLimit,maxRowLimit,wrsPath)
        ls_prod_id_cursor = connection.cursor()
        ls_prod_id_cursor.execute(ls_prod_id_sql, ls_prod_id_tuple)
        ls_prod_id_scenes = ls_prod_id_cursor.fetchall()
        ls_prod_id_cursor.close()
    else:
        if satellite == 'LE07':
            table = 'll0arcb.etm_scene_inventory@inv_l2_bridge_link'
        else:
            table = 'll0arcb.tm_scene_inventory@inv_l2_bridge_link'

        minRowLimit = wrsRow - 2
        maxRowLimit = wrsRow + 2

        ls_prod_id_sql = "select distinct LANDSAT_PRODUCT_ID_ALBERS from " + \
                          table + " where \
                          trunc(DATE_ACQUIRED) = to_date(:1,'YYYY-MM-DD') \
                          and LANDSAT_PRODUCT_ID_ALBERS is not null \
                          and WRS_ROW >= :2 and WRS_ROW <= :3 \
                          and WRS_PATH = :4"
        ls_prod_id_tuple = (acqdate,minRowLimit,maxRowLimit,wrsPath)
        ls_prod_id_cursor = connection.cursor()
        ls_prod_id_cursor.execute(ls_prod_id_sql, ls_prod_id_tuple)
        ls_prod_id_scenes = ls_prod_id_cursor.fetchall()
        ls_prod_id_cursor.close()



    scene_list = ()
    loop_ctr = 1
    where_clause = ''
    if len(ls_prod_id_scenes) > 0:
        for record in ls_prod_id_scenes:

            if loop_ctr == 1:
                where_clause += ' LANDSAT_PRODUCT_ID = :' + str(loop_ctr)
            else:
                where_clause += ' OR LANDSAT_PRODUCT_ID = :' + str(loop_ctr)

            if satellite == 'LC08':
                scene_list = scene_list + (record[0][:6] + '2' + record[0][7:38] + 'A' + record[0][39:],)
            else:
                scene_list = scene_list + (record[0][:6] + '2' + record[0][7:],)

            loop_ctr = loop_ctr + 1


    # Get coordinates for input scene and north and south scene.
    SQL="select LANDSAT_PRODUCT_ID, \
         'POLYGON ((' ||  CORNER_UL_LON || ' ' || CORNER_UL_LAT || ',' || \
         CORNER_LL_LON || ' ' || CORNER_LL_LAT || ',' || \
         CORNER_LR_LON || ' ' || CORNER_LR_LAT || ',' || \
         CORNER_UR_LON || ' ' || CORNER_UR_LAT || ',' || \
         CORNER_UL_LON || ' ' || CORNER_UL_LAT || '))' \
         from SCENE_COORDINATE_MASTER_V where " + where_clause + \
         " order by LANDSAT_PRODUCT_ID desc"

    select_cursor = connection.cursor()
    select_cursor.execute(SQL, scene_list)
    scene_records = select_cursor.fetchall()
    select_cursor.close()

    logger.info('Coordinate query response: {0}'.format(scene_records))

    input_scene_coords = ""
    north_scene_coords = ""
    north_north_scene_coords = ""
    south_scene_coords = ""
    south_south_scene_coords = ""
    if len(scene_records) > 0:
        for record in scene_records:
            if record[0] == landsatProdID:
                input_scene_coords = record[1]
            if northPathRow in record[0]:
                north_scene_coords = record[1]
            if northnorthPathRow in record[0]:
                north_north_scene_coords = record[1]
            if southPathRow in record[0]:
                south_scene_coords = record[1]
            if southsouthPathRow in record[0]:
                south_south_scene_coords = record[1]

    # Create geometry objects for each scenes coordinates
    if input_scene_coords != "":
        input_scene_geometry = ogr.CreateGeometryFromWkt(input_scene_coords)
    if north_scene_coords != "":
        north_scene_geometry = ogr.CreateGeometryFromWkt(north_scene_coords)
    if north_north_scene_coords != "":
        north_north_scene_geometry = ogr.CreateGeometryFromWkt(north_north_scene_coords)
    if south_scene_coords != "":
        south_scene_geometry = ogr.CreateGeometryFromWkt(south_scene_coords)
    if south_south_scene_coords != "":
        south_south_scene_geometry = ogr.CreateGeometryFromWkt(south_south_scene_coords)

    # Find all the tiles that intersect the input scene
    # and put into a list

    if 'ARD_AUX_DIR' in os.environ:
        aux_path = os.getenv('ARD_AUX_DIR')
        daShapefile = aux_path + "/shapefiles/" + region + "_ARD_tiles_geographic.shp"
        driver = ogr.GetDriverByName('ESRI Shapefile')
        dataSource = driver.Open(daShapefile, 0) # 0 means read-only. 1 means writeable.
    else:
        logger.error('ARD_AUX_DIR environment variable not set')
        raise KeyError('ARD_AUX_DIR environment variable not set')


    tile_list = []
    scenesForTilePathLU = {}

    # Check to see if shapefile is found.
    if dataSource is None:
        logger.info('Could not open {0}'.format(daShapefile))
    else:
        layer = dataSource.GetLayer()
        spatialRef = layer.GetSpatialRef()
        input_scene_geometry.AssignSpatialReference(spatialRef)
        if north_scene_coords != "":
            north_scene_geometry.AssignSpatialReference(spatialRef)
        if north_north_scene_coords != "":
            north_north_scene_geometry.AssignSpatialReference(spatialRef)
        if south_scene_coords != "":
            south_scene_geometry.AssignSpatialReference(spatialRef)
        if south_south_scene_coords != "":
            south_south_scene_geometry.AssignSpatialReference(spatialRef)

        for feature2 in layer:
            geom2 = feature2.GetGeometryRef()
            H_attr = feature2.GetField('H')
            V_attr = feature2.GetField('V')
            leftX_attr = feature2.GetField('UL_X')
            bottomY_attr = feature2.GetField('LL_Y')
            rightX_attr = feature2.GetField('LR_X')
            topY_attr = feature2.GetField('UR_Y')

            # select only the intersections
            if geom2.Intersects(input_scene_geometry): 

                # put intersected tiles into a list
                H_formatted = '{0:03d}'.format(H_attr)
                V_formatted = '{0:03d}'.format(V_attr)
                tile_tuple = H_formatted, V_formatted
                tile_tuple2 = leftX_attr, bottomY_attr, rightX_attr, topY_attr
                final_tuple = tile_tuple, tile_tuple2
                tile_list.append(final_tuple)

                # Add path, row of input scene to dictionary
                key = "{0:03d}{1:03d}{2:03d}".format(H_attr, V_attr, wrsPath)
                path_formatted = '{0:03d}'.format(wrsPath)
                row_formatted = '{0:03d}'.format(wrsRow)
                tuple = path_formatted, row_formatted
                my_list = []
                my_list.append(tuple)
                scenesForTilePathLU[key] = my_list

                # Now see if tile intersects with north and south scene
                # and put into a dictionary
                if north_scene_coords != "":
                    if geom2.Intersects(north_scene_geometry): 

                        row_formatted = '{0:03d}'.format(northRow)
                        tuple = path_formatted, row_formatted
                        tuple_already_exists = False
                        my_list = scenesForTilePathLU[key]
                        for tuple1 in my_list:
                            if tuple1 == tuple:
                                tuple_already_exists = True
                        if not tuple_already_exists:
                            my_list.append(tuple)
                if north_north_scene_coords != "":
                    if geom2.Intersects(north_north_scene_geometry): 

                        row_formatted = '{0:03d}'.format(northnorthRow)
                        tuple = path_formatted, row_formatted
                        tuple_already_exists = False
                        my_list = scenesForTilePathLU[key]
                        for tuple1 in my_list:
                            if tuple1 == tuple:
                                tuple_already_exists = True
                        if not tuple_already_exists:
                            my_list.append(tuple)
                if south_scene_coords != "":
                    if geom2.Intersects(south_scene_geometry): 

                        row_formatted = '{0:03d}'.format(southRow)
                        tuple = path_formatted, row_formatted
                        tuple_already_exists = False
                        my_list = scenesForTilePathLU[key]
                        for tuple1 in my_list:
                            if tuple1 == tuple:
                                tuple_already_exists = True
                        if not tuple_already_exists:
                            my_list.append(tuple)
                if south_south_scene_coords != "":
                    if geom2.Intersects(south_south_scene_geometry): 

                        row_formatted = '{0:03d}'.format(southsouthRow)
                        tuple = path_formatted, row_formatted
                        tuple_already_exists = False
                        my_list = scenesForTilePathLU[key]
                        for tuple1 in my_list:
                            if tuple1 == tuple:
                                tuple_already_exists = True
                        if not tuple_already_exists:
                            my_list.append(tuple)
        dataSource = None

    logger.info('Tile list: {0}'.format(tile_list))
    logger.info('scenesForTilePathLU: {0}'.format(scenesForTilePathLU))

    return tile_list, scenesForTilePathLU



# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Command line argument parsing
#
def readCmdLn(argv, logger):
    logger.info('argv count: {0}'.format(len(argv)))
    logger.info('argv : {0}'.format(argv))

    if len(argv) != 3:
       logger.info("need one command line argument")
       logger.info("example:  ARD_Clip.py '[('2013-07-09',40,29,")
       logger.info("                          '/hsm/lsat1/IT/collection01/etm/A1_L2/2012/33/36/LE07_L1TP_033036_20121220_20160909_01_A1.tar.gz',")
       logger.info("                          'LE07_L1TP_033036_20121220_20160909_01_A1'),")
       logger.info("                        ('2013-07-09',40,30,")
       logger.info("                          '/hsm/lsat1/IT/collection01/etm/A1_L2/2012/37/37/LE07_L1TP_037037_20121216_20160909_01_A1.tar.gz',")
       logger.info("                          'LE07_L1TP_037037_20121216_20160909_01_A1')]")
       logger.info("                        /hsm/lsat1/ST/collection01/lta_incoming")
       exit (0)
 
    try:
       segment = ast.literal_eval(argv[1])
       output_path = argv[2]
       return (segment, output_path)
    except Exception as e:
       logger.error('        Error: {0}'.format(e))
       sys.exit(0)


# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Reads values from config file
#
def readConfig(logger):

    logger.debug("readConfig")
    Config = ConfigParser.ConfigParser()
    if len(Config.read("ARD_Clip.conf")) > 0:
        logger.debug("Found ARD_Clip.conf")
        section = 'SectionOne'
        if Config.has_section(section):
           if Config.has_option(section, 'dbconnect'):
              connstr = Config.get(section, 'dbconnect')
           if Config.has_option(section, 'version'):
              version = Config.get(section, 'version')
              logger.info("version: {0}".format(version))
           if Config.has_option(section, 'soap_envelope_template'):
              soap_envelope = Config.get(section, 'soap_envelope_template')
           if Config.has_option(section, 'debug'):
              debug = Config.getboolean(section, 'debug')
    return connstr, version, soap_envelope, debug

# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Request required L2 scene bundles be moved to fastest cache
#                     available.
#
def stageFiles(segment,soap_envelope, logger):
    try:
       logger.info('Start staging...')
       url="https://dds.cr.usgs.gov/HSMServices/HSMServices?wsdl"
       headers = {'content-type': 'text/xml'}

       files = ''
       for scene_record in segment:
           files = files + '<files>' + scene_record[3] + '</files>'
       soap_envelope = soap_envelope.replace("#################", files)

       logger.debug('SOAP Envelope: {0}'.format(soap_envelope))
       request_object = urllib2.Request(url, soap_envelope, headers)

       response = urllib2.urlopen(request_object)

       html_string = response.read()
       logger.info('Stage response: {0}'.format(html_string))
    except Exception as e:
       logger.warning('Error staging files: {0}.  Continue anyway.'.format(e))

    else:
       logger.info('Staging succeeded...')


# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Insert a tile record into the ARD_COMPLETED_TILES table
#
#
def insert_tile_record(connection, completed_tile_list, logger):

    try:
        logger.info('Insert tile into ARD_COMPLETED_TILES table: {0}'.format(completed_tile_list))
        insert_cursor = connection.cursor()
        processed_tiles_insert = "insert /*+ ignore_row_on_dupkey_index(ARD_COMPLETED_TILES, TILE_ID_PK) */ into ARD_COMPLETED_TILES (tile_id,CONTRIBUTING_SCENES,COMPLETE_TILE,PROCESSING_STATE) values (:1,:2,:3,:4)"
        insert_cursor.bindarraysize = len(completed_tile_list)
        insert_cursor.prepare(processed_tiles_insert)
        insert_cursor.executemany(None, completed_tile_list)
        connection.commit()
    except:
        logger.error("insert_tile_record:  ERROR inserting into ARD_COMPLETED_TILES table")
        raise

    finally:
        insert_cursor.close()

# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Make file group wrietable
#
#
def make_file_group_writeable(filename):
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IWGRP)

# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Update the state of a scene record in the ARD_PROCESSED_SCENES
#             table
#
#
def update_scene_record(connection, scene_id, state, logger):

    try:
        logger.info('Update state in ARD_PROCESSED_SCENES table')
        logger.info('Scene ID: {0} State: {1}'.format(scene_id, state))
        update_cursor = connection.cursor()
        ard_processed_scenes_update = "update ARD_PROCESSED_SCENES set PROCESSING_STATE = :1, DATE_PROCESSED = sysdate where scene_id = :2"
        update_cursor.execute(ard_processed_scenes_update, (state, scene_id))
        connection.commit()
    except:
        logger.error("update_scene_record:  ERROR updating ARD_PROCESSED_SCENES table")
        raise

    finally:
        update_cursor.close()

# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Connect to the database
#
#
def db_connect(connstr, logger):
    try:
        return cx_Oracle.connect(connstr)
    except:
        logger.error("Error:  Unable to connect to the database.")
        sys.exit(1)

# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Disconnect from the database
#
#
def db_disconnect(connection):
    connection.close()
