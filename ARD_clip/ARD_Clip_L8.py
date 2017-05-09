#!/usr/bin/python

# ==========================================================================
#
#   ARD Processing Script
#
#   22 Mar 2017   -   0.1   -  test stack, Landsat 4,5,7 only 
#
# ==========================================================================
#
#  These variables should be checked for each "run" of this script.  They will affect filenames and file contents.
#
appVersion = "ARD Tile 1.0"                              # Currently unused... if used, would end up in the metadata
ARDversion = "_V01"                                           # Used in the filename for every tile band
connstr = 'L2_BRIDGE/L2b123@lsdsscant.cr.usgs.gov:1521/crdev'
soap_envelope = "Nothing"
debug = False                                        #true = output file,  false = stdout

				                                       # Landsat 8 crosswalk
filenameCrosswalk = [('toa_band1', 'TAB1'), ('toa_band2', 'TAB2'), ('toa_band3', 'TAB3'), \
                                 ('toa_band4', 'TAB4'), ('toa_band5', 'TAB5'), ('toa_band6', 'TAB6'), \
                                 ('toa_band7', 'TAB7'), ('toa_band9', 'TAB9'), \
                                 ('solar_azimuth_band4', 'SOA4'), ('solar_zenith_band4', 'SOZ4'), \
                                 ('sensor_azimuth_band4', 'SEA4'), ('sensor_zenith_band4', 'SEZ4'), \
                                 ('bt_band10', 'BTB10'), ('bt_band11', 'BTB11'), \
                                 ('sr_band1', 'SRB1'), ('sr_band2', 'SRB2'), ('sr_band3', 'SRB3'), \
                                 ('sr_band4', 'SRB4'), ('sr_band5', 'SRB5'), ('sr_band6', 'SRB6'), \
                                 ('sr_band7', 'SRB7'), ('pixel_qa', 'PIXELQA'), ('radsat_qa', 'RADSATQA'), \
                                 ('sr_aerosol', 'SRAEROSOLQA')]

				                                        # Define how each band will be processed
bandType01 = [ 'toa_band1',  'toa_band2', 'toa_band3', 'toa_band4', 'toa_band5', 'toa_band6', \
                         'toa_band7', 'toa_band9', 'sr_band1',   'sr_band2',   'sr_band3',  'sr_band4', \
                         'sr_band5',   'sr_band6', 'sr_band7', 'bt_band10', 'bt_band11' ]
bandType02 = [ 'solar_azimuth_band4', 'solar_zenith_band4', 'sensor_azimuth_band4', \
                         'sensor_zenith_band4' ]
bandType03 = [ ]
bandType04 = [ 'radsat_qa', 'pixel_qa', 'sr_aerosol' ]
bandType05 = [ ]

#bandType01 = ['toa_band1', 'toa_band3', 'toa_band4', 'toa_band5']       # for testing purposes...

finalDirectoryLU = {
               '4': 'tm',
               '5': 'tm',
               '7': 'etm',
               '8': 'oli_tirs'
}

			                                        # Executable locations
pythonLoc = "/usr/bin/python2.7"
gdalwarpLoc = "/usr/local/bin/gdalwarp"
gdaladdoLoc = "/usr/local/bin/gdaladdo"
gdaltranslateLoc = "/usr/local/bin/gdal_translate"
gdalcalcLoc = "/usr/local/bin/gdal_calc.py"
gdalinfoLoc = "/usr/local/bin/gdalinfo"
gdalmergeLoc = "/usr/local/bin/gdal_merge.py"

#
# ==========================================================================

import os
import traceback
import tarfile
import datetime
import sys
import glob
import shutil
import hashlib
from subprocess import call
import ast
import cx_Oracle
import ConfigParser
import urllib2
import logging


from ARD_HelperFunctions import logIt, appendToLog, reportToStdout, getARDName
from ARD_HelperFunctions import getTileFootprintCoords, makeMetadataString, parseHistFile
from ARD_HelperFunctions import getProductionDateTime, parseSceneHistFile, setup_logging
from ARD_tilesForSceneLU import tilesForSceneLU
from ARD_scenesForTilePathLU import scenesForTilePathLU
from ARD_regionLU import pathrow2regionLU
from ARD_metadata import *

from CU_tileFootprints import CU_tileFootprints
from AK_tileFootprints import AK_tileFootprints
from HI_tileFootprints import HI_tileFootprints

# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Command line argument parsing
#
def readCmdLn(argv):
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
      exit (1)

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
def readConfig():

    logger.info("readConfig")
    Config = ConfigParser.ConfigParser()
    if len(Config.read("ARD_Clip.conf")) > 0:
        logger.info("Found ARD_Clip.conf")
        section = 'SectionOne'
        if Config.has_section(section):
           if Config.has_option(section, 'dbconnect'):
              global connstr
              connstr = Config.get(section, 'dbconnect')
           if Config.has_option(section, 'version'):
              global version
              version = Config.get(section, 'version')
              logger.info("version: {0}".format(version))
           if Config.has_option(section, 'soap_envelope_template'):
              global soap_envelope
              soap_envelope = Config.get(section, 'soap_envelope_template')
           if Config.has_option(section, 'debug'):
              global debug
              debug = Config.getboolean(section, 'debug')

# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Request required L2 scene bundles be moved to fastest cache
#                     available.
#
def stageFiles(segment,soap_envelope):
    try:
       logger.info('Start staging...')
       url="https://dds.cr.usgs.gov/HSMServices/HSMServices?wsdl"
       headers = {'content-type': 'text/xml'}

       files = ''
       for scene_record in segment:
           files = files + '<files>' + scene_record[3] + '</files>'
       soap_envelope = soap_envelope.replace("#################", files)

       logger.info('SOAP Envelope: {0}'.format(soap_envelope))
       request_object = urllib2.Request(url, soap_envelope, headers)

       response = urllib2.urlopen(request_object)

       html_string = response.read()
       logger.info('Stage response: {0}'.format(html_string))
    except Exception as e:
       logger.error('Error staging files: {0}.  Continue anyway.'.format(e))

    else:
       logger.info('Staging succeeded...')


# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Clips tiles from a list of contiguous scenes aka segment.
#
def processScenes(segment):


                             # Session log, if we are in debug mode
                                   
    currentTime = datetime.datetime.now()
    logger.info('Start...')

    process_date = currentTime.strftime('%Y%m%d')
    sceneCtr = 0
    for scene_record in segment:

        # update PROCESSING_STATE in ARD_PROCESSED_SCENES to 'INWORK'
        updatesql = "update ARD_PROCESSED_SCENES set PROCESSING_STATE = 'INWORK' where scene_id = '" + scene_record[4] + "'"
        update_cursor = connection.cursor()
        update_cursor.execute(updatesql)
        connection.commit()
        update_cursor.close()
        logger.info("Scene {0} is INWORK.".format(scene_record[4]))

        additionalSceneCleanUpList = []
                             # Current scene to process
        #reportToStdout scene_record
        targzName = scene_record[4] + '.tar.gz'
        acqdate = scene_record[0]

        # LE07_L1TP_033037_20130902_20160907_01_A1
        targzMission = targzName[:4]
        targzPath = targzName[10:13]
        targzRow = targzName[13:16]
        targzYear = acqdate[:4]
        scene_id_parts = scene_record[4].split("_")

        logger.info("targzMission: {0} targzPath: {1} targzRow: {2} targzYear: {3}".format(targzMission, targzPath,targzRow, targzYear))


                             # Intersect scene with tile index to determine which tiles must be produced

        reqdTilesHV = tilesForSceneLU[targzPath+targzRow]
        logger.info("path: {0} row: {1}".format(targzPath,targzRow))
        region = pathrow2regionLU[targzPath+targzRow]
        logger.info("region: {0}".format(region))


        logger.info('Number of tiles to create: {0}'.format(len(reqdTilesHV)))

                             # Big loop for each tile needed

        for curTile in reqdTilesHV:

            productionDateTime = getProductionDateTime()
            tileErrorHasOccurred = False 

            if region == 'CU':
                cutLimits = getTileFootprintCoords(curTile, CU_tileFootprints)
            elif region == 'AK':
                cutLimits = getTileFootprintCoords(curTile, AK_tileFootprints)
            elif region == 'HI':
                cutLimits = getTileFootprintCoords(curTile, HI_tileFootprints)

            tile_id = scene_id_parts[0] + '_' + region + '_' + curTile[0]+curTile[1] + '_' + scene_id_parts[3] + '_' + process_date + '_C' + scene_id_parts[5] + ARDversion
            logger.info("tileid: {0}".format(tile_id))
            logger.info("cutlimits: {0}".format(cutLimits))

            # See if tile_id exists in ARD_COMPLETED_TILES table

            SQL="select tile_id, contributing_scenes, complete_tile from ARD_COMPLETED_TILES where tile_id = '" + tile_id + "'"
 
            #reportToStdout(SQL)
 
            cursor = connection.cursor()
            cursor.execute(SQL)
            tile_rec = cursor.fetchall()
            logger.info("Tile record: {0}".format(tile_rec))
            cursor.close()

            # db tile record check
            if len(tile_rec) < 1:
                logger.info("create tile")

                
                # Intersect this tile with the scenes to determine which
                # scenes will contribute to the tile.   Unpack each tar file 
                # if necessary.
                scenesForTilePath = scenesForTilePathLU[curTile[0]+curTile[1]+targzPath]
                logger.info('# Scenes needed for tile: {0}: {1}'.format(tile_id,len(scenesForTilePath)))
         
                contributingScenes = []
                contributingScenesforDB = []
                hsmFileNames = []
                hsmSceneID = []
                for pathRow in scenesForTilePath:
                    # LE07_L2TP_026028_19990709_20161112_01_A1
                    contrib_scene_id = targzName[:10] + pathRow[0] + pathRow[1] + targzName[16:25]
                    logger.info("          Contributing scene: {0}".format(contrib_scene_id))
                    hsm_wildcard_name = "/" + pathRow[0].lstrip("0") + "/" + pathRow[1].lstrip("0") + "/" + targzName[:4] + pathRow[0] + pathRow[1] + targzName[17:25] + "*.tar.gz"
                    logger.info("          HSM wildcard name: {0}".format(hsm_wildcard_name))

                    SQL="select file_location, scene_id from ARD_PROCESSED_SCENES where scene_id like '" + contrib_scene_id + "%'"
                    select_cursor = connection.cursor()
                    select_cursor.execute(SQL)
                    contrib_scene_rec = select_cursor.fetchall()
                    logger.info("Contributing scene record: {0}".format(contrib_scene_rec))
                    select_cursor.close()


                   # db scene record check
                    if len(contrib_scene_rec) > 0:
                        contributingScenes.append(contrib_scene_rec[0][0])
                        contributingScenesforDB.append(contrib_scene_rec[0][1])
                    else:
                        hsmFileNames.append(hsm_wildcard_name)
                        hsmSceneID.append(contrib_scene_id + '_' + scene_id_parts[4] + '_' + scene_id_parts[5] + '_' + scene_id_parts[6])


                complete_tile = 'Y'
                parsed_list = contributingScenes[0].rsplit("/",3)
                if len(contributingScenes) != len(scenesForTilePath):

                    # see if contributing file exists on hsm
                    fileFoundCtr = 0
                    loopCtr = 0
                    for hsm_wildcard_name in hsmFileNames:
                       hsm_full_wildcard = parsed_list[0] + hsm_wildcard_name
                       fullName = glob.glob(hsm_full_wildcard)
                       if len(fullName) > 0:
                          fileFoundCtr = fileFoundCtr + 1
                          contributingScenes.append(fullName[0])
                          contributingScenesforDB.append(hsmSceneID[loopCtr])
                          additionalSceneCleanUpList.append(hsmSceneID[loopCtr])
                       loopCtr = loopCtr + 1

                    if len(hsmFileNames) != fileFoundCtr:    
                       complete_tile = 'N'

                logger.info("Contributing scenes: {0}".format(contributingScenes))

   
                logger.info('Tile: {0}  - Number of contributing scenes: {1}'.format(tile_id, len(contributingScenes)))
   
                                # Incorrect number of contributing scenes, continue to next tile
   
                if (len(contributingScenes) > 3) or (len(contributingScenes) < 1):
                    logger.info('Skipping tile - Unexpected number of scenes = {0}'.format(len(contributingScenes)))
                    tileErrorHasOccurred = True
                    continue
   
                sceneInfoList = []
                stackA_metadataName = ''
                stackB_metadataName = ''
                stackC_metadataName = ''
       
   #     ----------- Start of loop gathering information about each contributing scene for this particular tile                            
   
                                 # If each contributing scene is not already unpacked, do it here
       
                contribSceneCtr = 0
                for thisTar in contributingScenes:

                    # Cleanup unneeded scene directories to save space
                    # Results of this check should keep no more than 3 scenes
                    # in work directory at a time
                    unneededSceneIdx = sceneCtr - 2
                    if unneededSceneIdx > -1:
                        unneededScene = segment[unneededSceneIdx][4]
                        if (os.path.isdir(unneededScene)):
                            shutil.rmtree(unneededScene)
                 
                    logger.info('Required Scene: {0}'.format(thisTar))
   
                    sceneDir = contributingScenesforDB[contribSceneCtr]
                    contribSceneCtr = contribSceneCtr + 1
   
                    if (not os.path.isdir(sceneDir)):
                        if (debug):
                            logger.info('Scene directory does not exist.  Will try to create: ' + sceneDir)
   
                        try:
                            os.makedirs(sceneDir)
                        except:
                            logger.error('Error creating scene directory: {0}'.format(sceneDir ))
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
   
                        logger.info('Created scene directory: {0}'.format(sceneDir))
   
                        try:
                            tar = tarfile.open(thisTar)
                            tar.extractall(sceneDir)
                            tar.close()
                        except:
                            logger.error('Error un-tarring: {0}'.format(thisTar))
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
                        logger.info('End unpacking tar')
   
                                # Get the scene prefix information from the metadata filename in each scene directory
   
                    sceneMetadataName = os.path.join(sceneDir, '*.xml')
                    sceneMetadataList = glob.glob(sceneMetadataName)
                    sceneMetadataPath = sceneMetadataList[0]
     
                                                             #LE07_L1TP_045028_20161016_20161020_01_A1_toa_band1
   
                    tp, scenePrefix = os.path.split(sceneMetadataPath)
                    scenePrefix = scenePrefix.replace(".xml", "")
                    logger.info('scenePrefix = {0}'.format(scenePrefix))
   
                                   # The sceneTuple objects are created for each contributing scene
        
                    scenePathString = targzPath
                    sceneRowString = sceneDir[13:16]
                    scenePathInt = int(scenePathString)
                    sceneRowInt = int(sceneRowString)
                    sceneTuple = (sceneDir, scenePrefix, scenePathInt, sceneRowInt, sceneMetadataName)
                    sceneInfoList.append(sceneTuple)
                    #print sceneTuple
   
   #     -----------  End of loop gathering information about each contributing scene for this particular tile
   
   
                                              # This tile info is the same regardless of which scenes will contribute.
                                              # We will use this tile information for building various filenames
   
                firstTuple = sceneInfoList[0]
          
                acqMonth = (firstTuple[1])[21:23]
                acqDay = (firstTuple[1])[23:25]
                collectionNumber = (firstTuple[1])[35:37]
                collectionCategory = (firstTuple[1])[38:40]
                firstTupleRow = firstTuple[3]
      
                logger.info('Starting to build tile: {0}'.format(tile_id))
      
                                   # Determine which scene(s) will overlay the other scene(s) for this tile.
                                   # North scenes (lower row #) will always overlay South scenes (higher row #)
                                   #
                                   # In the code, we will refer to the various scenes as:
                                   #     stackA  - the northern-most contributing scene
                                   #     stackB  - the next underlying contributing scene
                                   #     stackC  - the bottom contributing scene if there are 3
      
                if (len(sceneInfoList) == 1):
                    numScenesPerTile = 1
                    stackA_Prefix = sceneInfoList[0][1]
                    stackA_Dir = sceneInfoList[0][0]
                    stackA_Value = 1                                           
                    stackA_metadataName = sceneInfoList[0][4]      

                    logger.info('Only one contributing scene: {0}'.format(stackA_Prefix))
      
                elif (len(sceneInfoList) == 2):
                    numScenesPerTile = 2
                    secondTuple = sceneInfoList[1]
                    secondTupleRow = secondTuple[3]
                    if (firstTupleRow < secondTupleRow):
                        stackA_Prefix = sceneInfoList[0][1]
                        stackB_Prefix = sceneInfoList[1][1]
                        stackA_Dir = sceneInfoList[0][0]
                        stackB_Dir = sceneInfoList[1][0]
                        stackA_Value = 1
                        stackB_Value = 2
                        stackA_MetadataName = sceneInfoList[0][4]
                        stackB_MetadataName = sceneInfoList[1][4]
                    else:
                        stackA_Prefix = sceneInfoList[1][1]
                        stackB_Prefix = sceneInfoList[0][1]
                        stackA_Dir = sceneInfoList[1][0]
                        stackB_Dir = sceneInfoList[0][0]
                        stackA_Value = 2
                        stackB_Value = 1
                        stackA_MetadataName = sceneInfoList[1][4]
                        stackB_MetadataName = sceneInfoList[0][4]
      
                    logger.info('2 contributing scenes.')
                    logger.info('Contributing North scene: {0}'.format(stackA_Prefix))
                    logger.info('Contributing South scene: {0}'.format(stackB_Prefix))
      
                else:
                    numScenesPerTile = 3
                    secondTuple = sceneInfoList[1]
                    thirdTuple = sceneInfoList[2]
                    secondTupleRow = secondTuple[3]
                    thirdTupleRow = thirdTuple[3]
      
                    tileList = [firstTupleRow, secondTupleRow, thirdTupleRow]
                    tileList2 = [firstTupleRow, secondTupleRow, thirdTupleRow]
                    listMin = min(tileList)
                    minPos = tileList.index(listMin)
                    listMax = max(tileList)
                    maxPos = tileList.index(listMax)
                    tileList.remove(listMin)
                    tileList.remove(listMax)
                    listMid = tileList[0]
                    midPos = tileList2.index(listMid)
      
                    stackA_Prefix = sceneInfoList[minPos][1]
                    stackB_Prefix = sceneInfoList[midPos][1]
                    stackC_Prefix = sceneInfoList[maxPos][1]
                    stackA_Dir = sceneInfoList[minPos][0]
                    stackB_Dir = sceneInfoList[midPos][0]
                    stackC_Dir = sceneInfoList[maxPos][0]
                    stackA_Value = 1
                    stackB_Value = 2
                    stackC_Value = 3
                    stackA_MetadataName = sceneInfoList[minPos][4]
                    stackB_MetadataName = sceneInfoList[midPos][4]
                    stackC_MetadataName = sceneInfoList[maxPos][4]
             
                    logger.info('3 contributing scenes.')
                    logger.info('Northern most: {0}'.format(stackA_Prefix))
                    logger.info('       Middle: {0}'.format(stackB_Prefix))
                    logger.info('Southern most: {0}'.format(stackC_Prefix))
           
             
                                   # Set up the tile output directory
      
                #tileDir = os.path.join(curDir, 'tile_' + curTile[0] + curTile[1] + '_' + targzYear + acqMonth + acqDay)
                tileDir = tile_id
          
                if (not os.path.isdir(tileDir)):
                    try:
                        logger.info('Tile directory does not exist.  Will try to create: {0}'.format(tileDir))
                        os.makedirs(tileDir)
                    except:
                        logger.error('Error creating tile directory: {0}'.format(tileDir))
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue
      
                    logger.info('Created tile directory: {0}'.format(tileDir))
      
                                                               # these lists will hold the final files for each tar file
                toaFinishedList = []
                btFinishedList = []
                srFinishedList = []
                qaFinishedList = []
                browseList = []                                  # this list holds the 3 required (RGB) layers
             
                                                                # set lineage names when making toa_band1, 
                                                                # but build the actual tif late in the processing
                lineage01Name = ''
                lineage02Name = ''
                lineage03Name = ''
                lineageFileName = ''
                lineageFullName = ''
      
                                                                # these will get set in the Band Type 4 processing area
                pqaTileStart = ''
                pqaCirrusMask = ''
                pqaLowerBits = ''
                pqaCloudMask = ''
                pqaCloudCirrusMask = ''
                histCloudCirrus = ''
                histLowerBits = ''



# --------------------------------------------------------------------------------------------------------------------------- #
#
#     Band Type 1
#
#                   16 bit signed integer
#     		NoData = -9999 
#
#
                logger.info('band type 1')
                for curBand in bandType01:
      
                    if (tileErrorHasOccurred):
                        continue
                    logger.info('     Start processing for band: {0}'.format(curBand))
      
                    clipParams = ' -dstnodata "-9999" -srcnodata "-9999" '
                    clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                    newARDname = getARDName(curBand, filenameCrosswalk)
                    if (newARDname == 'ERROR'):
                        logger.error('Error in filenameCrosswalk for: {0}'.format(curBand))
                        tileErrorHasOccurred = True
                        continue
                    mosaicFileName = tile_id + '_' + newARDname + '.tif'
                    lineageTifName = tile_id + '_LINEAGEQA.tif'
                    lineageFullName = os.path.join(tileDir, lineageTifName)
      
                    if (numScenesPerTile == 1):                                     # 1 contributing scene
                        sceneFullName = os.path.join(stackA_Dir, stackA_Prefix + '_' + curBand + '.tif')
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          sceneFullName + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        Single scene command: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 1 contributing scene')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
                                                                   
                        if (curBand == 'toa_band1'):                                 # needed for lineage file generation later
                            lineage01Name = mosaicFullName
      
      
                    elif (numScenesPerTile == 2):                                     # 2 contributing scenes
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        southFilename = stackB_Prefix + '_' + curBand + '.tif'
      
                        northFullname = os.path.join(stackA_Dir, northFilename)
                        southFullname = os.path.join(stackB_Dir, southFilename)
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)
      
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          southFullname + ' ' + northFullname + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        Two scene command: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                        if (curBand == 'toa_band1'):                                    # needed for lineage file generation later
                            lineage01 = northFullname
                            lineage02 = southFullname
      
                    else:                                                                          # 3 contributing scenes
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        midFilename = stackB_Prefix + '_' + curBand + '.tif'
                        southFilename = stackC_Prefix + '_' + curBand + '.tif'
      
                        northFullname = os.path.join(stackA_Dir, northFilename)
                        midFullname = os.path.join(stackB_Dir, midFilename)
                        southFullname = os.path.join(stackC_Dir, southFilename)
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)
      
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + southFullname + \
                                          ' ' + midFullname + ' ' + northFullname + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        Three scene command: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 3 contributing scenes')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                        if (curBand == 'toa_band1'):                                    # needed for lineage file generation later
                            lineage01 = northFullname
                            lineage02 = midFullname
                            lineage03 = southFullname
      
                    logger.info('    End processing for: {0}'.format(curBand))

                    if (curBand == 'toa_band1') or (curBand == 'toa_band2') or (curBand == 'toa_band3') or \
                       (curBand == 'toa_band4') or (curBand == 'toa_band5') or (curBand == 'toa_band6') or \
                       (curBand == 'toa_band7') or (curBand == 'toa_band9') or \
                       (curBand == 'solar_azimuth_band4') or (curBand == 'solar_zenith_band4') or \
                       (curBand == 'sensor_azimuth_band4') or (curBand == 'sensor_zenith_band4'):
                        toaFinishedList.append(mosaicFileName)

                    if (curBand == 'sr_band1') or (curBand == 'sr_band2') or (curBand == 'sr_band3') or \
                       (curBand == 'sr_band4') or (curBand == 'sr_band5') or (curBand == 'sr_band6') or \
                       (curBand == 'sr_band7'):
                        srFinishedList.append(mosaicFileName)

                    if (curBand == 'bt_band10') or (curBand == 'bt_band11'):
                      btFinishedList.append(mosaicFileName)
  
                                                                      # save for browse later
                    if (curBand == 'toa_band4') or (curBand == 'toa_band5') or (curBand == 'toa_band6'):
                        browseList.append(mosaicFileName)
      
      
# --------------------------------------------------------------------------------------------------------------------------- #
#
#     Band Type 2
#
#                   16 bit signed integer
#     		NoData = -32768
#     
                logger.info('band type 2')
                for curBand in bandType02:
                      
                    if (tileErrorHasOccurred):
                        continue
                    logger.info('     Start processing for band: {0}'.format(curBand))
      
                    clipParams = ' -dstnodata "-32768" -srcnodata "-32768" '
                    clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                    newARDname = getARDName(curBand, filenameCrosswalk)
                    if (newARDname == 'ERROR'):
                        logger.error('Error in filenameCrosswalk for: {0}'.format(curBand))
                        tileErrorHasOccurred = True
                        continue
                    mosaicFileName = tile_id + '_' + newARDname + '.tif'
      
      
                    if (numScenesPerTile == 1):                                     # 1 contributing scene
                        sceneFullName = os.path.join(stackA_Dir, stackA_Prefix + '_' + curBand + '.tif')
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          sceneFullName + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        Single scene command: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 1 contributing scene')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
      
                    elif (numScenesPerTile == 2):                                     # 2 contributing scenes
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        southFilename = stackB_Prefix + '_' + curBand + '.tif'

                        northFullname = os.path.join(stackA_Dir, northFilename)
                        southFullname = os.path.join(stackB_Dir, southFilename)
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)

                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          southFullname + ' ' + northFullname + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        Two scene command: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
      
                    else:                                                                          # 3 contributing scenes
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        midFilename = stackB_Prefix + '_' + curBand + '.tif'
                        southFilename = stackC_Prefix + '_' + curBand + '.tif'

                        northFullname = os.path.join(stackA_Dir, northFilename)
                        midFullname = os.path.join(stackB_Dir, midFilename)
                        southFullname = os.path.join(stackC_Dir, southFilename)
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)

                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + southFullname + \
                                          ' ' + midFullname + ' ' + northFullname + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        Three scene command: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 3 contributing scenes')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                    logger.info('    End processing for: {0}'.format(curBand))
      
      
                    if (curBand == 'solar_azimuth_band4') or (curBand == 'solar_zenith_band4') or \
                       (curBand == 'sensor_azimuth_band4') or (curBand == 'sensor_zenith_band4'):
                        toaFinishedList.append(mosaicFileName)
      
      # --------------------------------------------------------------------------------------------------------------------------- #
      #
      #     Band Type 3
      #
      #     		16 bit unsigned integer
      #		NoData = 1  in ovelap areas and scan gaps
      #
      #     
                logger.info('band type 3')
                for curBand in bandType03:
                     
                    if (tileErrorHasOccurred):
                        continue
                    logger.info('    Start processing for: {0}'.format(curBand))
  
                    clipParams = ' -dstnodata "1" -srcnodata "1" '
                    tempFileName = targzMission + '_' + collectionLevel + '_' + curTile[0] + curTile[1] + \
                                             '_' + targzYear + acqMonth + acqDay + '_' + curBand + '_m0.tif'
                    tempFullName = os.path.join(tileDir, tempFileName)
      
                    if (numScenesPerTile == 1):                                                     # 1 contributing scene
                        inputFileName = stackA_Prefix + '_' + curBand + '.tif'
                        inputFullName = os.path.join(stackA_Dir, inputFileName)
      
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + inputFullName + ' '  + tempFullName
                        if (debug):
                            logger.info('        mosaic: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 1 contributing scene')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                    elif (numScenesPerTile == 2):                                                   # 2 contributing scenes
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        southFilename = stackB_Prefix + '_' + curBand + '.tif'
      
                        northFullname = os.path.join(stackA_Dir, northFilename)
                        southFullname = os.path.join(stackB_Dir, southFilename)
      
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                    southFullname + ' ' + northFullname + ' ' + tempFullName
                        if (debug):
                            logger.info('        mosaic: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                    else:                                                                                        # 3 contributing scenes
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        midFilename = stackB_Prefix + '_' + curBand + '.tif'
                        southFilename = stackC_Prefix + '_' + curBand + '.tif'
      
                        northFullname = os.path.join(stackA_Dir, northFilename)
                        midFullname = os.path.join(stackB_Dir, midFilename)
                        southFullname = os.path.join(stackC_Dir, southFilename)
      
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + southFullname + \
                                          ' ' + midFullname + ' ' + northFullname + ' ' + tempFullName
                        if (debug):
                            logger.info('        mosaic: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 3 contributing scenes')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # reassign nodata
                    clipParams = ' -dstnodata "0" -srcnodata "None" '
                    clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                    mosaicFileName = targzMission + '_' + collectionLevel + '_' + curTile[0] + curTile[1] + \
                                               '_' + targzYear + acqMonth + acqDay + '_' + curBand + '.tif'
                    mosaicFullName = os.path.join(tileDir, mosaicFileName)
      
                    warpCmd = gdalwarpLoc + clipParams + tempFullName + ' ' + mosaicFullName
                    if (debug):
                        logger.info('        reassign nodata & compress: {0}'.format(warpCmd))
                    try:
                        returnValue = call(warpCmd, shell=True)
                    except:
                        logger.error('Error: warpCmd - nodata')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue

                    logger.info('    End processing for: {0}'.format(curBand))
      
                    finishedMosaicList.append(mosaicFileName)
      
      
# --------------------------------------------------------------------------------------------------------------------------- #
#
#     Band Type 4
#
#     In the source scenes, these bands are:
#
#		8 bit unsigned integer
#                   NoData = 1   in overlap area and within scene footprint
#		Valid data = 0 - 255 
#
#      
                logger.info('band type 4')
                for curBand in bandType04:
                      
                    if (tileErrorHasOccurred):
                        continue
                    logger.info('    Start processing for: {0}'.format(curBand))
      
                    newARDname = getARDName(curBand, filenameCrosswalk)
                    if (newARDname == 'ERROR'):
                        logger.error('Error in filenameCrosswalk for: {0}'.format(curBand))
                        tileErrorHasOccurred = True
                        continue
                    baseName = tile_id + '_' + newARDname
      
                    if (numScenesPerTile == 1):                                                         # 1 contributing scene
                        inputFileName = stackA_Prefix + '_' + curBand + '.tif'
                        inputFullName = os.path.join(stackA_Dir, inputFileName)
      
                        clipParams = ' -dstnodata "1" -srcnodata "1" '
                        clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
      
                        mosaicFileName = baseName + '.tif'
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + inputFullName + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        mosaic: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 1 contributing scene')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                    elif (numScenesPerTile == 2):                                                       # 2 contributing scenes
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        southFilename = stackB_Prefix + '_' + curBand + '.tif'
      
                        northFullname = os.path.join(stackA_Dir, northFilename)
                        southFullname = os.path.join(stackB_Dir, southFilename)
      
                        clipParams = ' -dstnodata "1" -srcnodata "1" '
      
                                                                    # North - Clip to fill entire tile
                        tempName0 =  baseName + '_n0.tif'
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          northFullname + ' ' + os.path.join(tileDir, tempName0)
                        if (debug):
                            logger.info('        north0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes - north')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # South - Clip 2nd only
                        tempName1 = baseName + '_s0.tif'
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          southFullname + ' ' + os.path.join(tileDir, tempName1)
                        if (debug):
                            logger.info('        south0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes - south')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # mosaic
                        clipParams = '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                        mosaicFileName = baseName + '.tif'
                        warpCmd = gdalwarpLoc + ' ' + clipParams + os.path.join(tileDir, tempName1) + ' ' + \
                                          os.path.join(tileDir, tempName0) + ' ' + os.path.join(tileDir, mosaicFileName)
                        if (debug):
                            logger.info('        mosaic0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes - mosaic')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                    else:                                                                                             # 3 contributing scenes
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        midFilename = stackB_Prefix + '_' + curBand + '.tif'
                        southFilename = stackC_Prefix + '_' + curBand + '.tif'
      
                        northFullname = os.path.join(stackA_Dir, northFilename)
                        midFullname = os.path.join(stackB_Dir, midFilename)
                        southFullname = os.path.join(stackC_Dir, southFilename)
      
                        clipParams = ' -dstnodata "1" -srcnodata "1" '
      
                                                                    # North - Clip to fill entire tile
                        tempName0 =  baseName + '_n0.tif'
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          northFullname + ' ' + os.path.join(tileDir, tempName0)
                        if (debug):
                            logger.info('        north0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 3 contributing scenes - north')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # Middle - Clip 2nd only
                        tempName1 = baseName + '_mid.tif'
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          midFullname + ' ' + os.path.join(tileDir, tempName1)
                        if (debug):
                            logger.info('        mid0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes - middle')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # South - Clip 3rd only
                        tempName2 = baseName + '_s0.tif'
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          southFullname + ' ' + os.path.join(tileDir, tempName2)
                        if (debug):
                            logger.info('        south0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes - south')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # mosaic
                        clipParams = '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                        mosaicFileName = baseName + '.tif'
                        warpCmd = gdalwarpLoc + ' ' + clipParams + os.path.join(tileDir, tempName2) + ' ' + \
                                          os.path.join(tileDir, tempName1) + ' ' + os.path.join(tileDir, tempName0) + \
                                         ' ' + os.path.join(tileDir, mosaicFileName)
                        if (debug):
                            logger.info('        mosaic: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes - mosaic')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                    if (curBand == 'pixel_qa'):
                        pqaTileStart = os.path.join(tileDir, mosaicFileName)
                        pqaCirrusMask = pqaTileStart.replace('PIXELQA', 'pqaCirrusMask')
                        pqaLowerBits = pqaTileStart.replace('PIXELQA', 'pqaLowerBits')
                        pqaCloudMask = pqaTileStart.replace('PIXELQA', 'pqaCloudMask')
                        pqaCloudCirrusMask = pqaTileStart.replace('PIXELQA', 'pqaCloudCirrusMask')
                        histCloudCirrus = os.path.join(tileDir, 'histCloudCirrus.json')
                        histLowerBits = os.path.join(tileDir, 'histLowerBits.json')

                    if (curBand == 'radsat_qa') or (curBand == 'pixel_qa'):
                        toaFinishedList.append(mosaicFileName)
                        btFinishedList.append(mosaicFileName)
                        srFinishedList.append(mosaicFileName)
                        qaFinishedList.append(mosaicFileName)

                    if (curBand == 'sr_aerosol'):
                        srFinishedList.append(mosaicFileName)
                        qaFinishedList.append(mosaicFileName)

                    logger.info('    End processing for: {0}'.format(curBand))
      
      
      # --------------------------------------------------------------------------------------------------------------------------- #
      #
      #                                                       Create the lineage file
      #
      #
                logger.info('Creating lineage file')
                if (tileErrorHasOccurred):
                    continue
      
                lineageFileName = tile_id + '_LINEAGEQA.tif'

                                                                        
                if (numScenesPerTile == 1):                                   # 1 contributing scene

                    calcExpression = ' --calc="' + str(stackA_Value) + ' * (A > -1)"'
                    lineageTempFullName = lineageFullName.replace('.tif', '_linTemp.tif')

                    lineageCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + lineage01Name + ' --outfile ' + \
                                         lineageTempFullName + calcExpression + ' --type="Byte" --NoDataValue=0'
                    if (debug):
                        logger.info('        mosaic lineage command: {0}'.format(lineageCmd))
                    try:
                        returnValue = call(lineageCmd, shell=True)
                    except:
                        logger.error('Error: calcCmd - 1 contributing scene')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue
      
                                                                                            # 1 contributing scene-  compress
                    clipParams = ' -co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                    warpCmd = gdalwarpLoc +  clipParams + lineageTempFullName + ' ' + lineageFullName
                    if (debug):
                        logger.info('        compress lineage command: {0}'.format(warpCmd))
                    try:
                        returnValue = call(warpCmd, shell=True)
                    except:
                        logger.error('Error: warpCmd - 1 contributing scene')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue
      
                elif (numScenesPerTile == 2):                                      # 2 contributing scenes - north
                    northCalcExp = ' --calc="' + str(stackA_Value) + ' * (A > -1)"'
                    northTempName = lineageFullName.replace('.tif', '_srcTempN.tif')
                    northLineageCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + lineage01 + \
                                                 ' --outfile ' + northTempName + northCalcExp + \
                                                 ' --type="Byte" --NoDataValue=0'
                    if (debug):
                        logger.info('        north lineage conmmand: {0}'.format(northLineageCmd))
                    try:
                        returnValue = call(northLineageCmd, shell=True)
                    except:
                        logger.error('Error: warpCmd - 2 contributing scenes - north')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue
      
                                                                                       # 2 contributing scenes - south
                    southCalcExp = ' --calc="' + str(stackB_Value) + ' * (A > -1)"'
                    southTempName = lineageFullName.replace('.tif', '_srcTempS.tif')
                    southLineageCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + lineage02 + \
                                                 ' --outfile ' + southTempName + southCalcExp + \
                                                 ' --type="Byte" --NoDataValue=0'
                    if (debug):
                        logger.info('        south lineage conmmand: {0}'.format(southLineageCmd))
                    try:
                        returnValue = call(southLineageCmd, shell=True)
                    except:
                        logger.error('Error: warpCmd - 2 contributing scenes - south')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue
      
                                                                                        # 2 contributing scenes - mosaic North over South
                    clipParams = ' -dstnodata "0" -srcnodata "0" -ot "Byte" -wt "Byte" '
                    clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                    warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                      southTempName + ' ' + northTempName + ' ' + lineageFullName
                    if (debug):
                        logger.info('        mosaic lineage command: {0}'.format(warpCmd))
                    try:
                        returnValue = call(warpCmd, shell=True)
                    except:
                        logger.error('Error: warpCmd - 2 contributing scenes - mosaic')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue
      
                else:                                                         # 3 contributing scenes - north
                    northCalcExp = ' --calc="' + str(stackA_Value) + ' * (A > -1)"'
                    northTempName = lineageFullName.replace('.tif', '_srcTempN.tif')
                    northLineageCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + lineage01 + \
                                                 ' --outfile ' + northTempName + northCalcExp + \
                                                 ' --type="Byte" --NoDataValue=0'
                    if (debug):
                        logger.info('        north lineage conmmand: {0}'.format(northLineageCmd))
                    try:
                        returnValue = call(northLineageCmd, shell=True)
                    except:
                        logger.error('Error: warpCmd - 3 contributing scenes - north')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue

                                                                    # 3 contributing scenes - middle
                    midCalcExp = ' --calc="' + str(stackB_Value) + ' * (A > -1)"'
                    midTempName = lineageFullName.replace('.tif', '_srcTempM.tif')
                    midLineageCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + lineage02 + \
                                               ' --outfile ' + midTempName + midCalcExp + \
                                               ' --type="Byte" --NoDataValue=0'
                    if (debug):
                        logger.info('        middle lineage conmmand: {0}'.format(midLineageCmd))
                    try:
                        returnValue = call(midLineageCmd, shell=True)
                    except:
                        logger.error('Error: warpCmd - 3 contributing scenes - middle')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue

                                                                    # 3 contributing scenes - south
                    southCalcExp = ' --calc="' + str(stackC_Value) + ' * (A > -1)"'
                    southTempName = lineageFullName.replace('.tif', '_srcTempS.tif')
                    southLineageCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + lineage03 + \
                                                  ' --outfile ' + southTempName + southCalcExp + \
                                                  ' --type="Byte" --NoDataValue=0'
                    if (debug):
                        logger.info('        south lineage conmmand: {0}'.format(southLineageCmd))
                    try:
                        returnValue = call(southLineageCmd, shell=True)
                    except:
                        logger.error('Error: warpCmd - 3 contributing scenes - south')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue

                                                                  #  3 contributing scenes - mosaic North, Middle, South
                    clipParams = ' -dstnodata "0" -srcnodata "0" -ot "Byte" -wt "Byte" '
                    clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                    warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + southTempName + \
                                     ' ' + midTempName + ' ' + northTempName + ' ' + lineageFullName
                    if (debug):
                        logger.info('        mosaic lineage command: {0}'.format(warpCmd))
                    try:
                        returnValue = call(warpCmd, shell=True)
                    except:
                        logger.error('Error: warpCmd - 3 contributing scenes - mosaic')
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        tileErrorHasOccurred = True
                        continue

                toaFinishedList.append(lineageFileName)
                btFinishedList.append(lineageFileName)
                srFinishedList.append(lineageFileName)
                qaFinishedList.append(lineageFileName)
    
                logger.info('    End building lineage file')


# --------------------------------------------------------------------------------------------------------------------------- #
#
#         Check to see how many "contributing" scenes were actually used.  We need this check because of the
#         end overlap between scenes.  For example, when we calculated the number of contributing scenes
#         earlier in the processing, we performed a simple intersect between the scene footprint and the tile 
#         footprint.  Because of end overlap, we may not have actually used a scene, even though it intersected
#         the tile.
#
#         For this check, find the highest value in the lineage file by calculating a histogram.  This will tell us 
#         how many scenes were  actually used for this tile.
#

                if (tileErrorHasOccurred):
                    continue
                logger.info('    Start checking contributing scenes')
    
                sceneHistFilename = os.path.join(tileDir, 'scenes.json')
    
                sceneCmd = gdalinfoLoc + ' -hist ' + lineageFullName + ' >> ' + sceneHistFilename
                if (debug):
                    logger.info('        > sceneCmd: {0}'.format(sceneCmd))
                try:
                    returnValue = call(sceneCmd, shell=True)
                except:
                    logger.error('Error: generating histogram from lineage file')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                numScenesPerTile = parseSceneHistFile(sceneHistFilename)

                if (numScenesPerTile == 0):
                    logger.error('Warning: parsing histogram from lineage file: 0 contributing scenes')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True

                    # Insert tile into db and record it as "NOT NEEDED" i.e it's an empty tile
                    processingState = "NOT NEEDED"

                    completed_tile_list = []
                    sceneListStr = "none"
                    row = (tile_id,sceneListStr,complete_tile,processingState)
                    completed_tile_list.append(row)

                    insert_tile_record(connection, completed_tile_list)

                    # Remove the temporary work directory
                    shutil.rmtree(tileDir)
                    continue

                logger.info('finish updating contributing scenes')

		    
# --------------------------------------------------------------------------------------------------------------------------- #
#
#     Band Type 5
#
#		8 bit unsigned integer
#         	NoData is not set
#     		Zeroes are in the overlap areas and  also within the image footprint
#
#                   Because there is no real NoData value, we can't differentiate between pixels within the
#                   footprint and pixels outside the footprint.  This is a problem in the overlap areas.  Therefore,
#                   we will use the lineage file to specify which pixels from these bands are derived from the 
#                   various input scenes.
#   
                logger.info('band type 5')
                for curBand in bandType05:
                   
                    if (tileErrorHasOccurred):
                        continue 
                    logger.info('    Start processing for: {0}'.format(curBand))
      
                    newARDname = getARDName(curBand, filenameCrosswalk)
                    if (newARDname == 'ERROR'):
                        logger.error('Error in filenameCrosswalk for: {0}'.format(curBand))
                        tileErrorHasOccurred = True
                        continue
                    baseName = tile_id + '_' + newARDname
      
                                                                        # 
                                                                        # Only 1 contributing scene
                                                                        # 
                                                                        # Perform a simple clip and then 
                                                                        # reassign any NoData back to zero
                                                                        #
                    if (numScenesPerTile == 1): 
                        inputFileName = stackA_Prefix + '_' + curBand + '.tif'
                        inputFullName = os.path.join(stackA_Dir, inputFileName)
                  
                        clipParams = ' -dstnodata "0" -ot "Byte" -wt "Byte" '
      
                                                                    # Clip to fill entire tile
                        tempFileName =  baseName + '_m0.tif'
                        tempFullName = os.path.join(tileDir, tempFileName)
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + inputFullName + ' ' + tempFullName
                        if (debug):
                            logger.info('        mosaic: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 1 contributing scene')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # reassign nodata & compress
                        clipParams = ' -dstnodata "None" -srcnodata "0" '
                        clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                        mosaicFileName = baseName + '.tif'
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)
                        warpCmd = gdalwarpLoc + clipParams + tempFullName + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        nodata: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - nodata')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                        # 
                                                                        # 2 contributing scenes 
                                                                        # 
                                                                        # Since there is no real NoData value in the source
                                                                        # scenes, we can't overlay the Northern scene on
                                                                        # top of the Southern scene without obscuring 
                                                                        # pixels in the overlap area.  
                                                                        #
                                                                        # We will use the newly created lineage tile to mask
                                                                        # only those pixels we want for each contributing
                                                                        # scene.  Then we can re-combine the results of the 
                                                                        # two mask operations.
                                                                        #

                    elif (numScenesPerTile == 2): 
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        southFilename = stackB_Prefix + '_' + curBand + '.tif'
      
                        northFullname = os.path.join(stackA_Dir, northFilename)
                        southFullname = os.path.join(stackB_Dir, southFilename)
      
                        clipParams = ' -dstnodata "0" -ot "Byte" -wt "Byte" '
      
                                                                    # North - Clip to fill entire tile
                        clipFileNameN =  baseName + '_n0.tif'
                        clipFullNameN = os.path.join(tileDir, clipFileNameN)
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          northFullname + ' ' + clipFullNameN
                        if (debug):
                            logger.info('        north0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes - north')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # South - Clip 2nd only
                        clipFileNameS = baseName + '_s0.tif'
                        clipFullNameS = os.path.join(tileDir, clipFileNameS)
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          southFullname + ' ' + clipFullNameS
                        if (debug):
                            logger.info('        south0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes - south')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue

                                                              # Mask the North using the lineage file
                        maskNameN = baseName + '_n0mask.tif'
                        maskFullNameN = os.path.join(tileDir, maskNameN)
                        calcExpression = ' --calc="A*(B==1)"'
                        maskCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + clipFullNameN + \
                                          ' -B ' + lineageFullName + ' --outfile ' + maskFullNameN + \
                                          calcExpression + ' --type="Byte" --NoDataValue=0'
                        if (debug):
                            logger.info('        north mask: {0}'.format(maskCmd))
                        try:
                            returnValue = call(maskCmd, shell=True)
                        except:
                            logger.error('Error: maskCmd - 2 contributing scenes - north')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue

                                                              # Mask the South using the lineage file
                        maskNameS = baseName + '_s0mask.tif'
                        maskFullNameS = os.path.join(tileDir, maskNameS)
                        calcExpression = ' --calc="A*(B==2)"'
                        maskCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + clipFullNameS + \
                                          ' -B ' + lineageFullName + ' --outfile ' + maskFullNameS + \
                                          calcExpression + ' --type="Byte" --NoDataValue=0'
                        if (debug):
                            logger.info('        south mask: {0}'.format(maskCmd))
                        try:
                            returnValue = call(maskCmd, shell=True)
                        except:
                            logger.error('Error: maskCmd - 2 contributing scenes - south')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue

                                                                    # mosaic
                        tempFileName =  baseName + '_m0.tif'
                        tempFullName = os.path.join(tileDir, tempFileName)
                        warpCmd = gdalwarpLoc + ' ' +  maskFullNameS + ' ' + \
                                          maskFullNameN + ' ' + tempFullName
                        if (debug):
                            logger.info('        mosaic: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 2 contributing scenes - mosaic')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue

                                                              # reassign nodata & compress
                        clipParams = ' -dstnodata "None" -srcnodata "0" '
                        clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                        mosaicFileName = baseName + '.tif'
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)
                        warpCmd = gdalwarpLoc + clipParams + tempFullName + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        nodata: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - nodata')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue

                                                                        # 
                                                                        # 3 contributing scenes 
                                                                        # 
                                                                        # Since there is no real NoData value in the source
                                                                        # scenes, we can't overlay the Northern scene on
                                                                        # top of the Southern scenes without obscuring 
                                                                        # pixels in the overlap area.  
                                                                        #
                                                                        # We will use the newly created lineage tile to mask
                                                                        # only those pixels we want for each contributing
                                                                        # scene.  Then we can re-combine the results of the 
                                                                        # three mask operations.
                                                                        #

                    else:                                                                                       
                        northFilename = stackA_Prefix + '_' + curBand + '.tif'
                        midFilename = stackB_Prefix + '_' + curBand + '.tif'
                        southFilename = stackC_Prefix + '_' + curBand + '.tif'
      
                        northFullname = os.path.join(stackA_Dir, northFilename)
                        midFullname = os.path.join(stackB_Dir, midFilename)
                        southFullname = os.path.join(stackC_Dir, southFilename)
      
                        clipParams = ' -dstnodata "0" -ot "Byte" -wt "Byte" '
      
                                                                    # North - Clip to fill entire tile
                        clipFileNameN =  baseName + '_n0.tif'
                        clipFullNameN = os.path.join(tileDir, clipFileNameN)
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          northFullname + ' ' + clipFullNameN
                        if (debug):
                            logger.info('        north0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 3 contributing scenes - north')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # Mid - Clip 2nd only
                        clipFileNameM =  baseName + '_mid.tif'
                        clipFullNameM = os.path.join(tileDir, clipFileNameM)
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                              midFullname + ' ' + clipFullNameM
                        if (debug):
                            logger.info('        mid0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 3 contributing scenes - middle')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # South - Clip 3rd only
                        clipFileNameS = baseName + '_s0.tif'
                        clipFullNameS = os.path.join(tileDir, clipFileNameS)
                        warpCmd = gdalwarpLoc + ' -te ' + cutLimits + clipParams + \
                                          southFullname + ' ' + clipFullNameS
                        if (debug):
                            logger.info('        south0: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 3 contributing scenes - south')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                              # Mask the North using the lineage file
                        maskNameN = baseName + '_n0mask.tif'
                        maskFullNameN = os.path.join(tileDir, maskNameN)
                        calcExpression = ' --calc="A*(B==1)"'
                        maskCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + clipFullNameN + \
                                          ' -B ' + lineageFullName + ' --outfile ' + maskFullNameN + \
                                          calcExpression + ' --type="Byte" --NoDataValue=0'
                        if (debug):
                            logger.info('        north mask: {0}'.format(maskCmd))
                        try:
                            returnValue = call(maskCmd, shell=True)
                        except:
                            logger.error('Error: maskCmd - 3 contributing scenes - north')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                              # Mask the Middle using the lineage file
                        maskNameM = baseName + '_midmask.tif'
                        maskFullNameM = os.path.join(tileDir, maskNameM)
                        calcExpression = ' --calc="A*(B==2)"'
                        maskCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + clipFullNameM + \
                                          ' -B ' + lineageFullName + ' --outfile ' + maskFullNameM + \
                                          calcExpression + ' --type="Byte" --NoDataValue=0'
                        if (debug):
                            logger.info('        middle mask: {0}'.format(maskCmd))
                        try:
                            returnValue = call(maskCmd, shell=True)
                        except:
                            logger.error('Error: maskCmd - 3 contributing scenes - middle')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                              # Mask the South using the lineage file
                        maskNameS = baseName + '_s0mask.tif'
                        maskFullNameS = os.path.join(tileDir, maskNameS)
                        calcExpression = ' --calc="A*(B==3)"'
                        maskCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + clipFullNameS + \
                                          ' -B ' + lineageFullName + ' --outfile ' + maskFullNameS + \
                                          calcExpression + ' --type="Byte" --NoDataValue=0'
                        if (debug):
                            logger.info('        south mask: {0}'.format(maskCmd))
                        try:
                            returnValue = call(maskCmd, shell=True)
                        except:
                            logger.error('Error: maskCmd - 3 contributing scenes - south')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # mosaic
                        tempFileName =  baseName + '_m0.tif'
                        tempFullName = os.path.join(tileDir, tempFileName)
                        warpCmd = gdalwarpLoc + ' ' + maskFullNameS + ' ' + maskFullNameM + \
                                         ' ' + maskFullNameN + ' ' + tempFullName
                        if (debug):
                            logger.info('        mosaic: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - 3 contributing scenes - mosaic')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
                                                                    # reassign nodata
                        clipParams = ' -dstnodata "None" -srcnodata "0" '
                        clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
                        mosaicFileName = baseName + '.tif'
                        mosaicFullName = os.path.join(tileDir, mosaicFileName)
                        warpCmd = gdalwarpLoc + clipParams + tempFullName + ' ' + mosaicFullName
                        if (debug):
                            logger.info('        nodata: {0}'.format(warpCmd))
                        try:
                            returnValue = call(warpCmd, shell=True)
                        except:
                            logger.error('Error: warpCmd - nodata')
                            logger.error('        Error: {0}'.format(traceback.format_exc()))
                            tileErrorHasOccurred = True
                            continue
      
      
                    if (curBand == 'sr_aerosol'):
                        srFinishedList.append(mosaicFileName)
                        qaFinishedList.append(mosaicFileName)

                    logger.info('    End processing for: {0}'.format(curBand))
      

      # --------------------------------------------------------------------------------------------------------------------------- #
      #
      #                                                       Generate statistics we will need for the metadata
      #
                logger.info('Creating metadata')
                if (tileErrorHasOccurred):
                    continue
          
                                                        # The pixel_qa band is 16bit unsigned int.
                                                        #
                                                        # For L8, there is valid information in bits 8,9, and 10.
                                                        # If bits 8 and 9 are set, this means Cirrus Confidence 
                                                        # is high and this pixel should be included in the cloud
                                                        # percentage count.
                                                        # 

                                                        # Create an 8bit mask with only those pixels 
                                                        # that have High Cirrus Confidence.  Both bits
                                                        # 8 and 9 must be set.  This means the pixel
                                                        # values are between 768 and 1024
      
                calcExpression = ' --calc="logical_and((A>=768), (A<1024))"'
                pqaCirrusMaskCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + pqaTileStart + \
                                               ' --outfile ' + pqaCirrusMask + calcExpression + ' --type="Byte" --NoDataValue=255'
                if (debug):
                    logger.info('        > pqaCirrusMaskCmd: {0}'.format(pqaCirrusMaskCmd))
                try:
                    returnValue = call(pqaCirrusMaskCmd, shell=True)
                except:
                    logger.error('Error: creating pqaCirrusMask')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
      
                                                        # Create a new file with values only up to 255.
                                                        # Pixels greater than 255 are turned to 0

                pqaLowerBitsCmd = gdaltranslateLoc + ' -ot "Byte" -a_nodata 0 -unscale ' + pqaTileStart + ' ' + pqaLowerBits
                if (debug):
                    logger.info('        > pqaLowerBitsCmd: {0}'.format(pqaLowerBitsCmd))
                try:
                    returnValue = call(pqaLowerBitsCmd, shell=True)
                except:
                    logger.error('Error: converting pixelQA band to 8bit')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
      
                                                        # Create an 8bit mask with only those pixels 
                                                        # that have High Cloud Confidence.  Bit 5
                                                        # must be set in the Lower Bit file that
                                                        # we just created. 


                calcExpression = ' --calc="bitwise_and(A, 32)"'
                pqaCloudMaskCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + pqaLowerBits + \
                                               ' --outfile ' + pqaCloudMask + calcExpression + ' --type="Byte" --NoDataValue=255'
                if (debug):
                    logger.info('        > pqaCloudMaskCmd: {0}'.format(pqaCloudMaskCmd))
                try:
                    returnValue = call(pqaCloudMaskCmd, shell=True)
                except:
                    logger.error('Error: creating pqaCloudMask')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
      
                                                        # We now have two 8 bit masks - one denoting 
                                                        # Cirrus Clouds and the second denoting Clouds.
                                                        # We will logical_or these two together in case
                                                        # a pixel has both cases set.  The result will give
                                                        # us a new mask showing pixels that will be 
                                                        # counted when calculating the cloud cover 
                                                        # percentage for the tile.

                calcExpression = ' --calc="logical_or((A==1),(B==32))"'
                pqaCloudCirrusMaskCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + pqaCirrusMask + \
                                                       ' -B ' + pqaCloudMask + ' --outfile ' + pqaCloudCirrusMask + \
                                                       calcExpression + ' --type="Byte" --NoDataValue=255'
                if (debug):
                    logger.info('        > pqaCloudCirrusMaskCmd: {0}'.format(pqaCloudCirrusMaskCmd))
                try:
                    returnValue = call(pqaCloudCirrusMaskCmd, shell=True)
                except:
                    logger.error('Error: creating pqaCloudCirrusMask')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
      
                                                        # Generate a histogram for the values in the 
                                                        # CloudCirrusMask.  Since this is a mask, we
                                                        # will only get a 0 or a 1 back.  The 1's give us 
                                                        # the number of pixels that are either High
                                                        # Confidence Cloud, High Confidence Cirrus,
                                                        # or both.

                histCloudCmd = gdalinfoLoc + ' -hist ' + pqaCloudCirrusMask + ' >> ' + histCloudCirrus
                if (debug):
                    logger.info('        > histCloudCmd: {0}'.format(histCloudCmd))
                try:
                    returnValue = call(histCloudCmd, shell=True)
                except:
                    logger.error('Error: generating histogram for cloud/cirrus pixels')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
      
                                                       # Read the CloudCirrusMask histogram to find
                                                       # the number of pixels      

                statsTupleCloudCirrus = parseHistFile(histCloudCirrus)
                if (len(statsTupleCloudCirrus) == 1):
                    logger.error('        Error reading CloudCirrus Histogram file.')
                    tileErrorHasOccurred = True
                    continue
      
                                                        # Generate a histogram for the values in the 
                                                        # LowerBits file.  These values give us the
                                                        # number of pixels of Clear Terrain, Water, 
                                                        # Snow/Ice, and Cloud Shadow.  We will ignore
                                                        # the cloud bits since we already counted them
                                                        # in the previous step.

                histLowerBitsCmd = gdalinfoLoc + ' -hist ' + pqaLowerBits + ' >> ' + histLowerBits
                if (debug):
                    logger.info('        > histLowerBitsCmd: {0}'.format(histLowerBitsCmd))
                try:
                    returnValue = call(histLowerBitsCmd, shell=True)
                except:
                    logger.error('Error: generating histogram for pixels in the lower bits')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
      
                                                        # Read the LowerBits histogram to find
                                                        # the number of pixels      

                statsTupleLowerBits = parseHistFile(histLowerBits)
                if (len(statsTupleLowerBits) == 1):
                    logger.error('        Error reading LowerBits Histogram file.')
                    tileErrorHasOccurred = True
                    continue
      
                                                        # Gather pixel counts
                                                        # 
                countFill = statsTupleLowerBits[0]
                countClear = statsTupleLowerBits[1]
                countWater = statsTupleLowerBits[2]
                countSnow = statsTupleLowerBits[3]
                countShadow = statsTupleLowerBits[4]
                countCloud = statsTupleCloudCirrus[0]
      
                if (debug):
                    logger.info('        # pixels Fill: {0}'.format(str(countFill)))
                    logger.info('        # pixels Clear: {0}'.format(str(countClear)))
                    logger.info('        # pixels Water: {0}'.format(str(countWater)))
                    logger.info('        # pixels Snow: {0}'.format(str(countSnow)))
                    logger.info('        # pixels CloudShadow: {0}'.format(str(countShadow)))
                    logger.info('        # pixels CloudCover: {0}'.format(str(countCloud)))

                                                        # Build a new tuple to hold the pixel counts
                statsTupleCombo = (countFill, countClear, countWater, countSnow, countShadow, countCloud)
    
                                                        #
                                                        # Create the tile metadata file
                                                        #
      
                L2Scene01MetaFileName = os.path.join(stackA_Dir, stackA_Prefix + ".xml")
                L1Scene01MetaFileName = os.path.join(stackA_Dir, stackA_Prefix + "_MTL.txt")
                L1Scene01MetaString = makeMetadataString(L1Scene01MetaFileName)
    
                L2Scene02MetaFileName = ''
                L1Scene02MetaFileName = ''
                L1Scene02MetaString = ''
    
                L2Scene03MetaFileName = ''
                L1Scene03MetaFileName = ''
                L1Scene03MetaString = ''

                if (numScenesPerTile == 2) or (numScenesPerTile == 3):
                    L2Scene02MetaFileName = os.path.join(stackB_Dir, stackB_Prefix + ".xml")
                    L1Scene02MetaFileName = os.path.join(stackB_Dir, stackB_Prefix + "_MTL.txt")
                    L1Scene02MetaString = makeMetadataString(L1Scene02MetaFileName)

                if (numScenesPerTile == 3):
                    L2Scene03MetaFileName = os.path.join(stackC_Dir, stackC_Prefix + ".xml")
                    L1Scene03MetaFileName = os.path.join(stackC_Dir, stackC_Prefix + "_MTL.txt")
                    L1Scene03MetaString = makeMetadataString(L1Scene03MetaFileName)

                metaFileName = tile_id + ".xml"
                metaFullName = os.path.join(tileDir, metaFileName)

                metaResults = buildMetadata2(debug, logger, statsTupleCombo, cutLimits, tile_id, \
                                                  L2Scene01MetaFileName, L1Scene01MetaString, \
                                                  L2Scene02MetaFileName, L1Scene02MetaString, \
                                                  L2Scene03MetaFileName, L1Scene03MetaString, \
                                                  appVersion, productionDateTime, filenameCrosswalk, \
                                                  region, numScenesPerTile, metaFullName)
      
                if 'ERROR' in metaResults:
                    logger.error('Error: writing metadata file')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                if (not os.path.isfile(metaFullName)):
                    logger.error('Error: metadata file does not exist')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                                                                        # Copy the metadata file to the output directory

                metaOutputName = os.path.join(metaOutputDir, metaFileName)
                try:
                    shutil.copyfile(metaFullName, metaOutputName)
                except:
                    logger.error('Error: copying metadata file to output dir')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                toaFinishedList.append(metaFileName)
                btFinishedList.append(metaFileName)
                srFinishedList.append(metaFileName)
                qaFinishedList.append(metaFileName)

                logger.info('    End metadata')
      
      
      # --------------------------------------------------------------------------------------------------------------------------- #
      #
      #                                                       Package all of the Landsat  mosaics into the .tar files
      #
      #
                logger.info('start creating tar files')
                if (tileErrorHasOccurred):
                    continue
    
                                                               # toa
                toaFullName = os.path.join(tgzOutputDir, tile_id + "_TA.tar")
                try:
                    tar = tarfile.open(toaFullName, "w")
                    for mName in toaFinishedList:
                        #print 'toa: ' + str(len(toaFinishedList)) + ': ' + mName
                        tar.add(os.path.join(tileDir, mName), arcname=mName)
                    tar.close()
                except:
                    logger.error('Error: creating toa tarfile')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
      
                                                               # bt
                btFullName = os.path.join(tgzOutputDir, tile_id + "_BT.tar")
                try:
                    tar = tarfile.open(btFullName, "w")
                    for mName in btFinishedList:
                        #print 'bt: ' + str(len(btFinishedList)) + ' ' + mName
                        tar.add(os.path.join(tileDir, mName), arcname=mName)
                    tar.close()
                except:
                    logger.error('Error: creating bt tarfile')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                                                               # sr
                srFullName = os.path.join(tgzOutputDir, tile_id + "_SR.tar")
                try:
                    tar = tarfile.open(srFullName, "w")
                    for mName in srFinishedList:
                        #print 'sr: ' + str(len(srFinishedList)) + ' ' + mName
                        tar.add(os.path.join(tileDir, mName), arcname=mName)
                    tar.close()
                except:
                    logger.error('Error: creating sr tarfile')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
                                                               # qa
                qaFullName = os.path.join(tgzOutputDir, tile_id + "_QA.tar")
                try:
                    tar = tarfile.open(qaFullName, "w")
                    for mName in qaFinishedList:
                        #print 'qa: ' + str(len(qaFinishedList)) + ' ' + mName
                        tar.add(os.path.join(tileDir, mName), arcname=mName)
                    tar.close()
                except:
                    logger.error('Error: creating qa tarfile')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                logger.info('    End zipping')

      # --------------------------------------------------------------------------------------------------------------------------- #
      #
      #                                                       Create the browse file for EE
      #
      #
                logger.info('Start browse creation')
                if (tileErrorHasOccurred):
                    continue

                                                                # the browseList was filled up in the 'Band Type 1' section
                for browseBand in browseList:
                    if ('TAB6' in browseBand):
                        redBand = browseBand
                    elif ('TAB5' in browseBand):
                        grnBand = browseBand
                    else:
                        bluBand = browseBand
      
                                                                # merge r,g,b bands
                brw1FileName = tile_id + '_brw1.tif'
                brw1FullName = os.path.join(tileDir, brw1FileName)
                browseCmd1 = pythonLoc + ' ' + gdalmergeLoc + ' ' + "-o " + brw1FullName + \
                                       ' -separate ' + os.path.join(tileDir, redBand) + ' ' + \
                                      os.path.join(tileDir, grnBand) + ' ' + os.path.join(tileDir, bluBand)
                if (debug):
                    logger.info('        1st browse command: {0}'.format(browseCmd1))
                try:
                    returnValue = call(browseCmd1, shell=True)
                except:
                    logger.error('Error: browse mergeCmd')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                                                                # scale the pixel values
                brw2FileName = tile_id + '_brw2.tif'
                brw2FullName = os.path.join(tileDir, brw2FileName)
                browseCmd2 = gdaltranslateLoc + ' -scale 0 10000 -ot Byte ' + \
                                       brw1FullName + ' ' + brw2FullName
                if (debug):
                    logger.info('        2nd browse command: {0}'.format(browseCmd2))
                try:
                    returnValue = call(browseCmd2, shell=True)
                except:
                    logger.error('Error: browse scaleCmd')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                                                                # apply compression
                brw3FileName = tile_id + '_brw3.tif'
                brw3FullName = os.path.join(tileDir, brw3FileName)
                browseCmd3 = gdaltranslateLoc + ' -co COMPRESS=JPEG -co PHOTOMETRIC=YCBCR ' + \
                           brw2FullName + ' ' + brw3FullName
                if (debug):
                    logger.info('        3rd browse command: {0}'.format(browseCmd3))
                try:
                    returnValue = call(browseCmd3, shell=True)
                except:
                    logger.error('Error: browse compressCmd')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
    
                                                                # internal pyramids
                browseCmd4 = gdaladdoLoc + ' ' + brw3FullName + ' 2 4 8 16'
                if (debug):
                    logger.info('        4rd browse command: {0}'.format(browseCmd4))
                try:
                    returnValue = call(browseCmd4, shell=True)
                except:
                    logger.error('Error: browse internalPyramidsCmd')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
    
                                                                # move to final location
                browseFileName = tile_id + '.tif'
                browseFullName = os.path.join(browseOutputDir, browseFileName)
                if (debug):
                    logger.info('        Move browse to output directory')
                try:
                    shutil.move(brw3FullName, browseFullName)
                    logger.info('rename file: {0}'.format(brw3FullName))
                except:
                    logger.error('Error: Moving file: {0} ...'.format(brw3FullName))
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                logger.info('    End building browse.')


# ------------------------------------------------------------------------------------------------------------------ #
#
#                                                       Perform some cleanup tasks for the current tile
#
#

                                                                # Remove the temporary work directory
                if (debug == False):
                    logger.info('    Cleanup: Removing temp directory: {0} ...'.format(tileDir))
                    try:
                        shutil.rmtree(tileDir)
                        logger.info('test rmtree: {0}'.format(tileDir))
                    except:
                        logger.error('Error: Removing directory: {0} ...'.format(tileDir))
                        logger.error('        Error: {0}'.format(traceback.format_exc()))
                        # continue on, even if we encountered an error down here


# ------------------------------------------------------------------------------------------------------------------ #
#
#                                                       Create md5 checksum files
#
#
                toaMD5Name = toaFullName.replace(".tar", ".md5")
                btMD5Name = btFullName.replace(".tar", ".md5")
                srMD5Name = srFullName.replace(".tar", ".md5")
                qaMD5Name = qaFullName.replace(".tar", ".md5")
    
                toaHash =  hashlib.md5(open(toaFullName, 'rb').read()).hexdigest()
                btHash =  hashlib.md5(open(btFullName, 'rb').read()).hexdigest()
                srHash =  hashlib.md5(open(srFullName, 'rb').read()).hexdigest()
                qaHash =  hashlib.md5(open(qaFullName, 'rb').read()).hexdigest()

                toaFileName = os.path.basename(toaFullName)
                btFileName = os.path.basename(btFullName)
                srFileName = os.path.basename(srFullName)
                qaFileName = os.path.basename(qaFullName)
    
                try:
                    outfile = open(toaMD5Name, 'w')
                    outfile.write(toaHash + ' ' + toaFileName)
                    outfile.close()
                except:
                    logger.error('Error: writing toa md5 file')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                try:
                    outfile = open(btMD5Name, 'w')
                    outfile.write(btHash + ' ' + btFileName)
                    outfile.close()
                except:
                    logger.error('Error: writing bt md5 file')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue
   
                try:
                    outfile = open(srMD5Name, 'w')
                    outfile.write(srHash + ' ' + srFileName)
                    outfile.close()
                except:
                    logger.error('Error: writing sr md5 file')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

                try:
                    outfile = open(qaMD5Name, 'w')
                    outfile.write(qaHash + ' ' + qaFileName)
                    outfile.close()
                except:
                    logger.error('Error: writing qa md5 file')
                    logger.error('        Error: {0}'.format(traceback.format_exc()))
                    tileErrorHasOccurred = True
                    continue

# ------------------------------------------------------------------------------------------------------------------ #
#
#                                           No errors making this tile, record it in our database
#
#
                processingState = "ERROR"
                if not tileErrorHasOccurred:
                    processingState = "SUCCESS"

                completed_tile_list = []
                sceneListStr = ",".join(contributingScenesforDB)
                row = (tile_id,sceneListStr,complete_tile,processingState)
                completed_tile_list.append(row)

                insert_tile_record(connection, completed_tile_list)



        # update PROCESSING_STATE in ARD_PROCESSED_SCENES to 'COMPLETE'
        updatesql = "update ARD_PROCESSED_SCENES set PROCESSING_STATE = 'COMPLETE' where scene_id = '" + scene_record[4] + "'"
        update_cursor = connection.cursor()
        update_cursor.execute(updatesql)
        connection.commit()
        update_cursor.close()
        logger.info("Scene {0} is COMPLETE.".format(scene_record[4]))

        # cleanup untarred scene directories not in segment
        for additionalScene in additionalSceneCleanUpList:
           if (os.path.isdir(additionalScene)):
              shutil.rmtree(additionalScene)

        # increment segment scene counter
        sceneCtr = sceneCtr + 1
        # End for each segment loop
        logger.info('Segment loop: {0}'.format(sceneCtr))

    # cleanup any remaining scene directories
    for scene_record in segment:
        unneededScene = scene_record[4]
        if (os.path.isdir(unneededScene)):
           shutil.rmtree(unneededScene)

def insert_tile_record(connection, completed_tile_list):

   logger.info('Insert tile into ARD_COMPLETED_TILES table: {0}'.format(completed_tile_list))
   insert_cursor = connection.cursor()
   processed_tiles_insert = "insert /*+ ignore_row_on_dupkey_index(ARD_COMPLETED_TILES, TILE_ID_PK) */ into ARD_COMPLETED_TILES (tile_id,CONTRIBUTING_SCENES,COMPLETE_TILE,PROCESSING_STATE) values (:1,:2,:3,:4)"
   insert_cursor.bindarraysize = len(completed_tile_list)
   insert_cursor.prepare(processed_tiles_insert)
   insert_cursor.executemany(None, completed_tile_list)
   insert_cursor.close()
   connection.commit()





# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Main processing block
#
if __name__ == "__main__":


    logger = setup_logging()
    logger.info("current working directory: {0}".format(os.getcwd()))

    readConfig()

    logger.info('******************Start************')
    logger.info('             DB connection: {0}'.format(connstr))
    logger.info("             Version: {0}".format(version))

    try:
        connection = cx_Oracle.connect(connstr)
    except:
        logger.error("Error:  Unable to connect to the database.")
        sys.exit(1)

    segment, tgzOutputDir = readCmdLn(sys.argv)

    finalOutputDirectory = finalDirectoryLU[segment[0][4][3:4]]
    tgzOutputDir = os.path.join(tgzOutputDir, finalOutputDirectory)

    logger.info('segment: {0}'.format(segment))
    logger.info('output path: {0}'.format(tgzOutputDir))

    browseOutputDir = tgzOutputDir
    metaOutputDir = tgzOutputDir

    # Stage files to disk cache for faster access
    stageFiles(segment,soap_envelope)

    # Create tiles from segment of scenes
    processScenes(segment)

    connection.close()
    logger.info('\nNormal End')

