#!/usr/bin/python

import os
import sys
import glob
import shutil
import logging


import db
import util
import landsat
import external
import geofuncs
from util import logger
from ARD_regionLU import pathrow2regionLU
from ARD_metadata import buildMetadata


def process_tile(current_tile, segment, region, tiles_contrib_scenes, output_path, conf):
    """ Big loop for each tile needed """

    productionDateTime = landsat.get_production_timestamp()
    tile_id = landsat.generate_tile_id(segment['LANDSAT_PRODUCT_ID'], current_tile, region, conf.collection, conf.version)
    clip_extents = '{UL_X} {LL_Y} {LR_X} {UR_Y}'.format(**current_tile)

    logger.debug("tile_id: %s", tile_id)
    logger.debug("clip_extents: %s", clip_extents)

    # See if tile_id exists in ARD_COMPLETED_TILES table
    tile_rec = db.check_tile_status(db.connection(conf.connstr), tile_id)
    logger.debug("Tile status: %s", tile_rec)

    if len(tile_rec) != 0:
        logger.error('Tile already created! %s', tile_rec)
        return 'ERROR'

    logger.info("Create Tile %s", tile_id)

    # Get file location for scenes that will contribute to the tile
    key = 'H{H:03d}V{V:03d}'.format(**current_tile)
    contrib_tile_scenes = tiles_contrib_scenes[key]
    logger.debug('# Scenes needed for tile %s: %d', tile_id, len(contrib_tile_scenes))


    contributing_scenes = dict()
    for contrib_record in contrib_tile_scenes:
        wildcard = '{wrspath}{wrsrow}_{acqdate}'.format(**contrib_record)
        logger.debug("          Contributing scene: %s", wildcard)

        if wildcard in segment['LANDSAT_PRODUCT_ID']:
            provided_contrib_rec = {segment['LANDSAT_PRODUCT_ID']: segment['FILE_LOC']}
            logger.info("Contributing scene from segment: %s", provided_contrib_rec)
            contributing_scenes.update(provided_contrib_rec)

        if len(provided_contrib_rec) == 0:
            db_contrib_rec = db.fetch_file_loc(db.connection(conf.connstr),
                                               sat=segment['SATELLITE'],
                                               wildcard=wildcard)
            logger.debug('Fetch file locations from DB: %s', db_contrib_rec)
            if len(db_contrib_rec) > 0:
                db_contrib_rec = {r['LANDSAT_PRODUCT_ID']: glob.glob(r['FILE_LOC'])[0]
                                  for r in db_contrib_rec}
                logger.info("Contributing scene from db: %s", db_contrib_rec)
                contributing_scenes.update(db_contrib_rec)

    n_contrib_scenes = len(contributing_scenes)
    is_complete_tile = 'Y' if n_contrib_scenes == len(contrib_tile_scenes) else 'N'
    logger.info("Contributing scenes: %s", contributing_scenes)
    logger.info('Complete tile available: %s', is_complete_tile)
    logger.info('Tile: %s  - Number of contributing scenes: %d', tile_id, n_contrib_scenes)

    # Maximum # of scenes that can contribute to a tile is 3
    invalid_contrib_scenes = ((n_contrib_scenes > conf.maxscenespertile)
                              or (n_contrib_scenes < conf.minscenespertile))
    if invalid_contrib_scenes:
        logger.info('Unexpected number of scenes %d', n_contrib_scenes)
        return  "ERROR"


    if conf.hsmstage:
        # Stage files to disk cache for faster access
        external.stage_files(contributing_scenes.values(), conf.soap_envelope)

    # If each contributing scene is not already unpacked, do it here
    for product_id, tar_file_location in contributing_scenes.items():

        logger.info('Required Scene: %s', tar_file_location)
        try:
            scene_dir = util.untar_archive(tar_file_location, directory=product_id)
        except:
            logger.exception('Error staging input data for {}: {}'.format(product_id, tar_file_location))
            return 'ERROR'

    logger.info('Starting to build tile: %s', tile_id)

    # Determine which scene(s) will overlay the other scene(s) for this tile.
    # North scenes (lower row #) will always overlay South scenes (higher row #)
    logging.debug('%%%%%%%%%%% {}'.format(zip(range(1, n_contrib_scenes+1),
                                   sorted(contributing_scenes.keys()))))
    stacking = {i: {'LANDSAT_PRODUCT_ID': name,
                    'XML_LOC': glob.glob(os.path.join(name, '*.xml'))[0]}
                for i, name in zip(range(1, n_contrib_scenes+1),
                                   sorted(contributing_scenes.keys()))}

    util.make_dirs(tile_id)
    raise NotImplementedError('OK SO WE SHOULD BE GOOD TO GO HERE!@')

    if process_bandtype_1() == 'ERROR':
        return 'ERROR'

    if process_bandtype_2() == 'ERROR':
        return 'ERROR'

    if process_bandtype_3() == 'ERROR':
        return 'ERROR'

    if process_bandtype_4() == 'ERROR':
        return 'ERROR'

    if process_lineage() == 'ERROR':
        return 'ERROR'

    if process_bandtype_5() == 'ERROR':
        return 'ERROR'

    if process_metadata() == 'ERROR':
        return 'ERROR'

    if process_browse() == 'ERROR':
        return 'ERROR'

    if process_output() == 'ERROR':
        return 'ERROR'

    util.process_checksums()

                                                    # Remove the temporary work directory
    if (conf.debug == False):
        logger.info('    Cleanup: Removing temp directory: {0} ...'.format(tile_id))
        try:
            shutil.rmtree(tile_id)
            logger.info('test rmtree: {0}'.format(tile_id))
        except:
            logger.exception('Error: Removing directory: {0} ...'.format(tile_id))
            # continue on, even if we encountered an error down here

    # No errors making this tile, record it in our database
    completed_tile_list = [[tile_id,",".join(contributing_scenes.keys()), is_complete_tile, "SUCCESS"]]
    db.insert_tile_record(db.connection(conf.connstr), completed_tile_list)
    return "SUCCESS"


def process_bandtype_1():
    # --------------------------------------------------------------------------------------------------------------------------- #
    #
    #     Band Type 1
    #
    #                   16 bit signed integer
    #     		NoData = -9999
    #
    #
    logger.info('     Start processing for band: {0}'.format(curBand))

    clipParams = ' -dstnodata "-9999" -srcnodata "-9999" '
    clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
    newARDname = getARDName(curBand, filenameCrosswalk)
    if (newARDname == 'ERROR'):
        logger.error('Error in filenameCrosswalk for: {0}'.format(curBand))
        return "ERROR"
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
            return "ERROR"

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
            return 'ERROR'

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
            return 'ERROR'

        if (curBand == 'toa_band1'):                                    # needed for lineage file generation later
            lineage01 = northFullname
            lineage02 = midFullname
            lineage03 = southFullname

    logger.info('    End processing for: {0}'.format(curBand))

    if curBand in bands_for_toa:
        toaFinishedList.append(mosaicFileName)

    if curBand in bands_for_sr:
        srFinishedList.append(mosaicFileName)

    if curBand in bands_for_bt:
        btFinishedList.append(mosaicFileName)

    if curBand in bands_for_pqa:
        qaFinishedList.append(mosaicFileName)

    if curBand in bands_for_swater:
        swFinishedList.append(mosaicFileName)

    if curBand in bands_for_stemp:
        stFinishedList.append(mosaicFileName)

                                                        # save for browse later
    if curBand in bands_for_browse:
        browseList.append(mosaicFileName)


def process_bandtype_2():
    # --------------------------------------------------------------------------------------------------------------------------- #
    #
    #     Band Type 2
    #
    #                   16 bit signed integer
    #     		NoData = -32768
    #
    logger.info('     Start processing for band: {0}'.format(curBand))

    clipParams = ' -dstnodata "-32768" -srcnodata "-32768" '
    clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
    newARDname = getARDName(curBand, filenameCrosswalk)
    if (newARDname == 'ERROR'):
        logger.error('Error in filenameCrosswalk for: {0}'.format(curBand))
        return "ERROR"
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
            return 'ERROR'


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
            return 'ERROR'


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
            return 'ERROR'

    logger.info('    End processing for: {0}'.format(curBand))


    if curBand in bands_for_toa:
        toaFinishedList.append(mosaicFileName)

    if curBand in bands_for_sr:
        srFinishedList.append(mosaicFileName)

    if curBand in bands_for_bt:
        btFinishedList.append(mosaicFileName)

    if curBand in bands_for_pqa:
        qaFinishedList.append(mosaicFileName)

    if curBand in bands_for_swater:
        swFinishedList.append(mosaicFileName)

    if curBand in bands_for_stemp:
        stFinishedList.append(mosaicFileName)



def process_bandtype_3():
    # --------------------------------------------------------------------------------------------------------------------------- #
    #
    #     Band Type 3
    #
    #     		16 bit unsigned integer
    #		NoData = 1  in ovelap areas and scan gaps
    #
    #
    logger.info('    Start processing for: {0}'.format(curBand))

    clipParams = ' -dstnodata "1" -srcnodata "1" '
    tempFileName = targzMission + '_' + collectionLevel + '_' + current_tile[0][0] + current_tile[0][1] + \
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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

                                                    # reassign nodata
    clipParams = ' -dstnodata "0" -srcnodata "None" '
    clipParams = clipParams + '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
    mosaicFileName = targzMission + '_' + collectionLevel + '_' + current_tile[0][0] + current_tile[0][1] + \
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
        return "ERROR"

    logger.info('    End processing for: {0}'.format(curBand))

    finishedMosaicList.append(mosaicFileName)



def process_bandtype_4():
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
    logger.info('    Start processing for: {0}'.format(curBand))

    newARDname = getARDName(curBand, filenameCrosswalk)
    if (newARDname == 'ERROR'):
        logger.error('Error in filenameCrosswalk for: {0}'.format(curBand))
        return "ERROR"
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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

    if (curBand == 'pixel_qa'):
        pqaTileStart = os.path.join(tileDir, mosaicFileName)
        pqaCirrusMask = pqaTileStart.replace('PIXELQA', 'pqaCirrusMask')
        pqaLowerBits = pqaTileStart.replace('PIXELQA', 'pqaLowerBits')
        pqaCloudMask = pqaTileStart.replace('PIXELQA', 'pqaCloudMask')
        pqaCloudCirrusMask = pqaTileStart.replace('PIXELQA', 'pqaCloudCirrusMask')
        histCloudCirrus = os.path.join(tileDir, 'histCloudCirrus.json')
        histLowerBits = os.path.join(tileDir, 'histLowerBits.json')

    if curBand in bands_for_toa:
        toaFinishedList.append(mosaicFileName)

    if curBand in bands_for_sr:
        srFinishedList.append(mosaicFileName)

    if curBand in bands_for_bt:
        btFinishedList.append(mosaicFileName)

    if curBand in bands_for_pqa:
        qaFinishedList.append(mosaicFileName)

    if curBand in bands_for_swater:
        swFinishedList.append(mosaicFileName)

    if curBand in bands_for_stemp:
        stFinishedList.append(mosaicFileName)

    logger.info('    End processing for: {0}'.format(curBand))



def process_lineage():
    """ Create the lineage file """

    lineageFileName = tile_id + '_LINEAGEQA.tif'


    if (numScenesPerTile == 1):                                   # 1 contributing scene

        calcExpression = ' --calc="' + str(stackA_Value) + ' * (A > -101)"'
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
            return 'ERROR'

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
            return 'ERROR'

    elif (numScenesPerTile == 2):                                      # 2 contributing scenes - north
        northCalcExp = ' --calc="' + str(stackA_Value) + ' * (A > -101)"'
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
            return 'ERROR'

                                                                            # 2 contributing scenes - south
        southCalcExp = ' --calc="' + str(stackB_Value) + ' * (A > -101)"'
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
            return 'ERROR'

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
            return 'ERROR'

    else:                                                         # 3 contributing scenes - north
        northCalcExp = ' --calc="' + str(stackA_Value) + ' * (A > -101)"'
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
            return 'ERROR'

                                                        # 3 contributing scenes - middle
        midCalcExp = ' --calc="' + str(stackB_Value) + ' * (A > -101)"'
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
            return 'ERROR'

                                                        # 3 contributing scenes - south
        southCalcExp = ' --calc="' + str(stackC_Value) + ' * (A > -101)"'
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
            return 'ERROR'

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
            return 'ERROR'

    toaFinishedList.append(lineageFileName)
    btFinishedList.append(lineageFileName)
    srFinishedList.append(lineageFileName)
    qaFinishedList.append(lineageFileName)
    swFinishedList.append(lineageFileName)
    stFinishedList.append(lineageFileName)

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
        return "ERROR"

    contribSceneResults = parseSceneHistFile(sceneHistFilename)
    contribSceneCount = contribSceneResults[0]
    contribScenePixCountArray = contribSceneResults[1]

    if (contribSceneCount == 0):
        logger.error('Warning: parsing histogram from lineage file: 0 contributing scenes')
        logger.error('        Error: {0}'.format(traceback.format_exc()))

        # Insert tile into db and record it as "NOT NEEDED" i.e it's an empty tile
        processingState = "NOT NEEDED"

        completed_tile_list = []
        sceneListStr = "none"
        row = (tile_id,sceneListStr,complete_tile,processingState)
        completed_tile_list.append(row)

        db.insert_tile_record(db.connection(conf.connstr), completed_tile_list)

        # Remove the temporary work directory
        shutil.rmtree(tileDir)
        return processingState

    # decrement pixel values in lineage file if some scenes didn't contribute
    # any pixels
    if (contribSceneCount != numScenesPerTile):

        decrement_value = numScenesPerTile - contribSceneCount
        sceneCountDifference = decrement_value

        # Determine whether we need decrement the pixel values in the lineage file or not.
        calcExpression = ''
        if sceneCountDifference == 1:
            if contribScenePixCountArray[0] == 0:
                calcExpression = ' --calc="A-' + str(decrement_value) + '"'
            elif contribScenePixCountArray[1] == 0 and contribScenePixCountArray[2] > 0:
                calcExpression = ' --calc="A-(A==3)"'
        elif sceneCountDifference == 2:
            if contribScenePixCountArray[0] == 0 and contribScenePixCountArray[1] == 0:
                calcExpression = ' --calc="A-' + str(decrement_value) + '"'
            elif contribScenePixCountArray[0] == 0 and contribScenePixCountArray[2] == 0:
                calcExpression = ' --calc="A-' + str(1) + '"'

        if calcExpression != '':
            lineageTempFullName = lineageFullName.replace('.tif', '_linTemp.tif')

            recalcCmd = pythonLoc + ' ' + gdalcalcLoc + ' -A ' + lineageFullName + ' --outfile ' + lineageTempFullName + calcExpression + ' --type="Byte" --NoDataValue=0 --overwrite'

            logger.info('        recalc lineage command: {0}'.format(recalcCmd))

            try:
                returnValue = call(recalcCmd, shell=True)
            except:
                logger.error('Error: recalcCmd - recalculating pixels in Lineage file')
                logger.error('        Error: {0}'.format(traceback.format_exc()))
                return "ERROR"

            # compress
            clipParams = ' -co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" -overwrite '
            warpCmd = gdalwarpLoc +  clipParams + lineageTempFullName + ' ' + lineageFullName
            if (debug):
                logger.info('        compress lineage recalc command: {0}'.format(warpCmd))
            try:
                returnValue = call(warpCmd, shell=True)
            except:
                logger.error('Error: warpCmd - compressing lineage recalc')
                logger.error('        Error: {0}'.format(traceback.format_exc()))
                return "ERROR"

    logger.info('finish updating contributing scenes')


def process_bandtype_5():
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

    logger.info('    Start processing for: {0}'.format(curBand))

    newARDname = getARDName(curBand, filenameCrosswalk)
    if (newARDname == 'ERROR'):
        logger.error('Error in filenameCrosswalk for: {0}'.format(curBand))
        return "ERROR"
    baseName = tile_id + '_' + newARDname

                                                        #
                                                        # Only 1 contributing scene
                                                        #
                                                        # Perform a simple clip and then
                                                        # reassign any NoData back to zero
                                                        #
    if (contribSceneCount == 1):
        if contribScenePixCountArray[0] > 0:
            inputFileName = stackA_Prefix + '_' + curBand + '.tif'
            inputFullName = os.path.join(stackA_Dir, inputFileName)
        elif contribScenePixCountArray[1] > 0:
            inputFileName = stackB_Prefix + '_' + curBand + '.tif'
            inputFullName = os.path.join(stackB_Dir, inputFileName)
        elif contribScenePixCountArray[2] > 0:
            inputFileName = stackC_Prefix + '_' + curBand + '.tif'
            inputFullName = os.path.join(stackC_Dir, inputFileName)

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
            return 'ERROR'

                                                    # reassign nodata & compress
        clipParams = ' -dstnodata "None" -srcnodata "256" '
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
            return 'ERROR'

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

    elif (contribSceneCount == 2):
        if contribScenePixCountArray[0] > 0 and contribScenePixCountArray[1] > 0:
            northFilename = stackA_Prefix + '_' + curBand + '.tif'
            southFilename = stackB_Prefix + '_' + curBand + '.tif'

            northFullname = os.path.join(stackA_Dir, northFilename)
            southFullname = os.path.join(stackB_Dir, southFilename)
        elif contribScenePixCountArray[0] > 0 and contribScenePixCountArray[2] > 0:
            northFilename = stackA_Prefix + '_' + curBand + '.tif'
            southFilename = stackC_Prefix + '_' + curBand + '.tif'

            northFullname = os.path.join(stackA_Dir, northFilename)
            southFullname = os.path.join(stackC_Dir, southFilename)
        elif contribScenePixCountArray[1] > 0 and contribScenePixCountArray[2] > 0:
            northFilename = stackB_Prefix + '_' + curBand + '.tif'
            southFilename = stackC_Prefix + '_' + curBand + '.tif'

            northFullname = os.path.join(stackB_Dir, northFilename)
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
            logger.error('Error: warpCmd - 2 contributing scenes - north')
            logger.error('        Error: {0}'.format(traceback.format_exc()))
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

                                                # reassign nodata & compress
        clipParams = ' -dstnodata "None" -srcnodata "256" '
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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

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
            return 'ERROR'

                                                    # reassign nodata
        clipParams = ' -dstnodata "None" -srcnodata "256" '
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
            return 'ERROR'


    if curBand in bands_for_toa:
        toaFinishedList.append(mosaicFileName)

    if curBand in bands_for_sr:
        srFinishedList.append(mosaicFileName)

    if curBand in bands_for_bt:
        btFinishedList.append(mosaicFileName)

    if curBand in bands_for_pqa:
        qaFinishedList.append(mosaicFileName)

    if curBand in bands_for_swater:
        swFinishedList.append(mosaicFileName)

    if curBand in bands_for_stemp:
        stFinishedList.append(mosaicFileName)

    logger.info('    End processing for: {0}'.format(curBand))


def process_metadata():
    """ Create the tile metadata file, Generate statistics we will need """

    statsTupleBits = raster_value_count(pqaTileStart, True)
    if (len(statsTupleBits) == 1):
        logger.error('        Error {0} pixel counts.'.format(pqaTileStart))
        return "ERROR"


                                            # Gather pixel counts
                                            #
    countFill = statsTupleBits[0]
    countClear = statsTupleBits[1]
    countWater = statsTupleBits[2]
    countShadow = statsTupleBits[3]
    countSnow = statsTupleBits[4]
    countCloud = statsTupleBits[5]
    countCirrus = statsTupleBits[6]
    countTerrain = statsTupleBits[7]

    if (debug):
        logger.info('        # pixels Fill: {0}'.format(str(countFill)))
        logger.info('        # pixels Clear: {0}'.format(str(countClear)))
        logger.info('        # pixels Water: {0}'.format(str(countWater)))
        logger.info('        # pixels Snow: {0}'.format(str(countSnow)))
        logger.info('        # pixels CloudShadow: {0}'.format(str(countShadow)))
        logger.info('        # pixels CloudCover: {0}'.format(str(countCloud)))
        logger.info('        # pixels Cirrus: {0}'.format(str(countCirrus)))
        logger.info('        # pixels Terrain: {0}'.format(str(countTerrain)))

                                            # Build a new tuple to hold the pixel counts
    statsTupleCombo = (countFill, countClear, countWater, countSnow, countShadow,
                        countCloud, countCirrus, countTerrain)


    L2Scene01MetaFileName = ''
    L1Scene01MetaFileName = ''
    L1Scene01MetaString = ''

    L2Scene02MetaFileName = ''
    L1Scene02MetaFileName = ''
    L1Scene02MetaString = ''

    L2Scene03MetaFileName = ''
    L1Scene03MetaFileName = ''
    L1Scene03MetaString = ''

    if contribScenePixCountArray[0] > 0:
        L2Scene01MetaFileName = os.path.join(stackA_Dir, stackA_Prefix + ".xml")
        L1Scene01MetaFileName = os.path.join(stackA_Dir, stackA_Prefix + "_MTL.txt")
        L1Scene01MetaString = makeMetadataString(L1Scene01MetaFileName)

    if contribScenePixCountArray[1] > 0:
        if L2Scene01MetaFileName == '':
            L2Scene01MetaFileName = os.path.join(stackB_Dir, stackB_Prefix + ".xml")
            L1Scene01MetaFileName = os.path.join(stackB_Dir, stackB_Prefix + "_MTL.txt")
            L1Scene01MetaString = makeMetadataString(L1Scene01MetaFileName)
        else:

            L2Scene02MetaFileName = os.path.join(stackB_Dir, stackB_Prefix + ".xml")
            L1Scene02MetaFileName = os.path.join(stackB_Dir, stackB_Prefix + "_MTL.txt")
            L1Scene02MetaString = makeMetadataString(L1Scene02MetaFileName)

    if contribScenePixCountArray[2] > 0:
        if L2Scene01MetaFileName == '':
            L2Scene01MetaFileName = os.path.join(stackC_Dir, stackC_Prefix + ".xml")
            L1Scene01MetaFileName = os.path.join(stackC_Dir, stackC_Prefix + "_MTL.txt")
            L1Scene01MetaString = makeMetadataString(L1Scene01MetaFileName)
        elif L2Scene02MetaFileName == '':
            L2Scene02MetaFileName = os.path.join(stackC_Dir, stackC_Prefix + ".xml")
            L1Scene02MetaFileName = os.path.join(stackC_Dir, stackC_Prefix + "_MTL.txt")
            L1Scene02MetaString = makeMetadataString(L1Scene02MetaFileName)
        else:
            L2Scene03MetaFileName = os.path.join(stackC_Dir, stackC_Prefix + ".xml")
            L1Scene03MetaFileName = os.path.join(stackC_Dir, stackC_Prefix + "_MTL.txt")
            L1Scene03MetaString = makeMetadataString(L1Scene03MetaFileName)

    metaFileName = tile_id + ".xml"
    metaFullName = os.path.join(tileDir, metaFileName)

    metaResults = buildMetadata(debug, logger, statsTupleCombo, cutLimits, tile_id, \
                                        L2Scene01MetaFileName, L1Scene01MetaString, \
                                        L2Scene02MetaFileName, L1Scene02MetaString, \
                                        L2Scene03MetaFileName, L1Scene03MetaString, \
                                        appVersion, productionDateTime, filenameCrosswalk, \
                                        region, contribSceneCount, metaFullName)

    if 'ERROR' in metaResults:
        logger.error('Error: writing metadata file')
        logger.error('        Error: {0}'.format(traceback.format_exc()))
        return "ERROR"

    if (not os.path.isfile(metaFullName)):
        logger.error('Error: metadata file does not exist')
        logger.error('        Error: {0}'.format(traceback.format_exc()))
        return "ERROR"

                                                            # Copy the metadata file to the output directory

    metaOutputName = os.path.join(metaOutputDir, metaFileName)
    try:
        shutil.copyfile(metaFullName, metaOutputName)
        make_file_group_writeable(metaOutputName)
    except:
        logger.error('Error: copying metadata file to output dir')
        logger.error('        Error: {0}'.format(traceback.format_exc()))
        return "ERROR"

    toaFinishedList.append(metaFileName)
    btFinishedList.append(metaFileName)
    srFinishedList.append(metaFileName)
    qaFinishedList.append(metaFileName)
    swFinishedList.append(metaFileName)
    stFinishedList.append(metaFileName)

    logger.info('    End metadata')


def process_output():
    """ Package all of the Landsat  mosaics into the .tar files """

    output_tars = None
    try:
        util.tar_archive(stFullName, stFinishedList)
    except:
        logger.exception('Error: creating st tarfile')
        return "ERROR"

    logger.info('    End zipping')


def process_browse():
    """ Create the browse file for EE """
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
        return "ERROR"

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
        return "ERROR"

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
        return "ERROR"

                                                    # internal pyramids
    browseCmd4 = gdaladdoLoc + ' ' + brw3FullName + ' 2 4 8 16'
    if (debug):
        logger.info('        4rd browse command: {0}'.format(browseCmd4))
    try:
        returnValue = call(browseCmd4, shell=True)
    except:
        logger.error('Error: browse internalPyramidsCmd')
        logger.error('        Error: {0}'.format(traceback.format_exc()))
        return "ERROR"

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
        return "ERROR"

    logger.info('    End building browse.')


def process_segment(segment, output_path, conf):

    scene_state = "INWORK"
    # update PROCESSING_STATE in ARD_PROCESSED_SCENES to 'INWORK'
    db.update_scene_state(db.connection(conf.connstr), segment['LANDSAT_PRODUCT_ID'], scene_state)

    logger.info("Scene %s is %s.", segment['LANDSAT_PRODUCT_ID'], scene_state)
    id_parts = landsat.match_dt(segment['LANDSAT_PRODUCT_ID'])

    pathrow = id_parts['wrspath'] + id_parts['wrsrow']
    if pathrow in pathrow2regionLU.keys():
        region = pathrow2regionLU[pathrow]
    else:
        scene_state = "NOGRID"
        db.update_scene_state(db.connection(conf.connstr), segment['LANDSAT_PRODUCT_ID'], scene_state)
        return scene_state

    # Intersect scene with tile index to determine which tiles must be produced
    # Get tiles for scene and for each tile a list of path/rows from consecutive scenes
    hv_tiles, tiles_contrib_scenes = (
        geofuncs.get_tile_scene_intersections(db.connection(conf.connstr),
                                              segment['LANDSAT_PRODUCT_ID'],
                                              region))

    logger.info('Number of tiles to create: %d', len(hv_tiles))
    if len(hv_tiles) == 0:
        logger.error('No scene coordinates found for %s', segment['LANDSAT_PRODUCT_ID'])

        scene_state = "ERROR"
        # update PROCESSING_STATE in ARD_PROCESSED_SCENES
        db.update_scene_state(db.connection(conf.connstr), segment['LANDSAT_PRODUCT_ID'], scene_state)
        return scene_state

    for current_tile in hv_tiles:
        try:
            tile_state = process_tile(current_tile, segment, region, tiles_contrib_scenes, output_path, conf)
        except:
            logger.exception('Unexpected error processing tile {} !'.format(current_tile))
            tile_state = 'ERROR'

        if tile_state == 'ERROR':
            scene_state = tile_state
            # update PROCESSING_STATE in ARD_PROCESSED_SCENES
            db.update_scene_state(db.connection(conf.connstr), segment['LANDSAT_PRODUCT_ID'], scene_state)
            return scene_state

    scene_state = "COMPLETE"
    # update PROCESSING_STATE in ARD_PROCESSED_SCENES
    db.update_scene_state(db.connection(conf.connstr), segment['LANDSAT_PRODUCT_ID'], scene_state)

    return scene_state


def process_segments(segments, output_path, conf):
    """ Clips tiles from a list of contiguous scenes aka segment """

    for i, segment in enumerate(segments):
        # Cleanup unneeded scene directories to save space
        # Results of this check should keep no more than 3 scenes
        # in work directory at a time
        if (i - 2) > -1:
            unneededScene = segment[i - 2]['LANDSAT_PRODUCT_ID']
            if (os.path.isdir(unneededScene)):
                shutil.rmtree(unneededScene)

        scene_state = process_segment(segment, output_path, conf)
        logger.info("Scene %s is %s.", segment['LANDSAT_PRODUCT_ID'], scene_state)
        logger.info('Segment loop: %d', i)

    # cleanup any remaining scene directories
    for segment in segments:
        unneededScene = segment['LANDSAT_PRODUCT_ID']
        if not conf.debug and os.path.isdir(unneededScene):
           shutil.rmtree(unneededScene)
