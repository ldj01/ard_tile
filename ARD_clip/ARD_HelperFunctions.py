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
import urllib2

from osgeo import gdal, osr, ogr
import numpy as np

import db
import landsat
from util import logger


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

    # save scene pixel counts into an array and return to caller
    scenePixelCountArray = []
    scenePixelCountArray.append(count1)
    scenePixelCountArray.append(count2)
    scenePixelCountArray.append(count3)

    if (count1 > 0 and count2 > 0 and count3 > 0):
        return 3, scenePixelCountArray
    elif ((count1 > 0 and count2 > 0) or
          (count1 > 0 and count3 > 0) or
          (count2 > 0 and count3 > 0)):
        return 2, scenePixelCountArray
    elif (count1 > 0 or count2 > 0 or count3 > 0):
        return 1, scenePixelCountArray
    else:
        return 0, scenePixelCountArray

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


def get_tile_scene_intersections(connection, product_id, region, n=2):
    """ Find the Tile IDs intersecting with product_id, and neighboring same-day consecutive acqs

    Args:
        connection (cx_Oracle.Connection): open database connection
        product_id (str): landsat collection product id
        region (str): tile grid region following shapefile naming convention
        n (int): consecutive rows to intersect with

    Returns:
        list: all the tile ids that intersect the input landsat product id
        dict: tile ids that contain all the required path/rows that contribute to that tile

    Example:
        >>> get_tile_scene_intersections(db, 'LT04_L2TP_035027_19890712_20161110_01_A1', 'CU', n=2)
        ([{'H': 11,
           'V': 3,
           'LL_Y': 2714805,
           'LR_X': -765585,
           'UL_X': -915585,
           'UR_Y': 2864805}, ...],
        { 'H011V004P035': [{'acqdate': '19890712',
            'mission': 'LT04',
            'procdate': '20161110',
            'wrspath': '035',
            'wrsrow': '027'}, ...], ...})
    """

    # We need to get 2 consecutive wrsRows north and 2 consecutive wrsRows south
    # of input scene to account for possible 3 scene tile.
    id_info = landsat.match(product_id)
    ls_prod_id_scenes = db.select_consecutive_scene(connection, n=n, **id_info)
    pathrow_range_list = ['_{0:3s}{1:03d}_'.format(id_info['wrspath'], int(id_info['wrsrow']) + wrs_row_x)
                          for wrs_row_x in range(-n, n+1)]

    tile_list = []
    tilepath_scenes = {}

    if len(ls_prod_id_scenes) > 0:
        # Get coordinates for input scene and north and south scene.
        results = db.select_corner_polys(connection, ls_prod_id_scenes)
        scene_records = [r for r in results
                         if any(x in r['LANDSAT_PRODUCT_ID'] for x in pathrow_range_list)]
        logger.info('Coordinate query response: %s', scene_records)

        # Create geometry objects for each scenes coordinates
        scene_records = {r['LANDSAT_PRODUCT_ID']: ogr.CreateGeometryFromWkt(r['COORDS'])
                         for r in scene_records}

        # Find all the tiles that intersect the input scene
        # and put into a list

        region_shapefile = read_shapefile(region=region)

        layer = region_shapefile.GetLayer()
        spatialRef = layer.GetSpatialRef()
        for name in scene_records.keys():
            scene_records[name].AssignSpatialReference(spatialRef)
            logger.debug('???>>> %s ||', name)

        for feature2 in layer:
            geom2 = feature2.GetGeometryRef()

            # select only the intersections
            if geom2.Intersects(scene_records[product_id]):
                tile = {
                    'H':    feature2.GetField('H'),
                    'V':    feature2.GetField('V'),
                    'UL_X': feature2.GetField('UL_X'),
                    'LL_Y': feature2.GetField('LL_Y'),
                    'LR_X': feature2.GetField('LR_X'),
                    'UR_Y': feature2.GetField('UR_Y'),
                }
                tile_list.append(tile)

                for name in scene_records.keys():
                    # Add path, row of input scene to dictionary
                    key = ''.join([
                        'H%03d' % tile['H'],
                        'V%03d' % tile['V'],
                        'P', id_info['wrspath']
                    ])
                    # Now see if tile intersects with north and south scene
                    # and put into a dictionary
                    if geom2.Intersects(scene_records[name]):
                        if key not in tilepath_scenes:
                            tilepath_scenes[key] = list()

                        tilepath_scenes[key].append(landsat.match(product_id))

    logger.info('Tile list: {0}'.format(tile_list))
    logger.info('Neighboring scenes: {0}'.format(tilepath_scenes))

    return tile_list, tilepath_scenes


def read_shapefile(region='', ard_aux_dir=None):
    if (ard_aux_dir is None) and ('ARD_AUX_DIR' not in os.environ):
        logger.error('ARD_AUX_DIR environment variable not set')
        raise KeyError('ARD_AUX_DIR environment variable not set')
    elif (ard_aux_dir is None):
        ard_aux_dir = os.path.join(os.getenv('ARD_AUX_DIR'), "shapefiles")

    region_shp_filename = os.path.join(ard_aux_dir, region + "_ARD_tiles_geographic.shp")
    driver = ogr.GetDriverByName('ESRI Shapefile')
    logger.debug('Open shapefile %s', region_shp_filename)
    region_shapefile = driver.Open(region_shp_filename, 0) # 0 means read-only. 1 means writeable.

    if region_shapefile is None:
        logger.error('Could not open {0}'.format(region_shp_filename))
        raise IOError('Could not open {0}'.format(region_shp_filename))
    return region_shapefile
