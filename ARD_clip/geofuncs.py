"""Intersect footprints/grids or parse geospatial datasets."""
import os
import sys
import urllib2

from osgeo import gdal, osr, ogr
import numpy as np

import db
import landsat
from util import logger


def parseHistFile(histFilename):
    """Count histogram for Pixel-QA band for metadata percents."""
    if (not os.path.isfile(histFilename)):
        return ('ERROR - File does not exist',)
    else:
        try:
            infile = open(histFilename, 'r')
            histLines = infile.read()
            infile.close()
        except Exception:
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

    return (countFill, countClear, countWater, countSnow,
            countShadow, countCloud)


def parse_gdal_hist_output(gdal_hist_output):
    """Count histogram values from Lineage-QA, for potential all fill"""
    bucket_index = [i for i, line in enumerate(gdal_hist_output)
                    if 'buckets from ' in line].pop()
    histArray = gdal_hist_output[bucket_index + 1].split()
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


def sum_counts(unique_values, count_list, target_bit):
    """Return counts for each cover type."""
    # determine if each unique value contains target bit
    bit_bool = unique_values & 1 << target_bit > 0

    # sum counts from all values containing target bit
    final_count = np.sum(np.array(count_list)[bit_bool])

    return final_count


def raster_value_count(raster_in, tile_id):
    """Parse Pixel-QA GTIFF file for metadata percentages."""
    # open raster, read first band as array
    ds = gdal.Open(raster_in)
    band_arr = np.array(ds.GetRasterBand(1).ReadAsArray())

    # get unique values from array
    uni, counts = np.unique(band_arr, return_counts=True)

    # count bits
    bit_counts = {
        'fill': sum_counts(uni, counts, 0),
        'clear': sum_counts(uni, counts, 1),
        'water': sum_counts(uni, counts, 2),
        'cloud_shadow': sum_counts(uni, counts, 3),
        'snow_ice': sum_counts(uni, counts, 4),
        'cloud_cover': sum_counts(uni, counts, 5),
    }

    # get high-conf cirrus and terrain occlusion for L8
    if tile_id.startswith('LC08'):
        bit_counts['cirrus'] = sum_counts(uni, counts, 9)
        bit_counts['terrain'] = sum_counts(uni, counts, 10)

    logger.debug('        # pixels Fill: %s',
                 bit_counts.get('fill'))
    logger.debug('        # pixels Clear: %s',
                 bit_counts.get('clear'))
    logger.debug('        # pixels Water: %s',
                 bit_counts.get('water'))
    logger.debug('        # pixels Snow: %s',
                 bit_counts.get('snow_ice'))
    logger.debug('        # pixels CloudShadow: %s',
                 bit_counts.get('cloud_shadow'))
    logger.debug('        # pixels CloudCover: %s',
                 bit_counts.get('cloud_cover'))
    logger.debug('        # pixels Cirrus: %s',
                 bit_counts.get('cirrus'))
    logger.debug('        # pixels Terrain: %s',
                 bit_counts.get('terrain'))

    return bit_counts


def get_tile_scene_intersections(connection, product_id, region, n=2):
    """Intersect Tile IDs with product_id and neighboring same-day acqs.

    Args:
        connection (cx_Oracle.Connection): open database connection
        product_id (str): landsat collection product id
        region (str): tile grid region following shapefile naming
        n (int): consecutive rows to intersect with

    Returns:
        list: tile ids that intersect the input landsat product id
        dict: tile ids which contain required path/rows

    Example:
        >>> get_tile_scene_intersections(db,
        ...     'LT04_L2TP_035027_19890712_20161110_01_A1', 'CU', n=2)
        ([{'H': 11,
           'V': 3,
           'LL_Y': 2714805,
           'LR_X': -765585,
           'UL_X': -915585,
           'UR_Y': 2864805}, ...],
        { 'H011V004': [{'acqdate': '19890712',
            'mission': 'LT04',
            'procdate': '20161110',
            'wrspath': '035',
            'wrsrow': '027'}, ...], ...})

    """
    # We need to get 2 consecutive wrsRows north and 2 consecutive wrsRows
    # south of input scene to account for possible 3 scene tile.
    id_info = landsat.match(product_id)
    logger.info('Select consecutive scenes for %s', product_id)
    ls_prod_id_scenes = db.select_consecutive_scene(connection, n=n,
                                                    **id_info)
    pathrow_range_list = ['_{0:3s}{1:03d}_'.format(id_info['wrspath'],
                          int(id_info['wrsrow']) + wrs_row_x)
                          for wrs_row_x in range(-n, n+1)]

    tile_list = []
    tilepath_scenes = {}

    if len(ls_prod_id_scenes) > 0:
        # Get coordinates for input scene and north and south scene.
        results = db.select_corner_polys(connection, ls_prod_id_scenes)
        scene_records = [r for r in results
                         if any(x in r['LANDSAT_PRODUCT_ID'] for x in
                                pathrow_range_list)]
        logger.info('Coordinate query response: %s', scene_records)

        # Create geometry objects for each scenes coordinates
        scene_records = {
            r['LANDSAT_PRODUCT_ID']: ogr.CreateGeometryFromWkt(r['COORDS'])
            for r in scene_records
        }

        # Find all the tiles that intersect the input scene
        # and put into a list
        region_shapefile = read_shapefile(region=region)

        layer = region_shapefile.GetLayer()
        spatialRef = layer.GetSpatialRef()
        for name in scene_records.keys():
            scene_records[name].AssignSpatialReference(spatialRef)

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
                    key = 'H{H:03d}V{V:03d}'.format(**tile)
                    # Now see if tile intersects with north and south scene
                    # and put into a dictionary
                    if geom2.Intersects(scene_records[name]):
                        if key not in tilepath_scenes:
                            tilepath_scenes[key] = list()

                        tilepath_scenes[key].append(landsat.match(name))

    logger.info('Tile list: {0}'.format(tile_list))
    logger.info('Neighboring scenes: {0}'.format(tilepath_scenes))

    return tile_list, tilepath_scenes


def read_shapefile(region='', ard_aux_dir=None):
    """Read region shapefile from ARD_AUX_DIR."""
    if (ard_aux_dir is None) and ('ARD_AUX_DIR' not in os.environ):
        logger.error('ARD_AUX_DIR environment variable not set')
        raise KeyError('ARD_AUX_DIR environment variable not set')
    elif (ard_aux_dir is None):
        ard_aux_dir = os.path.join(os.getenv('ARD_AUX_DIR'), "shapefiles")

    region_shp_filename = os.path.join(ard_aux_dir, region +
                                       "_ARD_tiles_geographic.shp")
    driver = ogr.GetDriverByName('ESRI Shapefile')
    logger.debug('Open shapefile %s', region_shp_filename)
    read_only = 0
    region_shapefile = driver.Open(region_shp_filename, read_only)

    if region_shapefile is None:
        logger.error('Could not open {0}'.format(region_shp_filename))
        raise IOError('Could not open {0}'.format(region_shp_filename))
    return region_shapefile
