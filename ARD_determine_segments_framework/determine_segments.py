""" This program determines consecutive scenes to be tiled """

import glob
from operator import itemgetter
from itertools import groupby


from util import logger
import config
import db
import framework


def same_day_paths(reform_list):
    """ group like satellite, acq_date and paths together in a list

    Args:
        reform_list (list): dict db results from ARD_UNPROCESSED query

    Returns:
        list: lists grouped by unique satellite, date, path

    Example:
        >>> group_product_ids([
        ...    {'LANDSAT_PRODUCT_ID': 'LT04_L2TP_026026_19830102_20161110_01_A1'},
        ...    {'LANDSAT_PRODUCT_ID': 'LT04_L2TP_026027_19830102_20161110_01_A1'},
        ...    {'LANDSAT_PRODUCT_ID': 'LE07_L2TP_022033_20140228_20161103_01_A1'}])
        [[{'LANDSAT_PRODUCT_ID': 'LT04_L2TP_026026_19830102_20161110_01_A1'},
          {'LANDSAT_PRODUCT_ID': 'LT04_L2TP_026027_19830102_20161110_01_A1'}],
         [{'LANDSAT_PRODUCT_ID': 'LE07_L2TP_022033_20140228_20161103_01_A1'}]]
    """
    parse_id = lambda x: ''.join([
        x['LANDSAT_PRODUCT_ID'][:4],
        x['LANDSAT_PRODUCT_ID'][17:25],
        x['LANDSAT_PRODUCT_ID'][10:13]])
    return [list(g) for _, g in groupby(reform_list, parse_id)]


def segments_group_list(reform_list):
    """ group sequential rows together in a list to create a segment

    Args:
        reform_list (list): dict db results from ARD_UNPROCESSED query

    Returns:
        list: list of segments grouped together

    Example:
        >>> segements_group_list([
        ...    {'LANDSAT_PRODUCT_ID': 'LT04_L2TP_026026_19830102_20161110_01_A1', 'WRS_ROW': '026'},
        ...    {'LANDSAT_PRODUCT_ID': 'LT04_L2TP_026027_19830102_20161110_01_A1', 'WRS_ROW': '027'},
        ...    {'LANDSAT_PRODUCT_ID': 'LE07_L2TP_022033_20140228_20161103_01_A1', 'WRS_ROW': '033'}])
        [[{'LANDSAT_PRODUCT_ID': 'LT04_L2TP_026026_19830102_20161110_01_A1',
           'WRS_ROW': '026'},
          {'LANDSAT_PRODUCT_ID': 'LT04_L2TP_026027_19830102_20161110_01_A1',
           'WRS_ROW': '027'}],
         [{'LANDSAT_PRODUCT_ID': 'LE07_L2TP_022033_20140228_20161103_01_A1',
           'WRS_ROW': '033'}]]
    """
    return [
        map(itemgetter(1), g) for segment in same_day_paths(reform_list)
        for _, g in groupby(enumerate(segment), lambda (i,x):i-int(x['WRS_ROW']))
    ]


def determine_segments(l2_db_con='', segment_query='', indir='', outdir='', force_input_dir=None, **kwargs):

    connection = db.connection(l2_db_con)
    scenes_to_process = db.select(connection, segment_query)

    logger.info("Number of scenes returned from query: {0}".format(len(scenes_to_process)))
    logger.debug("Complete scene list: {0}".format(scenes_to_process))

    segments = []
    if len(scenes_to_process) < 1:
        logger.info("There are no scenes ready to process.")
        return segments

    reformatted_list = []
    dup_scene_list = []
    prev_file_loc = "init"

    for record in scenes_to_process:
        # Get complete file name
        file_loc = record['FILE_LOC'].replace(indir, force_input_dir or indir)
        logger.debug('Search for file location: %s', file_loc)
        tarFileName = glob.glob(file_loc)
        if len(tarFileName) > 0:
            if tarFileName[0] != prev_file_loc:
                reformatted_list.append({k: (v if k != 'FILE_LOC' else tarFileName[0]) for k,v in record.items()})
            else:
                dup_scene_row = (record['LANDSAT_PRODUCT_ID'], tarFileName[0], 'DUPLICATE')
                dup_scene_list.append(dup_scene_row)
            prev_file_loc = tarFileName[0]

    db.duplicate_scenes_insert(connection, dup_scene_list)

    segments_list = segments_group_list(reformatted_list)
    segments_list.sort(reverse=True, key=len)
    logger.info("Number of segments found: {0}".format(len(segments_list)))

    return segments_list
