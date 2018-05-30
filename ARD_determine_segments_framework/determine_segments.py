""" This program determines consecutive scenes to be tiled """

import re
import glob
from operator import itemgetter
from itertools import groupby
from functools import partial


from util import logger
import config
import db


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


def format_segment(record, indir='', force_input_dir='', ):
    """ Search filesystem for complete file name matching FILE_LOC glob """
    regex = re.compile(indir)
    file_loc = regex.sub(force_input_dir or indir,
                         record['FILE_LOC'])
    logger.debug('Search for file location: %s', file_loc)
    tarFileName = glob.glob(file_loc)
    if len(tarFileName) > 0:
        return {k: (v if k != 'FILE_LOC' else tarFileName[0])
                for k,v in record.items()}


def filter_dups(records):
    """ Separate duplicate matches """
    key = lambda x: x['FILE_LOC']
    groups = groupby(sorted(records, key=key), key)
    groups = [(k, list(m)) for k,m in groups]
    reals, dups = list(), list()
    p_dup = lambda x: (x['LANDSAT_PRODUCT_ID'], x['FILE_LOC'], 'DUPLICATE')
    for file_loc, matches in groups:
        if len(list(matches)) > 1:
            logger.warning('Duplicate matches for [ %s ]: %s', file_loc, list(matches))
            dups.extend(map(p_dup,  list(matches)[1:]))
        reals.append(list(matches)[0])
    return dups, reals


def determine_segments(l2_db_con='', segment_query='', indir='', outdir='', force_input_dir=None, **kwargs):

    scenes_to_process = db.select(db.connect(l2_db_con), segment_query)

    logger.info("Number of scenes returned from query: {0}".format(len(scenes_to_process)))
    logger.debug("Complete scene list: {0}".format(scenes_to_process))

    segments = []
    if len(scenes_to_process) < 1:
        logger.info("There are no scenes ready to process.")
        return segments

    fmt_segment = partial(format_segment, indir=indir,
                          force_input_dir=force_input_dir)
    segments = map(fmt_segment, scenes_to_process)
    dup_scenes, segments = filter_dups(segments)

    db.duplicate_scenes_insert(db.connect(l2_db_con), dup_scenes)

    segments = segments_group_list(segments)
    segments.sort(reverse=True, key=len)
    logger.info("Number of segments found: {0}".format(len(segments)))

    return segments
