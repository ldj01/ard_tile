#!/usr/bin/python

import os
import sys
import glob
import shutil
import logging


import db
import util
import config
import landsat
import external
import geofuncs
from util import logger
from ARD_regionLU import pathrow2regionLU
from ARD_metadata import buildMetadata


def process_tile(current_tile, segment, region, tiles_contrib_scenes, output_path, conf):
    """ Big loop for each tile needed """

    production_timestamp = landsat.get_production_timestamp()
    tile_id = landsat.generate_tile_id(segment['LANDSAT_PRODUCT_ID'], current_tile, region, conf.collection, conf.version)
    clip_extents = '{UL_X} {LL_Y} {LR_X} {UR_Y}'.format(**current_tile)

    logger.debug("tile_id: %s", tile_id)
    logger.debug("clip_extents: %s", clip_extents)

    # See if tile_id exists in ARD_COMPLETED_TILES table
    tile_rec = db.check_tile_status(db.connection(conf.connstr), tile_id)
    logger.debug("Tile status: %s", tile_rec)

    if len(tile_rec) != 0:
        logger.error('Tile already created! %s', tile_rec)
        raise ArdTileException

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
    logger.info('All scenes found to complete tile: %s', is_complete_tile)
    logger.info('Tile: %s  - Number of contributing scenes: %d', tile_id, n_contrib_scenes)

    # Maximum # of scenes that can contribute to a tile is 3
    invalid_contrib_scenes = ((n_contrib_scenes > conf.maxscenespertile)
                              or (n_contrib_scenes < conf.minscenespertile))
    if invalid_contrib_scenes:
        logger.info('Unexpected number of scenes %d', n_contrib_scenes)
        raise ArdTileException


    if conf.hsmstage:
        # Stage files to disk cache for faster access
        external.stage_files(contributing_scenes.values(), conf.soap_envelope)

    # If each contributing scene is not already unpacked, do it here
    for product_id, tar_file_location in contributing_scenes.items():

        logger.info('Required Scene: %s', tar_file_location)
        try:
            directory = os.path.join(conf.workdir, product_id)
            scene_dir = util.untar_archive(tar_file_location, directory=directory)
        except:
            logger.exception('Error staging input data for {}: {}'.format(product_id, tar_file_location))
            raise ArdTileException

    logger.info('Starting to build tile: %s', tile_id)

    # Determine which scene(s) will overlay the other scene(s) for this tile.
    # North scenes (lower row #) will always overlay South scenes (higher row #)
    stacking = {i: {'LANDSAT_PRODUCT_ID': name,
                    'XML_LOC': util.ffind(conf.workdir, name, '*.xml')}
                for i, name in zip(range(1, n_contrib_scenes+1),
                                   sorted(contributing_scenes.keys()))}

    util.make_dirs(os.path.join(conf.workdir, tile_id))

    producers = config.read_processing_config(sensor=segment['SATELLITE'])

    datatypes = {
        1: process_bandtype_1,
        2: process_bandtype_2,
        3: process_bandtype_3,
        4: process_bandtype_4,
        5: process_bandtype_5,
        6: process_bandtype_6, # WARNING: Type 6 requires LINEAGE to be run first
        7: process_bandtype_7, # WARNING: Type 7 requires LINEAGE to be run first
        8: process_bandtype_8, # WARNING: Type 8 requires LINEAGE to be run first
    }

    outputs = dict()
    for product_request in sorted(conf.products, reverse=True):
        logging.info('Create product %s', product_request)
        required_bands = config.determine_output_products(producers, product_request)
        for band_name, rename in required_bands.items():
            dtype = config.datatype_searches(producers, band_name)
            logger.info('Requires base band_name %s (Type %s)', band_name, dtype)

            # Process the current dataset type
            filename = datatypes[dtype](stacking, band_name, clip_extents, tile_id, rename, conf.workdir)
            outputs[band_name] = filename

            if band_name == 'toa_band1':
                outputs['_LINEAGE'] = process_lineage(stacking, band_name, clip_extents, tile_id, 'LINEAGEQA', conf.workdir)
                lng_count, lng_array = process_lineage_contributing(outputs['_LINEAGE'], n_contrib_scenes, tile_id, conf.workdir)

        if filename in ('ERROR', 'NOT NEEDED'):
            return filename

    outputs['XML'] = process_metadata(segment, stacking, tile_id, clip_extents, region, lng_count, lng_array,
                                      production_timestamp, producers, outputs, conf.workdir)

    process_output(conf.products, producers, outputs, tile_id, output_path)
    util.process_checksums(indir=conf.workdir, outdir=output_path)
    process_browse(producers['browse'], conf.workdir, tile_id, output_path)

    # Remove the temporary work directory, but keep adjacent scenes for other tiles
    if not conf.debug:
        logger.info('    Cleanup: Removing temp directory: {0} ...'.format(os.path.join(conf.workdir, tile_id)))
        util.remove(os.path.join(conf.workdir, tile_id))

        # No errors making this tile, record it in our database
        completed_tile_list = [[tile_id,",".join(contributing_scenes.keys()), is_complete_tile, "SUCCESS"]]
        db.insert_tile_record(db.connection(conf.connstr), completed_tile_list)
    return "SUCCESS"


def process_bandtype_1(stacking, band_name, clip_extents, tile_id, rename, workdir):
    # --------------------------------------------------------------------------------------------------------------------------- #
    #
    #     Band Type 1
    #
    #                   16 bit signed integer
    #     		NoData = -9999
    #
    #
    logger.info('     Start processing for band: %s', band_name)
    mosaic_filename =  os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    clip_params = ' -dstnodata "-9999" -srcnodata "-9999" '
    clip_params += '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '

    warp_cmd = 'gdalwarp -te ' + clip_extents + clip_params
    for level, stack in stacking.items():
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*' + band_name + '.tif')
        warp_cmd += ' ' + scene_name
    warp_cmd += ' ' + mosaic_filename
    util.execute_cmd(warp_cmd)

    logger.info('    End processing for %s as %s ', band_name, mosaic_filename)
    if not os.path.exists(mosaic_filename):
        logger.error('Processing failed to generate desired output: %s', mosaic_filename)
    return mosaic_filename


def process_bandtype_2(stacking, band_name, clip_extents, tile_id, rename, workdir):
    # --------------------------------------------------------------------------------------------------------------------------- #
    #
    #     Band Type 2
    #
    #                   16 bit signed integer
    #     		NoData = -32768
    #
    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename =  os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    clip_params = ' -dstnodata "-32768" -srcnodata "-32768" '
    clip_params += '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '

    warp_cmd = 'gdalwarp -te ' + clip_extents + clip_params
    for level, stack in stacking.items():
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*' + band_name + '.tif')
        warp_cmd += ' ' + scene_name
    warp_cmd += ' ' + mosaic_filename
    util.execute_cmd(warp_cmd)

    logger.info('    End processing for %s as %s ', band_name, mosaic_filename)
    if not os.path.exists(mosaic_filename):
        logger.error('Processing failed to generate desired output: %s', mosaic_filename)
    return mosaic_filename


def process_bandtype_3(stacking, band_name, clip_extents, tile_id, rename, workdir):
    # --------------------------------------------------------------------------------------------------------------------------- #
    #
    #     Band Type 3
    #
    #     		16 bit unsigned integer
    #		NoData = 1  in ovelap areas and scan gaps
    #
    #
    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename =  os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename


    warp_cmd = 'gdalwarp -te ' + clip_extents
    warp_cmd += ' -dstnodata "1" -srcnodata "1" '

    for level, stack in stacking.items():
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*' + band_name + '.tif')
        warp_cmd += ' ' + scene_name

    temp_filename =  os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')
    warp_cmd += ' ' + temp_filename
    util.execute_cmd(warp_cmd)

    # reassign nodata
    warp_cmd = 'gdalwarp -dstnodata "0" -srcnodata "None" '
    warp_cmd += '-co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
    warp_cmd += temp_filename + ' ' + mosaic_filename
    util.execute_cmd(warp_cmd)

    util.remove(temp_filename)

    logger.info('    End processing for %s as %s ', band_name, mosaic_filename)
    if not os.path.exists(mosaic_filename):
        logger.error('Processing failed to generate desired output: %s', mosaic_filename)
    return mosaic_filename


def process_bandtype_4(stacking, band_name, clip_extents, tile_id, rename, workdir):
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
    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename =  os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    warp_cmd = 'gdalwarp -te ' + clip_extents
    clip_params = ' -dstnodata "1" -srcnodata "1" '

    temp_names = list()
    for level, stack in stacking.items():
        temp_name =  os.path.join(workdir, tile_id, tile_id + '_temp%d' % level + '.tif')
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*' + band_name + '.tif')
        temp_warp_cmd = warp_cmd + ' ' + clip_params +  scene_name + ' ' + temp_name
        util.execute_cmd(temp_warp_cmd)
        temp_names.append(temp_name)

    warp_cmd = 'gdalwarp -co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
    warp_cmd += ' '.join(temp_names)
    warp_cmd += ' ' + mosaic_filename
    util.execute_cmd(warp_cmd)
    util.remove(*temp_names)

    logger.info('    End processing for %s as %s ', band_name, mosaic_filename)
    if not os.path.exists(mosaic_filename):
        logger.error('Processing failed to generate desired output: %s', mosaic_filename)
    return mosaic_filename


def process_bandtype_5(stacking, band_name, clip_extents, tile_id, rename, workdir):
    """ Band Type 5 DSWE NoData Convert from 255 to -9999 """
    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename =  os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    warp_cmd = 'gdalwarp -te ' + clip_extents
    clip_params = ' -dstnodata "-9999" -srcnodata "255" '

    temp_names = list()
    for level, stack in stacking.items():
        temp_name =  os.path.join(workdir, tile_id, tile_id + '_temp%d' % level + '.tif')
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*' + band_name + '.tif')
        temp_warp_cmd = warp_cmd + ' ' + clip_params +  scene_name + ' ' + temp_name
        util.execute_cmd(temp_warp_cmd)
        temp_names.append(temp_name)

    warp_cmd = 'gdalwarp -co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
    warp_cmd += ' '.join(temp_names)
    warp_cmd += ' ' + mosaic_filename
    util.execute_cmd(warp_cmd)
    util.remove(*temp_names)

    logger.info('    End processing for %s as %s ', band_name, mosaic_filename)
    if not os.path.exists(mosaic_filename):
        logger.error('Processing failed to generate desired output: %s', mosaic_filename)
    return mosaic_filename


def process_lineage(stacking, band_name, clip_extents, tile_id, rename, workdir):
    """ Create the lineage file """
    logger.info('     Start processing for band: %s', rename)

    lineage_filename = os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')

    if os.path.exists(lineage_filename):
        logger.warning("Skip previously generated result %s", lineage_filename)
        return lineage_filename

    temp_names = list()
    for level, stack in stacking.items():
        temp_name =  os.path.join(workdir, tile_id, tile_id + '_srcTemp%d' % level + '.tif')
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*' + band_name + '.tif')

        calc_cmd = (
            'gdal_calc.py -A {scene} --outfile {temp}'
            ' --calc=" {level} * (A > -101)" --type="Byte" --NoDataValue=0'
        )
        util.execute_cmd(calc_cmd.format(level=level, temp=temp_name, scene=scene_name))
        temp_names.append(temp_name)

    warp_cmd = (
        'gdalwarp -te {extents} -dstnodata "0" -srcnodata "0" -ot "Byte" -wt "Byte"'
        ' -co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2" '
    ).format(extents=clip_extents)
    warp_cmd += ' '.join(temp_names)
    warp_cmd += ' ' + lineage_filename
    util.execute_cmd(warp_cmd)
    util.remove(*temp_names)

    logger.info('    End processing for %s as %s ', band_name, lineage_filename)
    if not os.path.exists(lineage_filename):
        logger.error('Processing failed to generate desired output: %s', lineage_filename)
    return lineage_filename


def process_lineage_contributing(lineage_filename, n_contrib_scenes, tile_id, workdir):
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

    info_cmd = 'gdalinfo -hist {}'
    results = util.execute_cmd(info_cmd.format(lineage_filename))
    util.remove(lineage_filename + '.aux.xml')  # TODO: could potentially use this instead...
    count, array = geofuncs.parse_gdal_hist_output(results['output'])

    if (count == 0):
        logger.error('Parsing histogram from lineage file found 0 contributing scenes')
        raise ArdTileNotNeededException()


    # decrement pixel values in lineage file if some scenes didn't contribute
    # any pixels
    if (count != n_contrib_scenes):

        decrement_value = n_contrib_scenes - count
        sceneCountDifference = decrement_value

        # Determine whether we need decrement the pixel values in the lineage file or not.
        calcExpression = ''
        if sceneCountDifference == 1:
            if array[0] == 0:
                calcExpression = ' --calc="A-' + str(decrement_value) + '"'
            elif array[1] == 0 and array[2] > 0:
                calcExpression = ' --calc="A-(A==3)"'
        elif sceneCountDifference == 2:
            if array[0] == 0 and array[1] == 0:
                calcExpression = ' --calc="A-' + str(decrement_value) + '"'
            elif array[0] == 0 and array[2] == 0:
                calcExpression = ' --calc="A-' + str(1) + '"'

        if calcExpression != '':
            temp_name = lineage_filename.replace('.tif', '_linTemp.tif')
            calc_cmd = (
                'gdal_calc.py -A {lineage} --outfile {temp} {calc} --type="Byte" --NoDataValue=0 --overwrite'
            )
            util.execute_cmd(calc_cmd.format(lineage=lineage_filename, temp=temp_name, calc=calcExpression))

            # compress
            warp_cmd = (
                'gdalwarp -co "compress=deflate" -co "zlevel=9" -co "tiled=yes" -co "predictor=2"'
                ' -overwrite {} {}'
            )
            util.execute_cmd(warp_cmd.format(temp_name, lineage_filename))
            util.remove(temp_name)

    logger.info('finish updating contributing scenes')
    return count, array


def process_bandtype_6(stacking, band_name, clip_extents, tile_id, rename, workdir):
    # --------------------------------------------------------------------------------------------------------------------------- #
    #
    #     Band Type 6
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
    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename =  os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename


    temp_clipped_names = list()
    temp_masked_names = list()
    for level, stack in stacking.items():
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*' + band_name + '.tif')
        lineg_name = util.ffind(workdir, tile_id, '*LINEAGEQA.tif')

        temp_name1 =  os.path.join(workdir, tile_id, tile_id + '_temp%d' % level + '.tif')
        temp_warp_cmd = 'gdalwarp -te {extents} -dstnodata "0" -ot "Byte" -wt "Byte" {0} {1}'
        util.execute_cmd(temp_warp_cmd.format(scene_name, temp_name1, extents=clip_extents))
        temp_clipped_names.append(temp_name1)

        temp_name2 =  os.path.join(workdir, tile_id, tile_id + '_temp%dM' % level + '.tif')
        temp_calc_cmd = (
            'gdal_calc.py -A {0} -B {lineage} --outfile {1} --calc="A*(B=={level})" '
            '--type="Byte" --NoDataValue=0'
        )
        util.execute_cmd(temp_calc_cmd.format(temp_name1, temp_name2, lineage=lineg_name, level=level))
        temp_masked_names.append(temp_name2)

    temp_name =  os.path.join(workdir, tile_id, tile_id + '_temp.tif')
    temp_warp_cmd = 'gdalwarp {} {}'.format(' '.join(temp_masked_names), temp_name)
    util.execute_cmd(temp_warp_cmd)
    util.remove(*temp_masked_names + temp_clipped_names)

    warp_cmd = (
        'gdalwarp -dstnodata "None" -srcnodata "256" -co "compress=deflate"'
        ' -co "zlevel=9" -co "tiled=yes" -co "predictor=2" {} {}'
    )
    util.execute_cmd(warp_cmd.format(temp_name, mosaic_filename))
    util.remove(temp_name)

    logger.info('    End processing for %s as %s ', band_name, mosaic_filename)
    if not os.path.exists(mosaic_filename):
        logger.error('Processing failed to generate desired output: %s', mosaic_filename)
    return mosaic_filename


def process_bandtype_7(stacking, band_name, clip_extents, tile_id, rename, workdir):
    """ Band Type 7 NoData 255 and uses LINEAGE """

    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename =  os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    temp_clipped_names = list()
    temp_masked_names = list()
    for level, stack in stacking.items():
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*' + band_name + '.tif')

        temp_name1 =  os.path.join(workdir, tile_id, tile_id + '_temp%d' % level + '.tif')
        temp_warp_cmd = 'gdalwarp -te {extents} -dstnodata "1" -srcnodata "1"  {0} {1}'
        util.execute_cmd(temp_warp_cmd.format(scene_name, temp_name1, extents=clip_extents))
        temp_clipped_names.append(temp_name1)

        lineg_name = util.ffind(workdir, tile_id, '*LINEAGEQA.tif')
        temp_name2 =  os.path.join(workdir, tile_id, tile_id + '_temp%dM' % level + '.tif')
        temp_calc_cmd = (
            'gdal_calc.py -A {0} -B {lineage} --outfile {1} --calc="A*(B=={level})" '
            ' --NoDataValue=1'
        )
        util.execute_cmd(temp_calc_cmd.format(temp_name1, temp_name2, lineage=lineg_name, level=level))
        temp_masked_names.append(temp_name2)

    temp_name =  os.path.join(workdir, tile_id, tile_id + '_temp.tif')
    temp_warp_cmd = 'gdalwarp {} {}'.format(' '.join(temp_masked_names), temp_name)
    util.execute_cmd(temp_warp_cmd)
    util.remove(*temp_masked_names + temp_clipped_names)

    warp_cmd = (
        'gdalwarp -dstnodata "1" -srcnodata "1" -co "compress=deflate"'
        ' -co "zlevel=9" -co "tiled=yes" -co "predictor=2" {} {}'
    )
    util.execute_cmd(warp_cmd.format(temp_name, mosaic_filename))
    util.remove(temp_name)

    logger.info('    End processing for %s as %s ', band_name, mosaic_filename)
    if not os.path.exists(mosaic_filename):
        logger.error('Processing failed to generate desired output: %s', mosaic_filename)
    return mosaic_filename


def process_bandtype_8(stacking, band_name, clip_extents, tile_id, rename, workdir):
    """ Band Type 8 NoData -9999 and uses LINEAGE """

    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename =  os.path.join(workdir, tile_id, tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    temp_clipped_names = list()
    temp_masked_names = list()
    for level, stack in stacking.items():
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*' + band_name + '.tif')

        temp_name1 =  os.path.join(workdir, tile_id, tile_id + '_temp%d' % level + '.tif')
        temp_warp_cmd = 'gdalwarp -te {extents} -dstnodata "-9999" -srcnodata "-9999"  {0} {1}'
        util.execute_cmd(temp_warp_cmd.format(scene_name, temp_name1, extents=clip_extents))
        temp_clipped_names.append(temp_name1)

        lineg_name = util.ffind(workdir, tile_id, '*LINEAGEQA.tif')
        temp_name2 =  os.path.join(workdir, tile_id, tile_id + '_temp%dM' % level + '.tif')
        temp_calc_cmd = (
            'gdal_calc.py -A {0} -B {lineage} --outfile {1} --calc="A*(B=={level})" '
            ' --NoDataValue=-9999'
        )
        util.execute_cmd(temp_calc_cmd.format(temp_name1, temp_name2, lineage=lineg_name, level=level))
        temp_masked_names.append(temp_name2)

    temp_name =  os.path.join(workdir, tile_id, tile_id + '_temp.tif')
    temp_warp_cmd = 'gdalwarp {} {}'.format(' '.join(temp_masked_names), temp_name)
    util.execute_cmd(temp_warp_cmd)
    util.remove(*temp_masked_names + temp_clipped_names)

    warp_cmd = (
        'gdalwarp -dstnodata "-9999" -srcnodata "-9999" -co "compress=deflate"'
        ' -co "zlevel=9" -co "tiled=yes" -co "predictor=2" {} {}'
    )
    util.execute_cmd(warp_cmd.format(temp_name, mosaic_filename))
    util.remove(temp_name)

    logger.info('    End processing for %s as %s ', band_name, mosaic_filename)
    if not os.path.exists(mosaic_filename):
        logger.error('Processing failed to generate desired output: %s', mosaic_filename)
    return mosaic_filename


def process_metadata(segment, stacking, tile_id, clip_extents, region, lng_count, lng_array,
                     production_timestamp, producers, tiled_filenames, workdir):
    """ Create the tile metadata file, Generate statistics we will need """

    logger.info('     Start processing for metadata')

    pqa_name = util.ffind(workdir, tile_id, '*PIXELQA.tif')
    bit_counts = geofuncs.raster_value_count(pqa_name, tile_id)

    metadata_locs = list()
    for n_count, (level, stack) in zip(lng_array, stacking.items()):
        metadata_locs.append({
            'L2XML': util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], "*.xml"),
            'L1MTL': util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'], '*_MTL.txt'),
        })

    if {'ARD', 'L3'} != set(producers['xml'].keys()) or ['SW'] != producers['xml']['L3']:
        raise NotImplementedError('Logic for making _SW.xml is very brittle... ')

    filenames = dict()
    for xml_group, products in producers['xml'].items():

        xml_id = str(tile_id)
        if len(products) == 1:
            xml_id = tile_id + '_' + products[-1]

        logger.info('     Start processing for metadata group: %s', xml_group)
        metadata_filename =  os.path.join(workdir, tile_id, xml_id + '.xml')

        if os.path.exists(metadata_filename):
            logger.warning("Skip previously generated result %s", metadata_filename)
            continue

        group_filenames = {k:v  for p in products
                           for k,v in config.determine_output_products(producers, p).items()}
        buildMetadata(metadata_filename, bit_counts, clip_extents, tile_id, metadata_locs,
                      production_timestamp, group_filenames, segment, region, lng_count)

        util.make_file_group_writeable(metadata_filename)
        logger.info('    End processing for metadata as %s ', metadata_filename)
        if not os.path.exists(metadata_filename):
            logger.error('Processing failed to generate desired output: %s', metadata_filename)
        filenames.update({p: metadata_filename for p in products})
    return filenames


def process_output(products, producers, outputs, tile_id, output_path):
    """ Package all of the Landsat  mosaics into the .tar files """
    util.make_dirs(output_path)

    for product_request in sorted(products, reverse=True):
        output_archive = os.path.join(output_path, tile_id + '_' + product_request + '.tar')
        logging.info('Create product %s', output_archive)
        required_bands = config.determine_output_products(producers, product_request)
        included = [outputs[x] for x in required_bands.keys() + ['_LINEAGE']]
        included.append(outputs['XML'][product_request])
        util.tar_archive(output_archive, included)
    logger.info('    End zipping')


def process_browse(bands, workdir, tile_id, outpath):
    """ Create the browse file for EE """

    logger.info('     Start processing for BROWSE')

    browse_filename =  os.path.join(outpath, tile_id + '.tif')

    if os.path.exists(browse_filename):
        logger.warning("Skip previously generated result %s", browse_filename)
        return browse_filename

    bands = {k: util.ffind(workdir, tile_id, tile_id + '_' + v + '.tif')
             for k, v in bands.items()}

    # create RGB image
    temp_filename1 = os.path.join(workdir, tile_id, tile_id + '_brw1.tif')
    merge_cmd = 'gdal_merge.py -o {outfile} -separate {red} {green} {blue}'
    util.execute_cmd(merge_cmd.format(outfile=temp_filename1, **bands))

    # scale the pixel values
    temp_filename2 = os.path.join(workdir, tile_id, tile_id + '_brw2.tif')
    scale_cmd = 'gdal_translate -scale 0 10000 -ot Byte {} {}'
    util.execute_cmd(scale_cmd.format(temp_filename1, temp_filename2))

    # apply compression
    comp_cmd = 'gdal_translate -co COMPRESS=JPEG -co PHOTOMETRIC=YCBCR {} {}'
    util.execute_cmd(comp_cmd.format(temp_filename2, browse_filename))

    # internal pyramids
    addo_cmd = 'gdaladdo {} 2 4 8 16'
    util.execute_cmd(addo_cmd.format(browse_filename))
    util.remove(temp_filename1, temp_filename2)

    logger.info('    End building browse.')
    if not os.path.exists(browse_filename):
        logger.error('Processing failed to generate desired output: %s', browse_filename)
    return browse_filename

class ArdTileNotNeededException(Exception):
    """ Lineage found All-Fill inside sensor field of view """
    pass


class ArdTileException(Exception):
    """ An unrecoverable error has occurred """
    pass


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
    hv_tiles, tile_scenes = geofuncs.get_tile_scene_intersections(db.connection(conf.connstr),
                                                                  segment['LANDSAT_PRODUCT_ID'],
                                                                  region)

    logger.info('Number of tiles to create: %d', len(hv_tiles))
    if len(hv_tiles) == 0:
        logger.error('No scene coordinates found for %s', segment['LANDSAT_PRODUCT_ID'])

        scene_state = "ERROR"
        # update PROCESSING_STATE in ARD_PROCESSED_SCENES
        db.update_scene_state(db.connection(conf.connstr), segment['LANDSAT_PRODUCT_ID'], scene_state)
        return scene_state

    for current_tile in hv_tiles:
        try:
            tile_state = process_tile(current_tile, segment, region, tile_scenes, output_path, conf)
        except ArdTileNotNeededException:
            logger.warning('Lineage file found 0 contributing scenes, set to NOT NEEDED')
            tile_state = 'NOT NEEDED'
        except ArdTileException:
            logger.exception('Error caught while processing tile {} !'.format(current_tile))
            tile_state = 'ERROR'
        except Exception:
            logger.exception('Unexpected error processing tile {} !'.format(current_tile))
            tile_state = 'ERROR'

        if tile_state in ('ERROR', 'NOT NEEDED'):
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
            util.remove(unneededScene)

        scene_state = process_segment(segment, output_path, conf)
        logger.info("Scene %s is %s.", segment['LANDSAT_PRODUCT_ID'], scene_state)
        logger.info('Segment loop: %d', i)

    # cleanup any remaining scene directories
    for segment in segments:
        unneededScene = segment['LANDSAT_PRODUCT_ID']
        if not conf.debug:
           util.remove(unneededScene)
