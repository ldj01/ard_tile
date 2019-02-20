"""Intersect ARD Tile Grid and clip scenes."""

import os
import logging
import time

import db
import util
from util import logger
import config
import landsat
import external
import geofuncs
from ARD_regionLU import pathrow2regionLU
from ARD_metadata import buildMetadata


def process_tile(current_tile, tile_id, segment, region,
                 tiles_contrib_scenes, output_path, conf):
    """Process each tile needed for segment.

    Args:
        current_tile (dict):  information about current tile
        tile_id (str): tile id string (e.g. LT04_CU_011003_...)
        segment (dict): information about a scene
        region (str): ARD grid tile area (e.g. CU, AK, HI)
        tiles_contrib_scenes (list): neighboring scene details
        output_path (str): path to store outputs
        conf (dict): runtime configuration options

    """
    production_timestamp = landsat.get_production_timestamp()
    clip_extents = '{UL_X} {LL_Y} {LR_X} {UR_Y}'.format(**current_tile)

    logger.debug("tile_id: %s", tile_id)
    logger.debug("clip_extents: %s", clip_extents)

    # See if tile_id exists in ARD_COMPLETED_TILES table
    tile_rec = db.check_tile_status(db.connect(conf.connstr), tile_id)
    logger.debug("Tile status: %s", tile_rec)

    if tile_rec:
        logger.error('Tile already created! %s', tile_rec)
        raise ArdTileException

    logger.info("Create Tile %s", tile_id)

    # Get file location for scenes that will contribute to the tile
    key = 'H{H:03d}V{V:03d}'.format(**current_tile)
    contrib_tile_scenes = tiles_contrib_scenes[key]
    logger.debug('# Scenes needed for tile %s: %d',
                 tile_id, len(contrib_tile_scenes))

    contributing_scenes = dict()
    for contrib_record in contrib_tile_scenes:
        wildcard = '{wrspath}{wrsrow}_{acqdate}'.format(**contrib_record)
        logger.debug("          Contributing scene: %s", wildcard)

        provided_contrib_rec = []
        if wildcard in segment['LANDSAT_PRODUCT_ID']:
            provided_contrib_rec = {
                segment['LANDSAT_PRODUCT_ID']: segment['FILE_LOC']
            }
            logger.info("Contributing scene from segment: %s",
                        provided_contrib_rec)
            contributing_scenes.update(provided_contrib_rec)

        if not provided_contrib_rec:
            db_contrib_rec = db.fetch_file_loc(db.connect(conf.connstr),
                                               sat=segment['SATELLITE'],
                                               wildcard=wildcard)
            logger.debug('Fetch file locations from DB: %s', db_contrib_rec)
            if db_contrib_rec:
                db_contrib_rec = {
                    r['LANDSAT_PRODUCT_ID']: util.ffind(r['FILE_LOC'])
                    for r in db_contrib_rec
                }
                logger.info("Contributing scene from db: %s", db_contrib_rec)
                contributing_scenes.update(db_contrib_rec)

    n_contrib_scenes = len(contributing_scenes)
    is_complete_tile = ('Y' if n_contrib_scenes == len(contrib_tile_scenes)
                        else 'N')
    logger.info("Contributing scenes: %s", contributing_scenes)
    logger.info('All scenes found to complete tile: %s', is_complete_tile)
    logger.info('Tile: %s  - Number of contributing scenes: %d',
                tile_id, n_contrib_scenes)

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
            util.untar_archive(tar_file_location, directory=directory)
        except Exception:
            logger.exception('Error staging input data for %s: %s',
                             product_id, tar_file_location)
            raise ArdSceneException

    logger.info('Starting to build tile: %s', tile_id)

    # Determine which scene(s) will overlay the other scene(s) for this tile.
    # North scenes (--row#) will always overlay South scenes (++row#).
    # The row values are characters 13 - 15 in the product ID.
    stacking = [{'LANDSAT_PRODUCT_ID': name,
                 'XML_LOC': util.ffind(conf.workdir, name, '*.xml')}
                for name in sorted(contributing_scenes,
                                   key=lambda x: x[13:])]

    util.make_dirs(os.path.join(conf.workdir, tile_id))

    producers = config.read_processing_config(sensor=segment['SATELLITE'])

    datatypes = {
        "[ TYPE: Int16 ][ RANGE: -100,16000 ][ FILL: -9999 ]":
            direct_clip,
        "[ TYPE: Int16 ][ RANGE: -2000,16000 ][ FILL: -9999 ]":
            direct_clip,
        "[ TYPE: Int16 ][ RANGE: -32767,32767 ][ FILL: -32768 ]":
            direct_clip,
        "[ TYPE: Int16 ][ RANGE: ??? ][ FILL: -9999 ]":
            direct_clip,
        "[ TYPE: UInt16 ][ RANGE: 0,65535 ][ FILL: 1 ]":
            direct_clip,
        "[ TYPE: UInt8 ][ RANGE: 0,255 ][ FILL: 1 ]":
            direct_clip,
        "[ TYPE: UInt8 ][ RANGE: 0,255 ][ FILL: 255 ]":
            direct_clip,
        "[ TYPE: UInt8 ][ RANGE: 0,255 ][ FILL: NA ][ +LINEAGE ]":
            fill_zero_na_lineage,
        "[ TYPE: Int16 ][ RANGE: 0,255 ][ FILL: -9999 ][ +LINEAGE ]":
            calc_nodata_9999_uint_lineage,
        "[ TYPE: Int16 ][ RANGE: ??? ][ FILL: -9999 ][ +LINEAGE ]":
            calc_nodata_9999_lineage,
    }

    outputs = dict()
    for product_request in sorted(conf.products, reverse=True):
        logging.info('Create product %s', product_request)
        required_bands = config.determine_output_products(producers,
                                                          product_request)
        for band_name, rename in required_bands.items():
            dtype = config.datatype_searches(producers, band_name)
            logger.info('Requires base band_name %s (Type %s)',
                        band_name, dtype)

            # Process the current dataset type
            filename = datatypes[dtype](stacking, band_name, clip_extents,
                                        tile_id, rename, conf.workdir)
            outputs[band_name] = filename

            # WARNING: Assume LINEAGE will always be present!
            if band_name == 'toa_band1':
                outputs['LINEAGEQA'] = (
                    process_lineage(stacking, band_name, clip_extents,
                                    tile_id, 'LINEAGEQA', conf.workdir)
                )

    lng_count = process_lineage_contributing(outputs['LINEAGEQA'],
                                             n_contrib_scenes)

    outputs['XML'] = (
        process_metadata(segment, stacking, tile_id, clip_extents, region,
                         lng_count, production_timestamp, producers,
                         conf.workdir)
    )

    process_output(conf.products, producers, outputs, tile_id, output_path)
    util.process_checksums(globext=os.path.join(output_path,
                                                tile_id+'*.tar'))
    if process_browse(producers['browse'], conf.workdir, tile_id,
                      output_path) != 0:
        logger.error('Failed to produce the browse image.')
        return "ERROR"

    if not conf.debug:
        # No errors making this tile, record it in our database
        completed_tile_list = [
            [tile_id, ",".join(contributing_scenes.keys()),
             is_complete_tile, "SUCCESS"]
        ]
        db.insert_tile_record(db.connect(conf.connstr), completed_tile_list)
    return "SUCCESS"


def direct_clip(stacking, band_name, clip_extents, tile_id, rename, workdir):
    """Clip datatypes which require no special processing."""
    logger.info('     Start processing for band: %s', band_name)
    mosaic_filename = os.path.join(workdir, tile_id,
                                   tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    warp_cmd = (
        'gdalwarp -te {extents}'
        ' -co "compress=deflate" -co "zlevel=9"'
        ' -co "tiled=yes" -co "predictor=2"'
    ).format(extents=clip_extents)

    for stack in reversed(stacking):
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'],
                                '*' + band_name + '.tif')
        warp_cmd += ' ' + scene_name
    warp_cmd += ' ' + mosaic_filename
    util.execute_cmd(warp_cmd)

    logger.info('    End processing for %s as %s ', band_name, mosaic_filename)
    if not os.path.exists(mosaic_filename):
        logger.error('Processing failed to generate desired output: %s',
                     mosaic_filename)
    return mosaic_filename


def process_lineage(stacking, band_name, clip_extents,
                    tile_id, rename, workdir):
    """Create the lineage file."""
    logger.info('     Start processing for band: %s', rename)

    lineage_filename = os.path.join(workdir, tile_id,
                                    tile_id + '_' + rename + '.tif')

    if os.path.exists(lineage_filename):
        logger.warning("Skip previously generated result %s", lineage_filename)
        return lineage_filename

    temp_names = list()
    for level, stack in reversed(list(enumerate(stacking, start=1))):
        temp_name = lineage_filename.replace('.tif',
                                             '_srcTemp%d' % level + '.tif')
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'],
                                '*' + band_name + '.tif')

        calc_cmd = (
            'gdal_calc.py -A {scene} --outfile {temp}'
            ' --calc=" {level} * (A > -101)" --type="Byte" --NoDataValue=0'
        )
        util.execute_cmd(calc_cmd.format(
            level=level, temp=temp_name, scene=scene_name))
        temp_names.append(temp_name)

    warp_cmd = (
        'gdalwarp -te {extents} -dstnodata "0" -srcnodata "0"'
        ' -ot "Byte" -wt "Byte"'
        ' -co "compress=deflate" -co "zlevel=9"'
        ' -co "tiled=yes" -co "predictor=2" '
    ).format(extents=clip_extents)
    warp_cmd += ' '.join(temp_names)
    warp_cmd += ' ' + lineage_filename
    util.execute_cmd(warp_cmd)
    util.remove(*temp_names)

    logger.info('    End processing for %s as %s ',
                band_name, lineage_filename)
    if not os.path.exists(lineage_filename):
        logger.error('Processing failed to generate desired output: %s',
                     lineage_filename)
    return lineage_filename


def process_lineage_contributing(lineage_filename, n_contrib_scenes):
    """Check historgram for count of scenes which were not all-fill."""
    logger.info('    Start checking contributing scenes')

    info_cmd = 'gdalinfo -hist {}'
    results = util.execute_cmd(info_cmd.format(lineage_filename))
    # TODO: could potentially use this instead...
    util.remove(lineage_filename + '.aux.xml')
    count, array = geofuncs.parse_gdal_hist_output(results['output'])

    logger.info('Parsing histogram from lineage file found %d'
                ' contributing scenes', count)
    if count == 0:
        logger.warning('Found all fill lineage, tile not needed!')
        raise ArdTileNotNeededException()

    # decrement pixel values in lineage file if some scenes didn't contribute
    # any pixels
    if count != n_contrib_scenes:
        delta = n_contrib_scenes - count

        # Determine whether we need decrement the pixel
        # values in the lineage file or not.
        cmd = ''
        if delta == 1:
            if array[0] == 0:
                cmd = ' --calc="A-' + str(delta) + '"'
            elif array[1] == 0 and array[2] > 0:
                cmd = ' --calc="A-(A==3)"'
        elif delta == 2:
            if array[0] == 0 and array[1] == 0:
                cmd = ' --calc="A-' + str(delta) + '"'
            elif array[0] == 0 and array[2] == 0:
                cmd = ' --calc="A-' + str(1) + '"'

        if cmd != '':
            temp_name = lineage_filename.replace('.tif', '_linTemp.tif')
            calc_cmd = (
                'gdal_calc.py -A {lineage} --outfile {temp} {calc}'
                ' --type="Byte" --NoDataValue=0 --overwrite'
            )
            util.execute_cmd(calc_cmd.format(
                lineage=lineage_filename, temp=temp_name, calc=cmd))

            # compress
            warp_cmd = (
                'gdalwarp -co "compress=deflate" -co "zlevel=9"'
                ' -co "tiled=yes" -co "predictor=2"'
                ' -overwrite {} {}'
            )
            util.execute_cmd(warp_cmd.format(temp_name, lineage_filename))
            util.remove(temp_name)

    logger.info('finish updating contributing scenes')
    return count


def fill_zero_na_lineage(stacking, band_name, clip_extents,
                         tile_id, rename, workdir):
    """Clip scenes which need Lineage to determine NoData fill regions."""
    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename = os.path.join(workdir, tile_id,
                                   tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    temp_clipped_names = list()
    temp_masked_names = list()
    for level, stack in reversed(list(enumerate(stacking, start=1))):
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'],
                                '*' + band_name + '.tif')
        lineg_name = util.ffind(workdir, tile_id, '*LINEAGEQA.tif')

        temp_name1 = mosaic_filename.replace('.tif',
                                             '_temp%d' % level + '.tif')
        temp_warp_cmd = (
            'gdalwarp -te {extents} -dstnodata "0"'
            ' -ot "Byte" -wt "Byte" {0} {1}'
        )
        util.execute_cmd(temp_warp_cmd.format(
            scene_name, temp_name1, extents=clip_extents))
        temp_clipped_names.append(temp_name1)

        temp_name2 = mosaic_filename.replace('.tif',
                                             '_temp%dM' % level + '.tif')
        temp_calc_cmd = (
            'gdal_calc.py -A {0} -B {lineage} --outfile {1}'
            ' --calc="A*(B=={level})" '
            '--type="Byte" --NoDataValue=0'
        )
        util.execute_cmd(temp_calc_cmd.format(
            temp_name1, temp_name2, lineage=lineg_name, level=level))
        temp_masked_names.append(temp_name2)

    temp_name = mosaic_filename.replace('.tif', '_temp.tif')
    temp_warp_cmd = 'gdalwarp {} {}'.format(' '.join(temp_masked_names),
                                            temp_name)
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
        logger.error('Processing failed to generate desired output: %s',
                     mosaic_filename)
    return mosaic_filename


def calc_nodata_9999_uint_lineage(stacking, band_name, clip_extents,
                                  tile_id, rename, workdir):
    """Clip scenes which do not have NoData, apply -9999 where no LINEAGE."""
    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename = os.path.join(workdir, tile_id,
                                   tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    temp_clipped_names = list()
    temp_masked_names = list()
    for level, stack in reversed(list(enumerate(stacking, start=1))):
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'],
                                '*' + band_name + '.tif')

        temp_name1 = mosaic_filename.replace('.tif',
                                             '_temp%d' % level + '.tif')
        temp_warp_cmd = (
            'gdalwarp -te {extents} -ot "Int16"'
            ' -dstnodata "-9999" -srcnodata "None"  {0} {1}'
        )
        util.execute_cmd(temp_warp_cmd.format(
            scene_name, temp_name1, extents=clip_extents))
        temp_clipped_names.append(temp_name1)

        lineg_name = util.ffind(workdir, tile_id, '*LINEAGEQA.tif')
        temp_name2 = mosaic_filename.replace('.tif',
                                             '_temp%dM' % level + '.tif')
        temp_calc_cmd = (
            'gdal_calc.py -A {0} -B {lineage} --outfile {1}'
            ' --calc="(A*(B=={level}) + (-9999*(B!={level})))"'
            ' --NoDataValue=-9999'
        )
        util.execute_cmd(temp_calc_cmd.format(
            temp_name1, temp_name2, lineage=lineg_name, level=level))
        temp_masked_names.append(temp_name2)

    temp_name = mosaic_filename.replace('.tif', '_temp.tif')
    temp_warp_cmd = 'gdalwarp {} {}'.format(' '.join(temp_masked_names),
                                            temp_name)
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
        logger.error('Processing failed to generate desired output: %s',
                     mosaic_filename)
    return mosaic_filename


def calc_nodata_9999_lineage(stacking, band_name, clip_extents,
                             tile_id, rename, workdir):
    """Clip scenes which have data outside the lineage, apply -9999 fill."""
    logger.info('     Start processing for band: %s', band_name)

    mosaic_filename = os.path.join(workdir, tile_id,
                                   tile_id + '_' + rename + '.tif')

    if os.path.exists(mosaic_filename):
        logger.warning("Skip previously generated result %s", mosaic_filename)
        return mosaic_filename

    temp_clipped_names = list()
    temp_masked_names = list()
    for level, stack in reversed(list(enumerate(stacking, start=1))):
        scene_name = util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'],
                                '*' + band_name + '.tif')

        temp_name1 = mosaic_filename.replace('.tif',
                                             '_temp%d' % level + '.tif')
        temp_warp_cmd = (
            'gdalwarp -te {extents}'
            ' -dstnodata "-9999" -srcnodata "-9999" {0} {1}'
        )
        util.execute_cmd(temp_warp_cmd.format(
            scene_name, temp_name1, extents=clip_extents))
        temp_clipped_names.append(temp_name1)

        lineg_name = util.ffind(workdir, tile_id, '*LINEAGEQA.tif')
        temp_name2 = mosaic_filename.replace('.tif',
                                             '_temp%dM' % level + '.tif')
        temp_calc_cmd = (
            'gdal_calc.py -A {0} -B {lineage} --outfile {1}'
            ' --calc="(A*(B=={level}) + (-9999*(B!={level})))"'
            ' --NoDataValue=-9999'
        )
        util.execute_cmd(temp_calc_cmd.format(
            temp_name1, temp_name2, lineage=lineg_name, level=level))
        temp_masked_names.append(temp_name2)

    temp_name = mosaic_filename.replace('.tif', '_temp.tif')
    temp_warp_cmd = 'gdalwarp {} {}'.format(' '.join(temp_masked_names),
                                            temp_name)
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
        logger.error('Processing failed to generate desired output: %s',
                     mosaic_filename)
    return mosaic_filename


def process_metadata(segment, stacking, tile_id, clip_extents, region,
                     lng_count, production_timestamp, producers,
                     workdir):
    """Create the tile metadata file, Generate statistics we will need."""
    logger.info('     Start processing for metadata')

    pqa_name = util.ffind(workdir, tile_id, '*PIXELQA.tif')
    bit_counts = geofuncs.raster_value_count(pqa_name, tile_id)

    metadata_locs = list()
    for stack in stacking:
        metadata_locs.append({
            'L2XML': util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'],
                                "*.xml"),
            'L1MTL': util.ffind(workdir, stack['LANDSAT_PRODUCT_ID'],
                                '*_MTL.txt'),
        })

    if ({'ARD', 'L3'} != set(producers['xml'].keys())
            or ['SW'] != producers['xml']['L3']):
        raise NotImplementedError()

    filenames = dict()
    for xml_group, products in producers['xml'].items():

        xml_id = str(tile_id)
        if len(products) == 1:
            xml_id = tile_id + '_' + products[-1]

        logger.info('     Start processing for metadata group: %s', xml_group)
        metadata_filename = os.path.join(workdir, tile_id, xml_id + '.xml')

        if os.path.exists(metadata_filename):
            logger.warning("Skip previously generated result %s",
                           metadata_filename)
            continue

        group_filenames = {
            k: v for p in products for k, v in
            config.determine_output_products(producers, p).items()
        }
        buildMetadata(metadata_filename, bit_counts, clip_extents, tile_id,
                      metadata_locs, production_timestamp, group_filenames,
                      segment, region, lng_count)

        util.make_file_group_writeable(metadata_filename)
        logger.info('    End processing for metadata as %s',
                    metadata_filename)
        if not os.path.exists(metadata_filename):
            logger.error('Processing failed to generate desired output: %s',
                         metadata_filename)
        filenames.update({p: metadata_filename for p in products})
    return filenames


def process_output(products, producers, outputs, tile_id, output_path):
    """Combine the Landsat mosaics into the .tar files."""
    util.make_dirs(output_path)

    for product_request in sorted(products, reverse=True):
        output_archive = os.path.join(output_path,
                                      tile_id + '_' + product_request + '.tar')
        logging.info('Create product %s', output_archive)
        required = producers['package'][product_request]
        included = [o for o in outputs.values()
                    if any([r in o for r in required]) and isinstance(o, str)]
        included.append(outputs['XML'][product_request])
        util.tar_archive(output_archive, included)
        output_xml = os.path.join(output_path, os.path.basename
                                  (outputs['XML'][product_request]))
        util.shutil.copyfile(outputs['XML'][product_request], output_xml)
    logger.info('    End zipping')


def process_browse(bands, workdir, tile_id, outpath):
    """Create a pyramid-layered RBG browse file for EE."""
    logger.info('     Start processing for BROWSE')

    browse_filename = os.path.join(outpath, tile_id + '.tif')

    if os.path.exists(browse_filename):
        logger.warning("Skip previously generated result %s", browse_filename)
        return browse_filename

    bands = {k: util.ffind(workdir, tile_id, tile_id + '_' + v + '.tif')
             for k, v in bands.items()}

    # create RGB image
    temp_filename1 =  os.path.join(workdir, tile_id + '_brw1.tif')
    merge_cmd = 'gdal_merge.py -o {outfile} -separate {red} {green} {blue}'
    results = util.execute_cmd(merge_cmd.format(outfile=temp_filename1,
                                                **bands))
    if results['status'] != 0:
        return results['status']

    # scale the pixel values
    temp_filename2 = os.path.join(workdir, tile_id + '_brw2.tif')
    scale_cmd = 'gdal_translate -scale 0 10000 -ot Byte {} {}'
    results = util.execute_cmd(scale_cmd.format(temp_filename1,
                                                temp_filename2))
    if results['status'] != 0:
        return results['status']

    # apply compression
    comp_cmd = 'gdal_translate -co COMPRESS=JPEG -co PHOTOMETRIC=YCBCR {} {}'
    results = util.execute_cmd(comp_cmd.format(temp_filename2,
                                               browse_filename))
    if results['status'] != 0:
        # The browse generation failed on the HSM.
        # Wait a short period and try again.
        logger.warning('gdal_translate failed to create the browse.  '
                       'Trying again.')
        time.sleep(10)
        results = util.execute_cmd(comp_cmd.format(temp_filename2,
                                               browse_filename))
        if results['status'] != 0:
            return results['status']

    # internal pyramids
    addo_cmd = 'gdaladdo {} 2 4 8 16'
    results = util.execute_cmd(addo_cmd.format(browse_filename))
    if results['status'] != 0:
        # The pyramid generation failed on the HSM.
        # Wait a short period and try again.
        logger.warning('gdaladdo failed to create the pyramids.  '
                       'Trying again.')
        time.sleep(10)
        results = util.execute_cmd(addo_cmd.format(browse_filename))
        if results['status'] != 0:
            return results['status']

    util.remove(temp_filename1, temp_filename2, browse_filename + '.aux.xml')

    logger.info('    End building browse.')
    if not os.path.exists(browse_filename):
        logger.error('Processing failed to generate desired output: %s',
                     browse_filename)
    return 0


class ArdTileNotNeededException(Exception):
    """Lineage found All-Fill inside sensor field of view."""

    pass


class ArdTileException(Exception):
    """An unrecoverable error has occurred while processing a tile."""

    pass


class ArdSceneException(Exception):
    """An error has occurred while working with scene data."""

    pass


def process_segment(segment, output_path, conf):
    """Produce all necessary products for a segment of acquisitions."""
    scene_state = "INWORK"
    # update PROCESSING_STATE in ARD_PROCESSED_SCENES to 'INWORK'
    db.update_scene_state(db.connect(conf.connstr),
                          segment['LANDSAT_PRODUCT_ID'], scene_state)

    logger.info("Scene %s is %s.", segment['LANDSAT_PRODUCT_ID'], scene_state)
    id_parts = landsat.match_dt(segment['LANDSAT_PRODUCT_ID'])

    pathrow = id_parts['wrspath'] + id_parts['wrsrow']
    if pathrow in pathrow2regionLU.keys():
        region = pathrow2regionLU[pathrow]
    else:
        scene_state = "NOGRID"
        db.update_scene_state(db.connect(conf.connstr),
                              segment['LANDSAT_PRODUCT_ID'], scene_state)
        return scene_state

    # Intersect scene with tile grid to find all touching tiles, and
    # consecutive scenes needed to produce them
    hv_tiles, tile_scenes = (
        geofuncs.get_tile_scene_intersections(db.connect(conf.connstr),
                                              segment['LANDSAT_PRODUCT_ID'],
                                              region)
    )

    logger.info('Number of tiles to create: %d', len(hv_tiles))
    if not hv_tiles:
        logger.error('No scene coordinates found for %s',
                     segment['LANDSAT_PRODUCT_ID'])

        scene_state = "ERROR"
        # update PROCESSING_STATE in ARD_PROCESSED_SCENES
        db.update_scene_state(db.connect(conf.connstr),
                              segment['LANDSAT_PRODUCT_ID'], scene_state)
        return scene_state

    tiling_error_encountered = 0
    for current_tile in hv_tiles:
        try:
            tile_id = landsat.generate_tile_id(segment['LANDSAT_PRODUCT_ID'],
                                               current_tile, region,
                                               conf.collection, conf.version)
            tile_state = process_tile(current_tile, tile_id, segment, region,
                                      tile_scenes, output_path, conf)
            if tile_state == 'ERROR':
                tiling_error_encountered = 1
        except ArdTileNotNeededException:
            logger.warning('Lineage file found 0 contributing scenes,'
                           ' set to NOT NEEDED')
        except ArdTileException:
            logger.exception('Error caught while processing tile %s!',
                             current_tile)
        except ArdSceneException:
            logger.exception('Error caught while processing scene %s!',
                             segment['LANDSAT_PRODUCT_ID'])
            tiling_error_encountered = 1
        except Exception:
            logger.exception('Unexpected error processing tile %s!',
                             current_tile)
            tiling_error_encountered = 1

        # Remove the temporary work directory,
        # but keep adjacent scenes for other tiles
        if not conf.debug:
            logger.info('    Cleanup: Removing temp directory: %s ...',
                        os.path.join(conf.workdir, tile_id))
            util.remove(os.path.join(conf.workdir, tile_id))

    # If no tiling errors were encountered that may call for a retry,
    # we're done with this scene.  Otherwise, mark the scene for a
    # retry so that failed tiles can be reattempted.
    if not tiling_error_encountered:
        scene_state = "COMPLETE"
    else:
        scene_state = "ERROR"

    # update PROCESSING_STATE in ARD_PROCESSED_SCENES
    db.update_scene_state(db.connect(conf.connstr),
                          segment['LANDSAT_PRODUCT_ID'], scene_state)

    return scene_state


def process_segments(segments, output_path, conf):
    """Clips tiles from a list of contiguous scenes aka segment."""
    for i, segment in enumerate(segments):
        # Cleanup unneeded scene directories to save space
        # Results of this check should keep no more than 3 scenes
        # in work directory at a time
        if (i - 2) > -1:
            previous_scene = segments[i - 2]['LANDSAT_PRODUCT_ID']
            util.remove(previous_scene)

        scene_state = process_segment(segment, output_path, conf)
        logger.info("Scene %s is %s.",
                    segment['LANDSAT_PRODUCT_ID'], scene_state)
        logger.info('Segment loop: %d', i)

    # cleanup any remaining scene directories
    for segment in segments:
        previous_scene = segment['LANDSAT_PRODUCT_ID']
        if not conf.debug:
            util.remove(previous_scene)
