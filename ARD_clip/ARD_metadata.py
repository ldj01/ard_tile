"""Generate the ARD Metadata file."""
# ==========================================================================
#
#     The finished metadata file will follow this format:
#         <ard_metadata>
#                   <tile_metadata>
#                             <global_metadata />
#                             <bands>
#                                       <band>
#                                       ...
#                             </bands>
#                   </tile_metadata>
#                   <scene_metadata>
#                             <index />
#                             <global_metadata/>
#                             <bands>
#                                       <band/>
#                                        ...
#                             </bands>
#                    </scene_metadata>
#         </ard_metadata>
#
#     Start with one of the Level2 metadata files and add/remove to it
#          to build the section that describes the new tile.  After that
#          section will be one section per contributing scene.
#     We will include statistics already generated from a histogram of
#           the pixel_qa file and stored in the incoming statsTuple.
#
# ==========================================================================
import os
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom

from espa import Metadata

from util import logger
import util
import geofuncs
import landsat


def buildMetadata(metadata_filename, bit_counts, clip_extents, tile_id,
                  metadata_locs, production_timestamp, tiled_filenames,
                  segment, region, lng_count):
    """Build tile metadata starting from the source L2 metadata files."""
    logger.debug('Buildmetadata: Entered')

    SceneTagsToBeKept = ('data_provider', 'satellite', 'instrument',
                         'acquisition_date', 'scene_center_time',
                         'level1_production_date', 'wrs', 'product_id',
                         'lpgs_metadata_file')

    L1Tuple = []
    L2Tuple = []
    for i, metafilenames in enumerate(metadata_locs):
        if i < lng_count:
            l2tree = ET.parse(metafilenames['L2XML'])
            l2tree = l2tree.getroot()
            L2Tuple.append(l2tree)
            L1Tuple.append(landsat.read_metadatas(metafilenames['L1MTL']))

    # read any namespace from the 1st L2 scene
    namespace = ''
    if ('{' in l2tree.tag):
        endPos = (l2tree.tag).find('}')
        namespace = l2tree.tag[:endPos+1]

    # Start the output xml
    outRoot = ET.Element('ard_metadata')
    outTileMetadata = ET.SubElement(outRoot, 'tile_metadata')
    outTileGlobal = ET.SubElement(outTileMetadata, 'global_metadata')

    # data_provider - use L2 scene
    for child in l2tree.find(namespace + 'global_metadata'):
        if (child.tag == namespace + 'data_provider'):
            outDataProvider = ET.SubElement(outTileGlobal, 'data_provider',
                                            child.attrib)
            outDataProvider.text = child.text

    # satellite - new
    gm_satellite = ET.SubElement(outTileGlobal, 'satellite')
    satellite_strs = {
        'LT04': 'LANDSAT_4',
        'LT05': 'LANDSAT_5',
        'LE07': 'LANDSAT_7',
        'LC08': 'LANDSAT_8',
    }
    gm_satellite.text = satellite_strs[tile_id[:4]]

    # instrument - new
    gm_instrument = ET.SubElement(outTileGlobal, 'instrument')
    instrument_strs = {
        'LT04': 'TM',
        'LT05': 'TM',
        'LE07': 'ETM',
        'LC08': 'OLI/TIRS_Combined',
    }
    gm_instrument.text = instrument_strs[tile_id[:4]]

    # Level 1 Collection - new
    gm_l1coll = ET.SubElement(outTileGlobal, 'level1_collection')
    gm_l1coll.text = tile_id[34:36]

    # ARD Version - new
    gm_ardVersion = ET.SubElement(outTileGlobal, 'ard_version')
    gm_ardVersion.text = tile_id[38:40]

    # Region - new
    gm_region = ET.SubElement(outTileGlobal, 'region')
    gm_region.text = tile_id[5:7]

    # acquisition date - use L2 scene
    for child in l2tree.find(namespace + 'global_metadata'):
        if (child.tag == namespace + 'acquisition_date'):
            outAcqDate = ET.SubElement(outTileGlobal, 'acquisition_date',
                                       child.attrib)
            outAcqDate.text = child.text

    # tile_id - new
    gm_productid = ET.SubElement(outTileGlobal, 'product_id')
    gm_productid.text = tile_id

    # tile_production_date - new
    gm_tilepd = ET.SubElement(outTileGlobal, 'production_date')
    gm_tilepd.text = production_timestamp

    # bounding coordinates - modify L2 scene
    logger.debug('Buildmetadata: Ready for bounding_coordinates')
    horiz = tile_id[8:11]
    vertical = tile_id[11:14]
    newBoundingCoordsStr = getGeographicBoundingCoordinates(horiz,
                                                            vertical, region)
    tempBoundingElement = ET.fromstring(newBoundingCoordsStr)

    gm_bounding = ET.SubElement(outTileGlobal, tempBoundingElement.tag,
                                tempBoundingElement.attrib)

    for child in tempBoundingElement:
        gm_bounding_child = ET.SubElement(gm_bounding, child.tag, child.attrib)
        gm_bounding_child.text = child.text

    # projection information - modify L2 scene
    logger.debug('Buildmetadata: Ready for projection information')
    newProjInfo = global_createProjInfo(clip_extents, region)
    tempProjElement = ET.fromstring(newProjInfo)

    gm_ProjInfo = ET.SubElement(outTileGlobal, tempProjElement.tag,
                                tempProjElement.attrib)

    for child in tempProjElement:
        gm_proj_child = ET.SubElement(gm_ProjInfo, child.tag, child.attrib)
        gm_proj_child.text = child.text
        if (child.tag == "albers_proj_params"):
            for projChild in child:
                gm_proj_grandchild = ET.SubElement(gm_proj_child,
                                                   projChild.tag,
                                                   projChild.attrib)
                gm_proj_grandchild.text = projChild.text

    # orientation_angle - use L2 scene
    for child in l2tree.find(namespace + 'global_metadata'):
        if (child.tag == namespace + 'orientation_angle'):
            outOrientation = ET.SubElement(outTileGlobal, 'orientation_angle',
                                           child.attrib)
            outOrientation.text = child.text

    # tile_grid - new
    gm_tileid = ET.SubElement(outTileGlobal, 'tile_grid')
    gm_tileid.set('v', tile_id[11:14])
    gm_tileid.set('h', tile_id[8:11])

    # scene_count - new
    gm_sc = ET.SubElement(outTileGlobal, 'scene_count')
    gm_sc.text = str(lng_count)

    qa_percents = createPixelTypeTuple(bit_counts)

    # cloud_cover - new
    gm_cc = ET.SubElement(outTileGlobal, 'cloud_cover')
    gm_cc.text = qa_percents['cloud_cover']

    # cloud_shadow - new
    gm_cs = ET.SubElement(outTileGlobal, 'cloud_shadow')
    gm_cs.text = qa_percents['cloud_shadow']

    # snow_ice - new
    gm_si = ET.SubElement(outTileGlobal, 'snow_ice')
    gm_si.text = qa_percents['snow_ice']

    # fill - new
    gm_fill = ET.SubElement(outTileGlobal, 'fill')
    gm_fill.text = qa_percents['fill']

    #
    # Build all of the bands for the tile
    #
    # This group of tags originate from
    # a Level 2 metadata file.  This section will
    # describe the tile bands - most of the
    # information is already correct, but
    # anything tile related will have to be changed.
    #
    outTileBands = ET.SubElement(outTileMetadata, 'bands')

    # add lineage band
    lineageStr = createLineageSection(tile_id, production_timestamp)
    tempLineageElement = ET.fromstring(lineageStr)

    bands_lineage = ET.SubElement(outTileBands, tempLineageElement.tag,
                                  tempLineageElement.attrib)

    for child in tempLineageElement:
        bands_lineage_child = ET.SubElement(bands_lineage, child.tag,
                                            child.attrib)
        bands_lineage_child.text = child.text

    # Loop through all of the bands in the L2 file.
    # Each band will need to be modified to reflect the
    # characteristics of the tile.
    bandsElement = l2tree.find(namespace + 'bands')

    included_newnames = list()
    for curBand in bandsElement:
        oldBandStr = ET.tostring(curBand)
        newNameOnly, newBandStr = fixTileBand2(tile_id, tiled_filenames,
                                               production_timestamp,
                                               oldBandStr)

        if newBandStr is None:
            logger.debug('Skipping band not in current XML group')
            continue
        included_newnames.append(newNameOnly)

        tempBandElement = ET.fromstring(newBandStr)

        bands_band = ET.SubElement(outTileBands, tempBandElement.tag,
                                   tempBandElement.attrib)

        for child in tempBandElement:
            bands_band_child = ET.SubElement(bands_band, child.tag,
                                             child.attrib)
            bands_band_child.text = child.text
            if (child.tag in ["bitmap_description", "class_values"]):
                for bandChild in child:
                    bands_band_grandchild = ET.SubElement(bands_band_child,
                                                          bandChild.tag,
                                                          bandChild.attrib)
                    bands_band_grandchild.text = bandChild.text

    logger.debug('Buildmetadata: finished tile bands')

    #
    # "Global" and "bands" have now been created for the new tiles.
    #
    #  Next modify the scene metadata for each contributing scene.
    #  We'll have to read some values from the Level 1 (MTL.txt) file.
    #
    for i in range(lng_count):
        sceneRoot = (L2Tuple[i])

        #  Read some values from the Level 1 (MTL.txt) file.
        request_id = getL1Value(L1Tuple[i], "REQUEST_ID")
        scene_id = getL1Value(L1Tuple[i], "LANDSAT_SCENE_ID")
        elev_src = getL1Value(L1Tuple[i], "ELEVATION_SOURCE")

        if any(tile_id.startswith(x) for x in ('LT04', 'LT05', 'LE07')):
            sensor_mode = getL1Value(L1Tuple[i], "SENSOR_MODE")
            ephemeris_type = getL1Value(L1Tuple[i], "EPHEMERIS_TYPE")

        cpf_name = getL1Value(L1Tuple[i], "CPF_NAME")
        geometric_rmse_model = getL1Value(L1Tuple[i],
                                          "GEOMETRIC_RMSE_MODEL")
        geometric_rmse_model_x = getL1Value(L1Tuple[i],
                                            "GEOMETRIC_RMSE_MODEL_X")
        geometric_rmse_model_y = getL1Value(L1Tuple[i],
                                            "GEOMETRIC_RMSE_MODEL_Y")

        # opening tags for each scene
        outSceneMetadata = ET.SubElement(outRoot, 'scene_metadata')
        outSceneIndex = ET.SubElement(outSceneMetadata, 'index')
        outSceneIndex.text = str(i+1)
        outSceneGlobal = ET.SubElement(outSceneMetadata, 'global_metadata')

        # Regurgitate the L2 scene information,
        # interspursing some additional L1 info
        # along the way
        for child in sceneRoot.find(namespace + 'global_metadata'):
            newTag = (child.tag).replace('ns0:', '')
            newTag = (child.tag).replace(namespace, '')

            if (newTag in SceneTagsToBeKept):
                outGeneric = ET.SubElement(outSceneGlobal, newTag,
                                           child.attrib)
                outGeneric.text = child.text

            if (newTag == 'wrs'):
                outGeneric = ET.SubElement(outSceneGlobal, 'request_id')
                outGeneric.text = request_id
                outGeneric = ET.SubElement(outSceneGlobal, 'scene_id')
                outGeneric.text = scene_id

            if (newTag == 'product_id'):
                outGeneric = ET.SubElement(outSceneGlobal, 'elevation_source')
                outGeneric.text = elev_src

                is_landsat_4_7 = any(tile_id.startswith(x)
                                     for x in ('LT04', 'LT05', 'LE07'))
                if is_landsat_4_7:
                    outGeneric = ET.SubElement(outSceneGlobal, 'sensor_mode')
                    outGeneric.text = sensor_mode
                    outGeneric = ET.SubElement(outSceneGlobal,
                                               'ephemeris_type')
                    outGeneric.text = ephemeris_type

                outGeneric = ET.SubElement(outSceneGlobal, 'cpf_name')
                outGeneric.text = cpf_name

            if (newTag == 'lpgs_metadata_file'):

                if geometric_rmse_model.find("not found") == -1:
                    outGeneric = ET.SubElement(outSceneGlobal,
                                               'geometric_rmse_model')
                    outGeneric.text = geometric_rmse_model

                if geometric_rmse_model_x.find("not found") == -1:
                    outGeneric = ET.SubElement(outSceneGlobal,
                                               'geometric_rmse_model_x')
                    outGeneric.text = geometric_rmse_model_x

                if geometric_rmse_model_y.find("not found") == -1:
                    outGeneric = ET.SubElement(outSceneGlobal,
                                               'geometric_rmse_model_y')
                    outGeneric.text = geometric_rmse_model_y

        outSceneBands = ET.SubElement(outSceneMetadata, 'bands')

        # The scene bands
        for bandTag in sceneRoot.find(namespace + 'bands'):
            if bandTag.attrib.get('name') not in included_newnames:
                logger.debug('Skipping band not in current XML group')
                continue
            newTag = (bandTag.tag).replace(namespace, '')
            bandElement = ET.SubElement(outSceneBands, newTag, bandTag.attrib)
            bandElement.text = bandTag.text

            for child in bandTag:
                newTag2 = (child.tag).replace(namespace, '')
                childElement = ET.SubElement(bandElement, newTag2,
                                             child.attrib)
                childElement.text = child.text
                if (newTag2 in ["bitmap_description", "class_values"]):
                    for bitmapChild in child:
                        bitmapTag = (bitmapChild.tag).replace(namespace, '')
                        bands_band_bitmap = ET.SubElement(childElement,
                                                          bitmapTag,
                                                          bitmapChild.attrib)
                        bands_band_bitmap.text = bitmapChild.text

    logger.debug('Buildmetadata: Ready to write')

    namespace1Prefix = "xmlns"
    namespace2Prefix = "xmlns:xsi"
    namespace3Prefix = "xsi:schemaLocation"

    # TODO: these should come from the XSD
    namespace1URI = "https://landsat.usgs.gov/ard/v1"
    namespace2URI = "http://www.w3.org/2001/XMLSchema-instance"
    namespace3URI = ("https://landsat.usgs.gov/ard/v1 "
                     "https://landsat.usgs.gov/ard/ard_metadata_v1_1.xsd")

    outRoot.attrib[namespace3Prefix] = namespace3URI
    outRoot.attrib[namespace2Prefix] = namespace2URI
    outRoot.attrib[namespace1Prefix] = namespace1URI
    outRoot.attrib["version"] = "1.1"

    # Add string indentation - Unfortunately,
    # this function produces extra carriage returns
    # after some elements...

    prettyString = (
        minidom.parseString(ET.tostring(outRoot)
                            ).toprettyxml(encoding="utf-8", indent="    ")
    )

    # Write to temp file
    uglyFullName = metadata_filename.replace(".xml", "_ugly.xml")
    with open(uglyFullName, "w") as f:
        f.write(prettyString.encode('utf-8'))

    # Looks like the minidom pretty print added some
    # blank lines followed by CRLF.  The blank lines are
    # of more than one length in our case.  Remove any
    # blank lines.
    inMetafile = open(uglyFullName, "r")
    outMetafile = open(metadata_filename, "w")

    for curLine in inMetafile:

        allSpaces = True
        for curChar in curLine:
            if ((curChar != '\x20')
                    and (curChar != '\x0D')
                    and (curChar != '\x0A')):
                allSpaces = False
                continue

        if allSpaces is False:
            outMetafile.write(curLine)
        # else:
        #    print 'Found blank line'

    inMetafile.close()
    outMetafile.close()

    # Validate metafile that was just created
    tile_metadata = Metadata(xml_filename=metadata_filename)
    tile_metadata.validate()


def getL1Value(L1String, key):
    """Read a value from the level 1 metadata string."""
    startPos = L1String.find(key)
    if startPos != -1:
        startPos = L1String.find('=', startPos)
        endPos = L1String.find('\n', startPos)
        rawValue = L1String[startPos+2:endPos]
        return rawValue.replace('"', '')
    else:
        return key + " not found"


def createPixelTypeTuple(longsTuple):
    """Calculate percentage of pixels, based on 5000x5000=25000000px."""
    retval = dict()
    # Calculate fill
    fillLong = longsTuple['fill'] / 25000000.0 * 100.0
    retval['fill'] = '{:0.4f}'.format(fillLong)

    numNonFillPixels = 25000000.0 - fillLong

    # Calculate Cloud Cover
    cloudLong = longsTuple['cloud_cover'] / numNonFillPixels * 100.0
    retval['cloud_cover'] = '{:0.4f}'.format(cloudLong)

    # Calculate Cloud Shadow
    shadowLong = longsTuple['cloud_shadow'] / numNonFillPixels * 100.0
    retval['cloud_shadow'] = '{:0.4f}'.format(shadowLong)

    # Calculate Snow/Ice
    snowLong = longsTuple['snow_ice'] / numNonFillPixels * 100.0
    retval['snow_ice'] = '{:0.4f}'.format(snowLong)

    return retval


def createLineageSection(tileID, prodDate):
    """Create the lineage band metadata block."""
    lineageText = (
        '<band fill_value="0" nsamps="5000" nlines="5000" data_type="UINT8" '
        'category="metadata" name="LINEAGEQA" product="scene_index" '
        'source="level2">'
        '<short_name>TILEIDX</short_name>'
        '<long_name>index</long_name>'
        '<file_name>' + tileID + '_LINEAGEQA.tif</file_name>'
        '<pixel_size units="meters" y="30" x="30"/>'
        '<resample_method>none</resample_method>'
        '<data_units>index</data_units>'
        '<valid_range max="255.000000" min="0.000000"/>'
        '<production_date>' + prodDate + '</production_date>'
        '</band>'
    )
    return lineageText


def fixTileBand2(tileID, filenames, productionDateTime, bandTag):
    """Alter the L2 metadata to create describes the tile bands."""
    # remove namespace info
    bandTag = bandTag.replace('ns0:', '')
    nsBig = 'xmlns:ns0="http://espa.cr.usgs.gov/v2"'
    bandTag = bandTag.replace(nsBig, '')

    # find nsamps within the <band> tag
    sampsBeginPos = bandTag.find("nsamps")
    sampsEndPos = bandTag.find('"', sampsBeginPos + 8)
    oldSamps = bandTag[sampsBeginPos:sampsEndPos+1]
    newSamps = 'nsamps="5000"'

    # find nlines within the <band> tag
    linesBeginPos = bandTag.find("nlines")
    linesEndPos = bandTag.find('"', linesBeginPos + 8)
    oldLines = bandTag[linesBeginPos:linesEndPos+1]
    newLines = 'nlines="5000"'

    # L2 band names need to be renamed in two places -
    # 1. in the band tag
    nameBeginPos = bandTag.find("name")
    nameEndPos = bandTag.find('"', nameBeginPos + 7)
    oldBandName = bandTag[nameBeginPos:nameEndPos+1]
    nameOnly = bandTag[nameBeginPos+6:nameEndPos]
    if nameOnly not in filenames:
        logger.warning('Skipping band %s as not part of this XML group',
                       nameOnly)
        return nameOnly, None

    newName = filenames[nameOnly].split('_')[-1].split('.')[0]
    newName = 'name="%s"' % newName

    if nameOnly == 'hillshade':  # FIXME: this done based on time constraints
        # FIXME: modify band attribute="UINT8" to int16
        bandTag = bandTag.replace('data_type="UINT8"', 'data_type="INT16"')
        # FIXME: modify fill_value="255" to -9999
        bandTag = bandTag.replace('fill_value="255"', 'fill_value="-9999"')

    # 2. in the file_name tag
    startFilePos = bandTag.find('<file_name>', 0)
    endFilePos = bandTag.find('</file_name>', startFilePos)
    oldFileText = bandTag[startFilePos+11:endFilePos]
    newFileText = os.path.basename(filenames[nameOnly])

    # Modify <production_date>
    startDatePos = bandTag.find('<production_date>', 0)
    endDatePos = bandTag.find('</production_date>', startDatePos)
    oldDateText = bandTag[startDatePos:endDatePos+18]
    newDateText = (
        '<production_date>' + productionDateTime + '</production_date>'
    )

    # perform the substitutions in the <band> tag
    bandTag = bandTag.replace(oldSamps, newSamps)
    bandTag = bandTag.replace(oldLines, newLines)
    bandTag = bandTag.replace(oldBandName, newName)

    # perform the substitutions to other tags
    bandTag = bandTag.replace(oldFileText, newFileText)
    bandTag = bandTag.replace(oldDateText, newDateText)

    return nameOnly, bandTag


def getGeographicBoundingCoordinates(horiz, vertical, region):
    """Create a string containing the WGS84 geographic coordinates."""
    datasource = geofuncs.read_shapefile(region=region)
    layer = datasource.GetLayer()
    query = 'H=' + str(int(horiz)) + ' and V=' + str(int(vertical))
    layer.SetAttributeFilter(query)
    for feature in layer:
        latN = feature.GetField("LAT_NORTH")
        latS = feature.GetField("LAT_SOUTH")
        lonW = feature.GetField("LON_WEST")
        lonE = feature.GetField("LON_EAST")
        newBoundingCoords = (
            '<bounding_coordinates><west>' + str(lonW) + '</west><east>' +
            str(lonE) + '</east><north>' + str(latN) + '</north><south>' +
            str(latS) + '</south></bounding_coordinates>'
        )
        logger.debug('      > meta: %s', newBoundingCoords)

        return newBoundingCoords


def global_createProjInfo(cutLimits, region):
    """Build projection information for the geographic region."""
    # cutLimits is a string separated by blanks
    blank1Pos = cutLimits.find(' ')
    blank2Pos = cutLimits.find(' ', blank1Pos + 1)
    blank3Pos = cutLimits.find(' ', blank2Pos + 1)
    cutLeft = float(cutLimits[0:blank1Pos])
    cutBottom = float(cutLimits[blank1Pos+1:blank2Pos])
    cutRight = float(cutLimits[blank2Pos+1:blank3Pos])
    cutTop = float(cutLimits[blank3Pos+1:])

    prjStr = ''
    if (region == 'CU'):
        prjStr += '<standard_parallel1>29.500000</standard_parallel1>'
        prjStr += '<standard_parallel2>45.500000</standard_parallel2>'
        prjStr += '<central_meridian>-96.000000</central_meridian>'
        prjStr += '<origin_latitude>23.000000</origin_latitude>'
        prjStr += '<false_easting>0.000000</false_easting>'
        prjStr += '<false_northing>0.000000</false_northing>'
    elif (region == 'HI'):
        prjStr += '<standard_parallel1>8.000000</standard_parallel1>'
        prjStr += '<standard_parallel2>18.000000</standard_parallel2>'
        prjStr += '<central_meridian>-157.000000</central_meridian>'
        prjStr += '<origin_latitude>3.000000</origin_latitude>'
        prjStr += '<false_easting>0.000000</false_easting>'
        prjStr += '<false_northing>0.000000</false_northing>'
    else:
        prjStr += '<standard_parallel1>55.000000</standard_parallel1>'
        prjStr += '<standard_parallel2>65.000000</standard_parallel2>'
        prjStr += '<central_meridian>-154.000000</central_meridian>'
        prjStr += '<origin_latitude>50.000000</origin_latitude>'
        prjStr += '<false_easting>0.000000</false_easting>'
        prjStr += '<false_northing>0.000000</false_northing>'

    # build the new tags
    blurb = (
        '<projection_information' +
        ' units="meters" datum="WGS84" projection="AEA">' +
        '<corner_point y="' + '{:0.6f}'.format(cutTop) +
        '" x="' + '{:0.6f}'.format(cutLeft) + '" location="UL"/>' +
        '<corner_point y="' + '{:0.6f}'.format(cutBottom) +
        '" x="' + '{:0.6f}'.format(cutRight) + '" location="LR"/>' +
        '<grid_origin>UL</grid_origin>' +
        '<albers_proj_params>' + prjStr + '</albers_proj_params>' +
        '</projection_information>'
    )

    return blurb
