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
#     Start with one of the Level2 metadata files and add/remove to it to build the section
#         that describes the new tile.  After that section will be one section per contributing scene.
#     We will include statistics already generated from a histogram of the pixel_qa file
#             and stored in the incoming statsTuple.
#
# ==========================================================================
from ARD_HelperFunctions import logIt, appendToLog, reportToStdout, getARDName
from osgeo import osr,ogr
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import traceback


# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Create a metadata.xml file
#
#   Build a new tile metadata file by stealing a lot of the information from the 
#   source L2 metadata files.
#
def buildMetadata2(debug, logger, statsTuple, cutLimits, tileID, \
                                L2Scene01MetaFileName, L1Scene01MetaString, \
                                L2Scene02MetaFileName, L1Scene02MetaString, \
                                L2Scene03MetaFileName, L1Scene03MetaString, \
                                appVerStr, productionDateTime, filenameCrosswalk, \
                                regionStr, numScenesPerTile, metaFullName):

    if (debug):
        logger.info('Buildmetadata2: Entered')
    
    SceneTagsToBeKept = ('data_provider', 'satellite', 'instrument', \
                                       'acquisition_date', 'scene_center_time', \
                                       'level1_production_date', 'wrs', 'product_id', \
                                        'lpgs_metadata_file')
    
                                                        # L2 scene information sources
    s1L2_tree = ET.parse(L2Scene01MetaFileName)
    s1L2_root = s1L2_tree.getroot()
    
                                                        # read any namespace from the 1st L2 scene
    namespace = ''
    if ('{' in s1L2_root.tag):
        endPos = (s1L2_root.tag).find('}')
        namespace = s1L2_root.tag[:endPos+1]

    s2L2_tree = ''
    s2L2_root = ''
    if (numScenesPerTile > 1):
        s2L2_tree = ET.parse(L2Scene02MetaFileName)

    s3L2_tree = ''
    s3L2_root = ''
    if (numScenesPerTile > 2):
        s3L2_tree = ET.parse(L2Scene03MetaFileName)

                                                        # Start the output xml
    outRoot = ET.Element('ard_metadata')
    outTileMetadata = ET.SubElement(outRoot, 'tile_metadata')
    
                                                        #
                                                        #
                                                        # Build the tile's global metadata
                                                        #
                                                        #
    outTileGlobal = ET.SubElement(outTileMetadata, 'global_metadata')
    
                                                        # data_provider - use L2 scene
    for child in s1L2_root.find(namespace + 'global_metadata'):
        if (child.tag == namespace + 'data_provider'):
            outDataProvider = ET.SubElement(outTileGlobal, 'data_provider', child.attrib)
            outDataProvider.text = child.text
    
                                                        # satellite - new
    gm_satellite = ET.SubElement(outTileGlobal, 'satellite')
    satelliteStr = tileID[3]
    if (satelliteStr == '4'):
        gm_satellite.text = 'LANDSAT_4'
    elif (satelliteStr == '5'):
        gm_satellite.text = 'LANDSAT_5'
    elif (satelliteStr == '7'):
        gm_satellite.text = 'LANDSAT_7'
    else:
        gm_satellite.text = 'LANDSAT_8'

                                                        # instrument - new
    gm_instrument = ET.SubElement(outTileGlobal, 'instrument')
    instrumentStr = tileID[1]
    if (instrumentStr == 'C'):
        gm_instrument.text = 'OLI/TIRS_Combined'
    elif (instrumentStr == 'O'):
        gm_instrument.text = 'OLI-only'
    elif (instrumentStr == 'E'):
        gm_instrument.text = 'ETM'
    elif (instrumentStr == 'T') and (satelliteStr == '8'):
        gm_instrument.text = 'TIRS-only'
    else:
        gm_instrument.text = 'TM'
    
                                                            # Level 1 Collection - new
    gm_l1coll = ET.SubElement(outTileGlobal, 'level1_collection')
    gm_l1coll.text = tileID[34:36]
    
                                                                # ARD Version - new
    gm_ardVersion = ET.SubElement(outTileGlobal, 'ard_version')
    gm_ardVersion.text = tileID[38:40]
    
                                                                    # Region - new
    gm_region = ET.SubElement(outTileGlobal, 'region')
    gm_region.text = tileID[5:7]
    
                                                        # acquisition date - use L2 scene
    for child in s1L2_root.find(namespace + 'global_metadata'):
        if (child.tag == namespace + 'acquisition_date'):
            outAcqDate = ET.SubElement(outTileGlobal, 'acquisition_date', child.attrib)
            outAcqDate.text = child.text
    
                                                        # tile_id - new
    gm_productid = ET.SubElement(outTileGlobal, 'product_id')
    gm_productid.text = tileID

                                                        # tile_production_date - new
    gm_tilepd = ET.SubElement(outTileGlobal, 'production_date')
    gm_tilepd.text = productionDateTime

                                                        #bounding coordinates - modify L2 scene
    if (debug):
        logger.info('Buildmetadata2: Ready for bounding_coordinates')
    newBoundingCoordsStr = global_createBoundingCoordinates(debug, logger, cutLimits, regionStr)
    tempBoundingElement = ET.fromstring(newBoundingCoordsStr)
    
    gm_bounding = ET.SubElement(outTileGlobal, tempBoundingElement.tag, tempBoundingElement.attrib)
    
    for child in tempBoundingElement:
        gm_bounding_child = ET.SubElement(gm_bounding, child.tag, child.attrib)
        gm_bounding_child.text = child.text

                                                        #projection information - modify L2 scene
    if (debug):
        logger.info('Buildmetadata2: Ready for projection information')
    newProjInfo = global_createProjInfo(debug, logger, cutLimits, regionStr)
    tempProjElement = ET.fromstring(newProjInfo)
    
    gm_ProjInfo = ET.SubElement(outTileGlobal, tempProjElement.tag, tempProjElement.attrib)

    for child in tempProjElement:
        gm_proj_child = ET.SubElement(gm_ProjInfo, child.tag, child.attrib)
        gm_proj_child.text = child.text
        if (child.tag == "albers_proj_params"):
            for projChild in child:
                gm_proj_grandchild = ET.SubElement(gm_proj_child, projChild.tag, projChild.attrib)
                gm_proj_grandchild.text = projChild.text

                                                        # orientation_angle - use L2 scene
    for child in s1L2_root.find(namespace + 'global_metadata'):
        if (child.tag == namespace + 'orientation_angle'):
            outOrientation = ET.SubElement(outTileGlobal, 'orientation_angle', child.attrib)
            outOrientation.text = child.text

                                                        # tile_grid - new
    gm_tileid = ET.SubElement(outTileGlobal, 'tile_grid')
    gm_tileid.set('v', tileID[11:14])
    gm_tileid.set('h', tileID[8:11])
    
                                                        # scene_count - new
    gm_sc = ET.SubElement(outTileGlobal, 'scene_count')
    gm_sc.text = str(numScenesPerTile)
    
    pixelTypeTuple =  createPixelTypeTuple(debug, logger, statsTuple)
    
                                                        # cloud_cover - new
    gm_cc = ET.SubElement(outTileGlobal, 'cloud_cover')
    gm_cc.text = pixelTypeTuple[0]
    
                                                        # cloud_shadow - new
    gm_cs = ET.SubElement(outTileGlobal, 'cloud_shadow')
    gm_cs.text = pixelTypeTuple[1]
    
                                                        # snow_ice - new
    gm_si = ET.SubElement(outTileGlobal, 'snow_ice')
    gm_si.text = pixelTypeTuple[2]
    
                                                        # fill - new
    gm_fill = ET.SubElement(outTileGlobal, 'fill')
    gm_fill.text = pixelTypeTuple[3]

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
    lineageStr = createLineageSection(debug, logger, tileID, appVerStr, productionDateTime)
    tempLineageElement = ET.fromstring(lineageStr)
    
    bands_lineage = ET.SubElement(outTileBands, tempLineageElement.tag, tempLineageElement.attrib)

    for child in tempLineageElement:
        bands_lineage_child = ET.SubElement(bands_lineage, child.tag, child.attrib)
        bands_lineage_child.text = child.text

                                                        # Loop through all of the bands in the L2 file.
                                                        # Each band will need to be modified to reflect the
                                                        # characteristics of the tile.

    bandsElement =  s1L2_root.find(namespace + 'bands')
    
    for curBand in bandsElement:
        oldBandStr =ET.tostring(curBand)
        newBandStr = fixTileBand2(debug, logger, tileID, filenameCrosswalk, \
                                                productionDateTime, oldBandStr)

        tempBandElement = ET.fromstring(newBandStr)
    
        bands_band = ET.SubElement(outTileBands, tempBandElement.tag, tempBandElement.attrib)

        for child in tempBandElement:
            bands_band_child = ET.SubElement(bands_band, child.tag, child.attrib)
            bands_band_child.text = child.text
            if (child.tag == "bitmap_description"):
                for bandChild in child:
                    bands_band_grandchild = ET.SubElement(bands_band_child, bandChild.tag, bandChild.attrib)
                    bands_band_grandchild.text = bandChild.text

    if (debug):
        logger.info('Buildmetadata2: finished tile bands')

                                                        #
                                                        # "Global" and "bands" have now been created for the new tiles.
                                                        #
                                                        #  Next modify the scene metadata for each contributing scene.
                                                        #  We'll have to read some values from the Level 1 (MTL.txt) file.
                                                        #
    L1Tuple = [L1Scene01MetaString, L1Scene02MetaString, L1Scene03MetaString]   # already strings
    
    L2Tuple = [s1L2_tree, s2L2_tree, s3L2_tree]    # At the top of this function, we already 
                                                                          # parsed in the L2 metadata files.  These
                                                                          # are the trees
    i = 0
    while (i < numScenesPerTile):           # for each contributing scene 

        sceneRoot = (L2Tuple[i]).getroot()              # L2 input object

                                                                          #  Read some values from the Level 1 (MTL.txt) file.
        request_id = getL1Value(debug, logger, L1Tuple[i], "REQUEST_ID")
        scene_id = getL1Value(debug, logger, L1Tuple[i], "LANDSAT_SCENE_ID")
        elev_src = getL1Value(debug, logger, L1Tuple[i], "ELEVATION_SOURCE")
        sensor_mode = getL1Value(debug, logger, L1Tuple[i], "SENSOR_MODE")
        ephemeris_type = getL1Value(debug, logger, L1Tuple[i], "EPHEMERIS_TYPE")
        cpf_name = getL1Value(debug, logger, L1Tuple[i], "CPF_NAME")
        geometric_rmse_model = getL1Value(debug, logger, L1Tuple[i], "GEOMETRIC_RMSE_MODEL")
        geometric_rmse_model_x = getL1Value(debug, logger, L1Tuple[i], "GEOMETRIC_RMSE_MODEL_X")
        geometric_rmse_model_y = getL1Value(debug, logger, L1Tuple[i], "GEOMETRIC_RMSE_MODEL_Y")

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
                outGeneric = ET.SubElement(outSceneGlobal, newTag, child.attrib)
                outGeneric.text = child.text

            if (newTag == 'wrs'):
                outGeneric = ET.SubElement(outSceneGlobal, 'request_id')
                outGeneric.text = request_id
                outGeneric = ET.SubElement(outSceneGlobal, 'scene_id')
                outGeneric.text = scene_id

            if (newTag == 'product_id'):
                outGeneric = ET.SubElement(outSceneGlobal, 'elevation_source')
                outGeneric.text = elev_src
                outGeneric = ET.SubElement(outSceneGlobal, 'sensor_mode')
                outGeneric.text = sensor_mode
                outGeneric = ET.SubElement(outSceneGlobal, 'ephemeris_type')
                outGeneric.text = ephemeris_type
                outGeneric = ET.SubElement(outSceneGlobal, 'cpf_name')
                outGeneric.text = cpf_name

            if (newTag == 'lpgs_metadata_file'):
                outGeneric = ET.SubElement(outSceneGlobal, 'geometric_rmse_model')
                outGeneric.text = geometric_rmse_model
                outGeneric = ET.SubElement(outSceneGlobal, 'geometric_rmse_model_x')
                outGeneric.text = geometric_rmse_model_x
                outGeneric = ET.SubElement(outSceneGlobal, 'geometric_rmse_model_y')
                outGeneric.text = geometric_rmse_model_y

        outSceneBands = ET.SubElement(outSceneMetadata, 'bands')

                                                                       # The scene bands
        for bandTag in sceneRoot.find(namespace + 'bands'):
            newTag = (bandTag.tag).replace(namespace, '')
            bandElement = ET.SubElement(outSceneBands, newTag, bandTag.attrib)
            bandElement.text = bandTag.text

            for child in bandTag:
                newTag2 = (child.tag).replace(namespace, '')
                childElement = ET.SubElement(bandElement, newTag2, child.attrib)
                childElement.text = child.text
                if (newTag2 == "bitmap_description"):
                    for bitmapChild in child:
                        bitmapTag = (bitmapChild.tag).replace(namespace, '')
                        bands_band_bitmap = ET.SubElement(childElement, bitmapTag, bitmapChild.attrib)
                        bands_band_bitmap.text = bitmapChild.text

        i = i + 1

    if (debug):
        logger.info('Buildmetadata2: Ready to write')

    namespace1Prefix = "xmlns"
    namespace2Prefix = "xmlns:xsi"
    namespace3Prefix = "xsi:schemaLocation"
    
    namespace1URI = "http://ard.cr.usgs.gov/v1"
    namespace2URI = "http://www.w3.org/2001/XMLSchema-instance"
    namespace3URI = "http://ard.cr.usgs.gov/v1 http://espa.cr.usgs.gov/schema/ard_metadata_v1_0.xsd"
    
    outRoot.attrib[namespace3Prefix] = namespace3URI
    outRoot.attrib[namespace2Prefix] = namespace2URI
    outRoot.attrib[namespace1Prefix] = namespace1URI
    outRoot.attrib["version"] = "1.0"

                                                                    # Add string indentation - Unfortunately, 
                                                                    # this function produces extra carriage returns 
                                                                    # after some elements...

    prettyString = minidom.parseString(ET.tostring(outRoot)).toprettyxml(encoding="utf-8", indent="    ")
    
                                                                    # Write to temp file
    try:
        uglyFullName = metaFullName.replace(".xml", "_ugly.xml")
        f = open(uglyFullName, "w")
        f.write(prettyString.encode('utf-8'))
        f.close()

    except:
        logger.error('Error: Buildmetadata2: Error when writing temp file')
        logger.error('        Error: {0}'.format(traceback.format_exc()))
        return 'metadata ERROR'

                                                                      # Looks like the minidom pretty print added some
                                                                      # blank lines followed by CRLF.  The blank lines are
                                                                      # of more than one length in our case.  Remove any
                                                                      # blank lines.

    try:
        inMetafile = open(uglyFullName, "r")
        outMetafile = open(metaFullName, "w")

        for curLine in inMetafile:

            allSpaces = True
            for curChar in curLine:
                if (curChar != '\x20') and (curChar != '\x0D') and (curChar != '\x0A'):
                    allSpaces = False
                    continue

            if (allSpaces == False):
                outMetafile.write(curLine)
            #else:
            #    print 'Found blank line'

        inMetafile.close()
        outMetafile.close()

    except:
        logger.error('Error: Buildmetadata2: Error when fixing file')
        logger.error('        Error: {0}'.format(traceback.format_exc()))
        return 'metadata ERROR'

    return 'okay'
    
    
# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Read a value from the level 1 metadata string
#
def getL1Value(debug, logger, L1String, key):

    startPos = L1String.find(key)
    startPos = L1String.find('=', startPos)
    endPos = L1String.find('\n', startPos)
    rawValue = L1String[startPos+2:endPos]
    return rawValue.replace('"', '')


# ----------------------------------------------------------------------------------------------
#
#  A tile always has 5000 x 5000 = 25000000 pixels
#
#  Fill - % of the entire tile that is fill
#
#  Cloud, Cloud Shadow, and Snow/Ice are % of Non-Fill pixels
#
#  Cloud Cover is calculate by adding two separate categories:
#            True Cloud Cover
#            Snow/Cloud combination - see the 'parseHistFile' function for more info
#
def createPixelTypeTuple(debug, logger, longsTuple):

                                                 # Calculate fill
    fillLong = longsTuple[0] / 25000000.0 * 100.0
    fillStr = '{:0.4f}'.format(fillLong)

    numNonFillPixels = 25000000.0 - longsTuple[0]
    
                                                 # Calculate Cloud Cover
    cloudLong = (longsTuple[5] + longsTuple[6]) / numNonFillPixels * 100.0
    cloudStr = '{:0.4f}'.format(cloudLong)

                                                 # Calculate Cloud Shadow
    shadowLong = longsTuple[4] / numNonFillPixels * 100.0
    shadowStr = '{:0.4f}'.format(shadowLong)
    
                                                 # Calculate Snow/Ice
    snowLong = longsTuple[3] / numNonFillPixels * 100.0
    snowStr = '{:0.4f}'.format(snowLong)
    
    pixelTypeTuple = (cloudStr, shadowStr, snowStr, fillStr)
    return pixelTypeTuple


# ----------------------------------------------------------------------------------------------
#
#   Purpose:  To create the lineage band metadata block.  This band originates with
#                      the tile, not with the scene.
#
def createLineageSection(debug, logger, tileID, appVersion, prodDate):

    lineageText = '<band fill_value="0" nsamps="5000" nlines="5000" data_type="UINT8" '
    lineageText += 'category="metadata" name="LINEAGEQA" product="scene_index" '
    lineageText += 'source="level2">'
    lineageText += '<short_name>TILEIDX</short_name>'
    lineageText += '<long_name>index</long_name>'
    lineageText += '<file_name>' + tileID + '_LINEAGEQA.tif</file_name>'
    lineageText += '<pixel_size units="meters" y="30" x="30"/>'
    lineageText += '<resample_method>none</resample_method>'
    lineageText += '<data_units>index</data_units>'
    lineageText += '<valid_range max="255.000000" min="0.000000"/>'
#    lineageText += '<app_version>' + appVersion + '</app_version>'
    lineageText += '<production_date>' + prodDate + '</production_date>'
    lineageText += '</band>'

    return lineageText


# ----------------------------------------------------------------------------------------------
#
#   This is used to alter the L2 metadata to create the section of the new metadata that
#   describes the tile bands.
#
#   curPos coming in is the position in the file pointing to a <band> tag.
#
def fixTileBand2(debug, logger, tileID, filenameCrosswalk, \
                            productionDateTime, bandTag):

                                                                # remove namespace info
    bandTag = bandTag.replace('ns0:', '')
    nsBig = 'xmlns:ns0="http://espa.cr.usgs.gov/v2"'
    bandTag = bandTag.replace(nsBig, '')

    
                                                                # find nsamps within the <band> tag
    sampsBeginPos = bandTag.find("nsamps")
    sampsEndPos = bandTag.find('"', sampsBeginPos + 8)
    oldSamps = bandTag[sampsBeginPos:sampsEndPos+1]
    if (debug):
        logger.info('      > meta > fixTileBand > oldSamps: {0}'.format(oldSamps))
    newSamps = 'nsamps="5000"'
    
                                                                 # find nlines within the <band> tag
    linesBeginPos = bandTag.find("nlines")
    linesEndPos = bandTag.find('"', linesBeginPos + 8)
    oldLines = bandTag[linesBeginPos:linesEndPos+1]
    if (debug):
        logger.info('      > meta > fixTileBand > oldLines: {0}'.format(oldLines))
    newLines = 'nlines="5000"'
    
                                                                 # L2 band names need to be renamed in two places -
                                                                 # 1. in the band tag
    nameBeginPos = bandTag.find("name")
    nameEndPos = bandTag.find('"', nameBeginPos + 7)
    oldBandName = bandTag[nameBeginPos:nameEndPos+1]
    if (debug):
        logger.info('      > meta > fixTileBand > oldBandName: {0}'.format(oldBandName))
    nameOnly = bandTag[nameBeginPos+6:nameEndPos]
    newName = 'name="' + getARDName(nameOnly, filenameCrosswalk) + '"'
    
                                                                 # 2. in the file_name tag
    startFilePos = bandTag.find('<file_name>', 0)
    endFilePos = bandTag.find('</file_name>', startFilePos)
    oldFileText = bandTag[startFilePos+11:endFilePos]
    newFileText = tileID + '_' + getARDName(nameOnly, filenameCrosswalk) + '.tif'
    if (debug):
        logger.info('      > meta > fixTileBand > oldFilename: {0}'.format(oldFileText))
    
                                                                 # Modify <production_date>
    startDatePos = bandTag.find('<production_date>', 0)
    endDatePos = bandTag.find('</production_date>', startDatePos)
    oldDateText = bandTag[startDatePos:endDatePos+18]
    newDateText = '<production_date>' + productionDateTime + '</production_date>'
    if (debug):
        logger.info('      > meta > fixTileBand > oldDate: {0}'.format(oldDateText))
    
                                                                 # perform the substitutions in the <band> tag
    bandTag = bandTag.replace(oldSamps, newSamps)
    bandTag = bandTag.replace(oldLines, newLines)
    bandTag = bandTag.replace(oldBandName, newName)

                                                                 # perform the substitutions to other tags
    bandTag = bandTag.replace(oldFileText, newFileText)
    bandTag = bandTag.replace(oldDateText, newDateText)

    return bandTag
    
    
# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Create a string containing the WGS84 geographic coordinates of the 
#                      bounding box.
#
def global_createBoundingCoordinates(debug, logger, cutLimits, region):

                                                        # Reproject corner coords into WGS84 
                                                        # geographic for the metadata

    prjStrCU = "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 " + \
                    "+y_0=0 +datum=WGS84 +units=m +no_defs"
    prjStrHI = "+proj=aea +lat_1=8 +lat_2=18 +lat_0=3 +lon_0=-157 +x_0=0 " + \
                    "+y_0=0 +datum=WGS84 +units=m +no_defs"
    prjStrAK = "+proj=aea +lat_1=55 +lat_2=65 +lat_0=50 +lon_0=-154 +x_0=0 " + \
                    "+y_0=0 +datum=WGS84 +units=m +no_defs"

    if (region == 'CU'):
        prjStr = prjStrCU
    elif (region == 'HI'):
        prjStr = prjStrHI
    else:
        prjStr = prjStrAK
    
    blank1Pos = cutLimits.find(' ')
    blank2Pos = cutLimits.find(' ', blank1Pos + 1)
    blank3Pos = cutLimits.find(' ', blank2Pos + 1)
    cutLeft = cutLimits[0:blank1Pos]
    cutBottom = cutLimits[blank1Pos+1:blank2Pos]
    cutRight = cutLimits[blank2Pos+1:blank3Pos]
    cutTop = cutLimits[blank3Pos+1:len(cutLimits)-1]

    s_srs = osr.SpatialReference()
    t_srs = osr.SpatialReference()
    s_srs.ImportFromProj4(prjStr)
    t_srs.ImportFromEPSG(4326)
    geom = ogr.Geometry(ogr.wkbPoint)
    
    geom.SetPoint_2D(0, float(cutLeft), float(cutTop))
    geom.AssignSpatialReference(s_srs)
    geom.TransformTo(t_srs)
    ulCoordStr =  geom.GetPoint_2D()
    
    geom.SetPoint_2D(0, float(cutRight), float(cutTop))
    geom.AssignSpatialReference(s_srs)
    geom.TransformTo(t_srs)
    urCoordStr =  geom.GetPoint_2D()
    
    geom.SetPoint_2D(0, float(cutLeft), float(cutBottom))
    geom.AssignSpatialReference(s_srs)
    geom.TransformTo(t_srs)
    llCoordStr = geom.GetPoint_2D()
    
    geom.SetPoint_2D(0, float(cutRight), float(cutBottom))
    geom.AssignSpatialReference(s_srs)
    geom.TransformTo(t_srs)
    lrCoordStr = geom.GetPoint_2D()

    
    coordMsg = "Newly generated corner coordinates: " + "UL = " + str(ulCoordStr) + \
                      " UR = " + str(urCoordStr) + " LL = " + str(llCoordStr) + " LR = " + str(lrCoordStr)
    logger.info('      > meta: {0}'.format(coordMsg))
    
    longitudes = []
    longitudes.append(ulCoordStr[0])
    longitudes.append(urCoordStr[0])
    longitudes.append(llCoordStr[0])
    longitudes.append(lrCoordStr[0])
    
    latitudes = []
    latitudes.append(ulCoordStr[1])
    latitudes.append(urCoordStr[1])
    latitudes.append(llCoordStr[1])
    latitudes.append(lrCoordStr[1])
    
    latN = max(latitudes)
    latS = min(latitudes)
    lonW = min(longitudes)
    lonE = max(longitudes)
    
    print coordMsg
    newBoundingCoords = '<bounding_coordinates><west>' + str(lonW) + '</west><east>' + \
            str(lonE) + '</east><north>' + str(latN) + '</north><south>' + str(latS) + \
            '</south></bounding_coordinates>'
    logger.info('      > meta: {0}'.format(newBoundingCoords))
    
    return newBoundingCoords


# ----------------------------------------------------------------------------------------------
#
#   Purpose:  The projection information depends on the geographic region - 
#                      Conterminous US, Alaska, or Hawaii
#                     
#                      The corner points are from the tile limits
#
def global_createProjInfo(debug, logger, cutLimits, region):

                                                               # cutLimits is a string separated by blanks
    blank1Pos = cutLimits.find(' ')
    blank2Pos = cutLimits.find(' ', blank1Pos + 1)
    blank3Pos = cutLimits.find(' ', blank2Pos + 1)
    cutLeft = float(cutLimits[0:blank1Pos])
    cutBottom = float(cutLimits[blank1Pos+1:blank2Pos])
    cutRight = float(cutLimits[blank2Pos+1:blank3Pos])
    cutTop = float(cutLimits[blank3Pos+1:len(cutLimits)-1])
    
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
    blurb = '<projection_information units="meters" datum="WGS84" projection="AEA">'
    blurb += '<corner_point y="' + '{:0.6f}'.format(cutTop) 
    blurb += '" x="' + '{:0.6f}'.format(cutLeft)  + '" location="UL"/>'
    blurb += '<corner_point y="' + '{:0.6f}'.format(cutBottom) 
    blurb += '" x="' + '{:0.6f}'.format(cutRight)  + '" location="LR"/>'
    blurb += '<grid_origin>CORNER</grid_origin>' 
    blurb += '<albers_proj_params>' + prjStr + '</albers_proj_params>'
    blurb += '</projection_information>'
    
    return blurb
