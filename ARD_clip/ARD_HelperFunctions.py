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
#   Purpose:  Either write to logfile, or write to stdout, depending on debug flag
#
def logIt(debug, logName, inStr):

    if (debug):
        appendToLog(logName, inStr)
    else:
        reportToStdout(inStr)
    
# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Prepend any log entry with a date-time stamp
#
def appendToLog(logName, inStr):

    currentTime = datetime.datetime.now()
    content = currentTime.strftime('%Y-%m-%d %H:%M:%S > ' + inStr + '\n')
    f = open(logName, 'a')
    f.write(content)
    f.close()

# ----------------------------------------------------------------------------------------------
#
#   Purpose:  Prepend any stdout logging with a date-time stamp
#
def reportToStdout(inStr):

    currentTime = datetime.datetime.now()
    content = currentTime.strftime('ARD_Tiling> %Y-%m-%d %H:%M:%S > ' + inStr + '\n')
    print content
    
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
#                     5000p X 5000p = 25000000 pixels should all be accounted for.
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

    histArray = []
    headLoc = histLines.find('  ', colonLoc) + 2
    while (len(histArray) <= 255):
        tailLoc = histLines.find(' ', headLoc)
        histArray.append(histLines[headLoc:tailLoc])
        headLoc = tailLoc + 1
    
    countFill = long(histArray[1])
    countClear = long(histArray[2]) + long(histArray[66]) + long(histArray[130]) + long(histArray[194])
    countWater = long(histArray[4]) + long(histArray[68]) + long(histArray[132]) + long(histArray[196])
    countShadow = long(histArray[8]) + long(histArray[72]) + long(histArray[136]) + long(histArray[200])
    countSnow = long(histArray[16]) + long(histArray[80]) + long(histArray[144]) + long(histArray[208])
    countCloud = long(histArray[32]) + long(histArray[96]) + long(histArray[160]) +long(histArray[224])
    
                                                                       # Per discussion with Calli Jenkerson and Steve Foga on
                                                                       # 16 Feb 2017:
                                                                       #
                                                                       # The following are Snow/Cloud categories
                                                                       # They will be treated as "Cloud" for the purposes
                                                                       # of calculating the percentages
                                                                       #

    countCombo = long(histArray[48]) + long(histArray[112]) + long(histArray[176]) + long(histArray[240])
     
    total = countFill + countClear + countWater + countSnow + countShadow + countCloud + countCombo
    
                                                                       # Even if error, send back a tuple 
    if (total != 25000000):
        return ('Pixel QA values do not add up',)
    else:
        return (countFill, countClear, countWater, countSnow, countShadow, countCloud, countCombo)

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

    if (count3 > 0):
        return 3
    elif (count2 > 0):
        return 2
    elif (count1 > 0):
        return 1
    else:
        return 0

# ----------------------------------------------------------------------------------------------
#
# Message logging filter
class L2PGS_LoggingFilter(logging.Filter):
    def filter(self, record):
        record.subsystem = 'ARDTile'

        return True

# ----------------------------------------------------------------------------------------------
#
# Exception formatter
class L2PGS_ExceptionFormatter(logging.Formatter):
    def formatException(self, exc_info):
        result = super(L2PGS_ExceptionFormatter, self).formatException(exc_info)
        return repr(result)

    def format(self, record):
        s = super(L2PGS_ExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', ' ')
            s = s.replace('\\n', ' ')
        return s

# ----------------------------------------------------------------------------------------------
#
# Initialize the message logging components.
def setup_logging():


    # Setup the logging level
    logging_level = logging.INFO

    handler = logging.StreamHandler(sys.stdout)
    formatter = L2PGS_ExceptionFormatter(fmt=('%(asctime)s.%(msecs)03d'
                                              ' %(subsystem)s'
                                              ' %(levelname)-8s'
                                              ' %(message)s'),
                                         datefmt='%Y-%m-%dT%H:%M:%S')

    handler.setFormatter(formatter)
    handler.addFilter(L2PGS_LoggingFilter())

    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.addHandler(handler)

    logging.getLogger('requests').setLevel(logging.WARNING)

    return logger

# ==========================================================================





