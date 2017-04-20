FROM ard-external

MAINTAINER USGS EROS SDW http://eros.usgs.gov
LABEL description="Create Tiles from Scene(s)"


COPY ARD_Clip_L457.py /usr/local/bin/
COPY ARD_Clip_L8.py /usr/local/bin/
COPY ARD_regionLU.py /usr/local/bin/
COPY ARD_scenesForTilePathLU.py /usr/local/bin/
COPY ARD_tilesForSceneLU.py /usr/local/bin/
Copy ARD_HelperFunctions.py /usr/local/bin/
COPY CU_tileFootprints.py /usr/local/bin/
COPY HI_tileFootprints.py /usr/local/bin/
COPY AK_tileFootprints.py /usr/local/bin/
COPY ARD_metadata.py /usr/local/bin/

WORKDIR /home/tilest

#ENTRYPOINT ["ARD_Clip.py"]
