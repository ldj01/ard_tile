FROM geos-ard-external

MAINTAINER USGS EROS SDW http://eros.usgs.gov
LABEL description="Create Tiles from Scene(s)"

ENV ARD_SCHEMA=/usr/local/usgs/ard_tile/auxiliaries/ard_metadata_v1_0.xsd \
    ARD_AUX_DIR=/usr/local/usgs/ard_tile/auxiliaries

RUN pip install --upgrade git+https://github.com/USGS-EROS/espa-python-library.git@v1.1.0#espa

COPY ARD_Clip_L457.py /usr/local/bin/
COPY ARD_Clip_L8.py /usr/local/bin/
COPY ARD_regionLU.py /usr/local/bin/
COPY ARD_HelperFunctions.py /usr/local/bin/
COPY ARD_metadata.py /usr/local/bin/

WORKDIR /home/tilest

#ENTRYPOINT ["ARD_Clip.py"]
