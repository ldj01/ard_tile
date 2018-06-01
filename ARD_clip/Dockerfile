FROM losrlcmp02.cr.usgs.gov:5000/geos-ard-external as ard-clip

LABEL description="Create Tiles from Scene(s)" \
      maintainer="USGS EROS SDW http://eros.usgs.gov"

ENV ARD_SCHEMA=/usr/local/usgs/ard_tile/auxiliaries/ard_metadata_v1_1.xsd \
    ARD_AUX_DIR=/usr/local/usgs/ard_tile/auxiliaries

RUN pip install --upgrade PyYAML git+https://github.com/USGS-EROS/espa-python-library.git@v1.1.0#espa

COPY ./* /usr/local/bin/
WORKDIR /usr/local/data

ENTRYPOINT ["cli.py"]
