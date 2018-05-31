FROM losrlcmp02.cr.usgs.gov:5000/ora_inst:latest as geos-ard-external

LABEL description="External GIS software" \
      maintainer="USGS EROS LSRD http://eros.usgs.gov"


# Compilers and all the rquired libraries
RUN yum group install -y "Development Tools" \
    && yum install -y \
        e2fsprogs \
        expat-devel \
        freetype-devel \
        glibc-static \
        libcurl-devel \
        libidn-devel \
        libgfortran-static \
        libquadmath-static \
        perl-ExtUtils-MakeMaker \
        texinfo \
        bzip2-devel \
        zlib-devel \
        zlib-static \
        libpng-devel \
        libpng-static \
        rpm-build \
    && yum clean metadata


# Versions and checksums
ENV LIBXML2_VERSION=2.9.3 \
	LIBXML2_SHA256=4de9e31f46b44d34871c22f54bfc54398ef124d6f7cafb1f4a5958fbcd3ba12d  \
    LIBXSLT_VERSION=1.1.28 \
	LIBXSLT_SHA256=5fc7151a57b89c03d7b825df5a0fae0a8d5f05674c0e7cf2937ecec4d54a028c  \
    PROJ4_VERSION=4.9.1 \
	PROJ4_SHA256=fca0388f3f8bc5a1a803d2f6ff30017532367992b30cf144f2d39be88f36c319  \
    GEOS_VERSION=3.6.1 \
    GEOS_SHA256=4a2e4e3a7a09a7cfda3211d0f4a235d9fd3176ddf64bd8db14b4ead266189fc5  \
    GDAL_VERSION=1.11.4 \
    GDAL_SHA256=f60e736db626b9d7ca3549bdd53d1699950af488ed4190b4ddb54c71d22411be  \
    PYTHON_NUMPY_VERSION=1.11.0

# Source and installation locations
ENV PREFIX=/usr/local \
    SRC_DIR=/usr/local/src \
    ZLIB_PREFIX=/usr/local \
    XZ_PREFIX=/usr/local \
    SZIP_PREFIX=/usr/local \
    PNG_PREFIX=/usr/local \
    FREETYPE2_PREFIX=/usr/local \
    XML2_PREFIX=/usr/local \
    XSLT_PREFIX=/usr/local \
    IDN_PREFIX=/usr/local/idn \
    CURL_PREFIX=/usr/local/curl \
    JPEG_PREFIX=/usr/local \
    JBIG_PREFIX=/usr/local \
    TIFF_PREFIX=/usr/local \
    GEOTIFF_PREFIX=/usr/local \
    HDF4_PREFIX=/usr/local \
    HDF5_PREFIX=/usr/local \
    NETCDF4_PREFIX=/usr/local \
    HDFEOS_PREFIX=/usr/local \
    PROJ4_PREFIX=/usr/local \
    GDAL_PREFIX=/usr/local \
    GEOS_PREFIX=/usr/local


# Include and library paths
ENV ZLIBINC=$ZLIB_PREFIX/include \
    ZLIBLIB=$ZLIB_PREFIX/lib \
    XZINC=$XZ_PREFIX/include \
    XZLIB=$XZ_PREFIX/lib \
    LZMAINC=$XZ_PREFIX/include \
    LZMALIB=$XZ_PREFIX/lib \
    SZIPINC=$SZIP_PREFIX/include \
    SZIPLIB=$SZIP_PREFIX/lib \
    PNGINC=$PNG_PREFIX/include \
    PNGLIB=$PNG_PREFIX/lib \
    FREETYPE2INC=$FREETYPE2_PREFIX/include \
    FREETYPE2LIB=$FREETYPE2_PREFIX/lib \
    XML2INC=$XML2_PREFIX/include/libxml2 \
    XML2LIB=$XML2_PREFIX/lib \
    XSLTINC=$XSLT_PREFIX/include/libxslt \
    XSLTLIB=$XSLT_PREFIX/lib \
    IDNINC=$IDN_PREFIX/include \
    IDNLIB=$IDN_PREFIX/lib \
    CURLINC=$CURL_PREFIX/include \
    CURLLIB=$CURL_PREFIX/lib \
    JPEGINC=$JPEG_PREFIX/include \
    JPEGLIB=$JPEG_PREFIX/lib \
    JBIGINC=$JBIG_PREFIX/include \
    JBIGLIB=$JBIG_PREFIX/lib \
    TIFFINC=$TIFF_PREFIX/include \
    TIFFLIB=$TIFF_PREFIX/lib \
    GEOTIFFINC=$GEOTIFF_PREFIX/include \
    GEOTIFFLIB=$GEOTIFF_PREFIX/lib \
    GEOTIFF_INC=$GEOTIFF_PREFIX/include \
    GEOTIFF_LIB=$GEOTIFF_PREFIX/lib \
    HDFINC=$HDF4_PREFIX/include \
    HDFLIB=$HDF4_PREFIX/lib \
    HDF4INC=$HDF4_PREFIX/include \
    HDF4LIB=$HDF4_PREFIX/lib \
    HDF5INC=$HDF5_PREFIX/include \
    HDF5LIB=$HDF5_PREFIX/lib \
    NCDF4INC=$NETCDF4_PREFIX/include \
    NCDF4LIB=$NETCDF4_PREFIX/lib \
    NETCDF4INC=$NETCDF4_PREFIX/include \
    NETCDF4LIB=$NETCDF4_PREFIX/lib \
    HDFEOSINC=$HDFEOS_PREFIX/include \
    HDFEOSLIB=$HDFEOS_PREFIX/lib \
    HDFEOS_INC=$HDFEOS_PREFIX/include \
    HDFEOS_LIB=$HDFEOS_PREFIX/lib \
    HDFEOS_GCTPINC=$HDFEOS_PREFIX/include \
    HDFEOS_GCTPLIB=$HDFEOS_PREFIX/lib \
    PROJ4_INC=$PROJ4_PREFIX/include \
    PROJ4_LIB=$PROJ4_PREFIX/lib \
    GDAL_INC=$GDAL_PREFIX/include \
    GDAL_LIB=$GDAL_PREFIX/lib


# Needed for python to find installed shared libraries
# May also solve some hdfeos issues
ENV LD_LIBRARY_PATH=/usr/local/lib


# Install libxml2
RUN wget -nv ftp://xmlsoft.org/libxml2/libxml2-${LIBXML2_VERSION}.tar.gz \
    && sha256sum libxml2-${LIBXML2_VERSION}.tar.gz | grep -E "^${LIBXML2_SHA256}" \
    && { tar -xf libxml2-${LIBXML2_VERSION}.tar.gz \
    && cd libxml2-${LIBXML2_VERSION} \
    && ./configure --prefix=${XML2_PREFIX} \
        --with-zlib=${ZLIB_PREFIX} \
        --with-lzma=${XZ_PREFIX} \
        --with-python \
        --enable-shared \
        --enable-static \
    && make -j4  \
    && make install; } &>/dev/null \
    && cd ${SRC_DIR} \
    && rm -rf *


# Install libxslt
RUN wget -nv ftp://xmlsoft.org/libxslt/libxslt-${LIBXSLT_VERSION}.tar.gz \
    && sha256sum libxslt-${LIBXSLT_VERSION}.tar.gz | grep -E "^${LIBXSLT_SHA256}" \
    && { tar -xf libxslt-${LIBXSLT_VERSION}.tar.gz \
    && cd libxslt-${LIBXSLT_VERSION} \
    && ./configure --prefix=${XSLT_PREFIX} \
        --with-libxml-libs-prefix=${XML2LIB} \
        --with-python \
        --enable-shared \
        --enable-static \
    && make -j4 \
    && make install; } &>/dev/null \
    && cd ${SRC_DIR} \
    && rm -rf *


# Install proj4
RUN wget -nv http://download.osgeo.org/proj/proj-${PROJ4_VERSION}.tar.gz \
    && sha256sum proj-${PROJ4_VERSION}.tar.gz | grep -E "^${PROJ4_SHA256}" \
    && { tar -xf proj-${PROJ4_VERSION}.tar.gz \
    && cd proj-${PROJ4_VERSION} \
    && ./configure --prefix=${PROJ4_PREFIX} \
        --enable-shared \
        --enable-static \
    && make -j4 \
    && make install; } &>/dev/null \
    && cd ${SRC_DIR} \
    && rm -rf *


# Python environment setup
# GDAL is installed into python through the below installation, because Numpy
# must be installed into python before building GDAL
RUN pip install --upgrade pip \
    && pip install \
        --global-option=build_ext \
        --global-option="-L/usr/local/lib" \
        --global-option="-I/usr/local/include" \
        --global-option="-I/usr/local/include/libxml2" \
        python-dateutil==2.5.3 \
        requests==2.10.0 \
        lxml==3.6.0 \
        numpy==${PYTHON_NUMPY_VERSION}


# Install GEOS
RUN wget -nv http://download.osgeo.org/geos/geos-${GEOS_VERSION}.tar.bz2 \
    && sha256sum geos-${GEOS_VERSION}.tar.bz2 | grep -E "^${GEOS_SHA256}" \
    && { tar -xjf geos-${GEOS_VERSION}.tar.bz2 \
    && cd geos-${GEOS_VERSION} \
    && ./configure --prefix=${GEOS_PREFIX} \
        --enable-shared \
        --enable-static \
    && make -j4 \
    && make install; } &>/dev/null \
    && cd ${SRC_DIR} \
    && rm -rf *


# Install GDAL
RUN wget -nv http://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz \
    && sha256sum gdal-${GDAL_VERSION}.tar.gz | grep -E "^${GDAL_SHA256}" \
    && { tar -xf gdal-${GDAL_VERSION}.tar.gz \
    && cd gdal-${GDAL_VERSION} \
    && ./configure --prefix=${GDAL_PREFIX} \
        --with-liblzma \
        --with-python=yes \
        --with-static-proj4=${PROJ4_LIB} \
        --enable-shared \
        --enable-static \
    && make -j4 \
    && make install; } &>/dev/null \
    && cd ${SRC_DIR} \
    && rm -rf *

ENV PATH=/usr/local/bin:${PATH}
