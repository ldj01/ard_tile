FROM losrlcmp02.cr.usgs.gov:5000/ora_inst:latest


MAINTAINER USGS EROS LSRD http://eros.usgs.gov


LABEL description="External software we use installed into the image as well as additional CENTOS repo packages required for building and executing ESPA-Product-Fulfillment."


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


# Update git to a version that isn't broken
RUN cd /usr/local/src \
    && git clone \
        --depth 1 \
        --branch v2.9.2 \
        https://github.com/git/git.git git.git \
    && cd git.git \
    && make configure \
    && ./configure --without-tcltk \
    && make \
    && make install \
    && cd .. \
    && rm -rf git.git


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
    GDAL_PREFIX=/usr/local


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
COPY external_tools/libxml2-2.9.3.tar.gz $SRC_DIR
RUN cd $SRC_DIR \
    && tar -xvf libxml2-2.9.3.tar.gz \
    && cd libxml2-2.9.3 \
    && ./configure --prefix=$XML2_PREFIX \
        --with-zlib=$ZLIB_PREFIX \
        --with-lzma=$XZ_PREFIX \
        --with-python \
        --enable-shared \
        --enable-static \
    && make \
    && make install \
    && cd $SRC_DIR \
    && rm -rf libxml2-2.9.3 libxml2-2.9.3.tar.gz


# Install libxslt
COPY external_tools/libxslt-1.1.28.tar.gz $SRC_DIR
RUN cd $SRC_DIR \
    && tar -xvf libxslt-1.1.28.tar.gz \
    && cd libxslt-1.1.28 \
    && ./configure --prefix=$XSLT_PREFIX \
        --with-libxml-libs-prefix=$XML2LIB \
        --with-python \
        --enable-shared \
        --enable-static \
    && make \
    && make install \
    && cd $SRC_DIR \
    && rm -rf libxslt-1.1.28 libxslt-1.1.28.tar.gz

# Install proj4
COPY external_tools/proj-4.9.1.tar.gz $SRC_DIR
RUN cd $SRC_DIR \
    && tar -xvf proj-4.9.1.tar.gz \
    && cd proj-4.9.1 \
    && ./configure --prefix=$PROJ4_PREFIX \
        --enable-shared \
        --enable-static \
    && make \
    && make install \
    && cd $SRC_DIR \
    && rm -rf proj-4.9.1 proj-4.9.1.tar.gz


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
        numpy==1.11.0 \
        matplotlib==1.5.1


# Install GDAL
COPY external_tools/gdal-1.11.4.tar.gz $SRC_DIR
RUN cd $SRC_DIR \
    && tar -xvf gdal-1.11.4.tar.gz \
    && cd gdal-1.11.4 \
    && ./configure --prefix=$GDAL_PREFIX \
        --with-liblzma \
        --with-python=yes \
        --with-static-proj4=$PROJ4_LIB \
        --enable-shared \
        --enable-static \
    && make \
    && make install \
    && cd $SRC_DIR \
    && rm -rf gdal-1.11.4 gdal-1.11.4.tar.gz
