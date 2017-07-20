FROM losrlcmp02.cr.usgs.gov:5000/ora_inst:latest
MAINTAINER USGS EROS L2PGS http://eros.usgs.gov
LABEL description="Segment of Scenes Determination"

RUN yum -y update && yum clean all

RUN yum -y install http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm && \
    yum install -y mesos

RUN yum -y downgrade mesos-1.1.0

RUN pip install protobuf

# Set the Mesos SSL environment variables.
ENV LIBPROCESS_SSL_ENABLED=1 \
    LIBPROCESS_SSL_VERIFY_CERT=0 \
    LIBPROCESS_SSL_CERT_FILE="/etc/mesos/mesos_certpack/mesos.crt" \
    LIBPROCESS_SSL_KEY_FILE="/etc/mesos/mesos_certpack/mesos.key" \
    LIBPROCESS_SSL_CA_DIR="/etc/mesos/mesos_certpack" \
    LIBPROCESS_SSL_ENABLE_SSL_V3=0 \
    LIBPROCESS_SSL_ENABLE_TLS_V1_0=0 \
    LIBPROCESS_SSL_ENABLE_TLS_V1_1=0 \
    LIBPROCESS_SSL_ENABLE_TLS_V1_2=1


COPY determine_segments.py /usr/local/bin/
WORKDIR /home/tilest

#CMD determine_segments.py
