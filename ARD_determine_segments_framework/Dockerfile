FROM losrlcmp02.cr.usgs.gov:5000/ora_inst:latest
MAINTAINER USGS EROS L2PGS http://eros.usgs.gov
LABEL description="Segment of Scenes Determination"

RUN yum -y update && yum clean all

RUN yum -y install http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm && \
    yum install -y mesos

RUN yum -y downgrade mesos-1.1.0

RUN pip install protobuf

COPY determine_segments.py /usr/local/bin/
WORKDIR /home/tilest

#CMD determine_segments.py
