FROM losrlcmp02.cr.usgs.gov:5000/ora_inst:latest
MAINTAINER USGS EROS LSIS http://eros.usgs.gov
LABEL description="Segment of Scenes Determination"

RUN yum -y update && yum clean all


# The latest versions of Mesos require svn development package >= 1.9
COPY wandisco-svn.repo /etc/yum.repos.d/wandisco-svn.repo

RUN yum -y install http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-3.noarch.rpm \
    && yum install -y mesos \
    && yum clean all


RUN pip install protobuf


COPY determine_segments.py /usr/local/bin/
WORKDIR /home/tilest


#CMD determine_segments.py
