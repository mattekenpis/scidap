#################################################################
# Dockerfile
#
# Software:         tabix
# Software Version: 1.2.1
# Description:      tabix image for SciDAP
# Website:          https://samtools.github.io, http://scidap.com/
# Provides:         tabix|bgzip|htsfile
# Base Image:       scidap/scidap:v0.0.1
# Build Cmd:        docker build --rm -t scidap/tabix:v1.2.1 .
# Pull Cmd:         docker pull scidap/tabix:v1.2.1
# Run Cmd:          docker run --rm scidap/tabix:v1.2.1 tabix
#################################################################

### Base Image
FROM scidap/scidap:v0.0.1
MAINTAINER Andrey V Kartashov "porter@porter.st"
ENV DEBIAN_FRONTEND noninteractive

################## BEGIN INSTALLATION ######################

WORKDIR /tmp

ENV VERSIONH 1.2.1
ENV NAMEH htslib
ENV URLH "https://github.com/samtools/htslib/archive/${VERSIONH}.tar.gz"

RUN wget -q -O - $URLH | tar -zxv && \
cd ${NAMEH}-${VERSIONH} && \
make -j 4 && \
cd .. && \
cp ./${NAMEH}-${VERSIONH}/tabix /usr/local/bin/ && \
cp ./${NAMEH}-${VERSIONH}/bgzip /usr/local/bin/ && \
cp ./${NAMEH}-${VERSIONH}/htsfile /usr/local/bin/ && \
strip /usr/local/bin/tabix; true && \
strip /usr/local/bin/bgzip; true && \
strip /usr/local/bin/htsfile; true && \
rm -rf ./${NAMEH}-${VERSIONH}/

