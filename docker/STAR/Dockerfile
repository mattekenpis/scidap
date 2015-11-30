#################################################################
# Dockerfile
#
# Software:         STAR 
# Software Version: 2.5.0a
# Description:      STAR image for SciDAP
# Website:          https://github.com/alexdobin/STAR, http://scidap.com/
# Provides:         STAR
# Base Image:       scidap/scidap:v0.0.1
# Build Cmd:        docker build --rm -t scidap/star:v2.5.0a .
# Pull Cmd:         docker pull scidap/star:v2.5.0a
# Run Cmd:          docker run --rm scidap/star:v2.5.0a STAR
#################################################################

### Base Image
FROM scidap/scidap:v0.0.1
MAINTAINER Andrey V Kartashov "porter@porter.st"
ENV DEBIAN_FRONTEND noninteractive

################## BEGIN INSTALLATION ######################

WORKDIR /tmp

### Install STAR

ENV VERSION 2.5.0a
ENV NAME STAR
ENV URL "https://github.com/alexdobin/STAR/archive/${NAME}_${VERSION}.tar.gz"

RUN wget -q -O - $URL | tar -zxv && \
cd ${NAME}-${NAME}_${VERSION}/source && \
make -j 4 && \
cd .. && \
cp ./bin/Linux_x86_64_static/STAR /usr/local/bin/ && \
cd .. && \
strip /usr/local/bin/${NAME}; true && \
rm -rf ./${NAME}-${NAME}_${VERSION}/
