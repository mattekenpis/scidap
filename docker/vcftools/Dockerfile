#################################################################
# Dockerfile
#
# Software:         vcftools
# Software Version: 0.1.14
# Description:      vcftools image for SciDAP
# Website:          https://vcftools.github.io/man_latest.html, http://scidap.com/
# Provides:         vcftools
# Base Image:       scidap/scidap:v0.0.1
# Build Cmd:        docker build --rm -t scidap/vcftools:v0.1.14 .
# Pull Cmd:         docker pull scidap/vcftools:v0.1.14
# Run Cmd:          docker run --rm scidap/vcftools:v0.1.14 vcftools
#################################################################

### Base Image
FROM scidap/scidap:v0.0.1
MAINTAINER Andrey V Kartashov "porter@porter.st"
ENV DEBIAN_FRONTEND noninteractive

################## BEGIN INSTALLATION ######################

WORKDIR /tmp

### Installing VCFTools

ENV VERSION 0.1.14
ENV NAME vcftools
ENV URL "https://github.com/vcftools/vcftools/releases/download/v${VERSION}/${NAME}-${VERSION}.tar.gz"

RUN wget -q -O - $URL | tar -zxv && \
cd ${NAME}-${VERSION} && \
./configure --prefix=/usr/local && \
make -j 4 install && \
cd .. && \
rm -rf ./${NAME}-${VERSION}/
