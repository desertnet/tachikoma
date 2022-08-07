FROM debian:bullseye-slim

ARG CONFIG=default
ARG TACHIKOMA_UID=1000

RUN apt-get update && \
    apt-get install -y \
        vim less procps \
        git build-essential \
        libberkeleydb-perl \
        libcgi-pm-perl \
        libcrypt-openssl-rsa-perl \
        libdevice-serialport-perl \
        libdbd-sqlite3-perl \
        libdbi-perl \
        libio-socket-ssl-perl \
        libjson-perl \
        libnet-ssleay-perl \
        libterm-readline-gnu-perl \
        libwww-perl \
    && rm -rf /var/lib/apt/lists/*

RUN    useradd -s /bin/bash -u ${TACHIKOMA_UID} -d /home/tachikoma -M tachikoma \
    && usermod -aG adm tachikoma

WORKDIR /usr/src
RUN git clone https://github.com/datapoke/tachikoma

WORKDIR /usr/src/tachikoma
RUN    bin/install_tachikoma \
    && rm -f /home/tachikoma/.tachikoma/run/*
COPY ./docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["docker-entrypoint.sh"]
