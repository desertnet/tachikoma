FROM debian:bullseye-slim

ARG CONFIG=default
ARG TACHIKOMA_UID=1000

RUN apt-get update && \
    apt-get install -y \
        build-essential \
        libberkeleydb-perl \
        libcgi-pm-perl \
        libcrypt-openssl-rsa-perl \
        libdbd-sqlite3-perl \
        libdbi-perl \
        libio-socket-ssl-perl \
        libjson-perl \
        libnet-ssleay-perl \
        libterm-readline-gnu-perl \
        libwww-perl \
    && rm -rf /var/lib/apt/lists/*
#        libdevice-serialport-perl \

RUN useradd -s /bin/bash -u ${TACHIKOMA_UID} -d /home/tachikoma -m tachikoma

# NOTE: add your own configs to etc/scripts/local
#       and use --env CONFIG=local

COPY ./                     /usr/src/tachikoma
COPY ./docker-entrypoint.sh /usr/local/bin/

RUN    cd /usr/src/tachikoma \
    && bin/install_tachikoma

ENTRYPOINT ["docker-entrypoint.sh"]
