FROM debian:bullseye-slim

ARG CONFIG=default

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

WORKDIR /usr/src
RUN git clone https://github.com/datapoke/tachikoma
RUN cp ./tachikoma/docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["docker-entrypoint.sh"]
