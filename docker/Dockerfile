FROM debian:bookworm-slim

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
        perl-doc \
    && rm -rf /var/lib/apt/lists/*
#        libdevice-serialport-perl \

# add a dir for tachikoma
RUN mkdir /home/tachikoma

# default directory for shell access
WORKDIR /home/tachikoma

# copy entrypoint script
COPY ./docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["docker-entrypoint.sh"]
