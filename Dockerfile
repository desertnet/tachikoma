FROM debian:buster

ARG CONFIG=default

RUN apt-get update && \
    apt-get install -y \
        git build-essential \
        libwww-perl \
        libcrypt-openssl-rsa-perl \
        libnet-ssleay-perl \
        libio-socket-ssl-perl \
        libberkeleydb-perl \
        libdbi-perl \
        libdbd-sqlite3-perl \
        libjson-perl \
        libcgi-pm-perl \
        libterm-readline-gnu-perl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src
RUN git clone https://github.com/datapoke/tachikoma

WORKDIR /usr/src/tachikoma
RUN bin/install_tachikoma ${CONFIG}

CMD [ "/usr/local/bin/tachikoma-server", "--daemon=no" ]
