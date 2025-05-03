#!/bin/bash
if [ -n "$1" ] ; then
    exec $@
fi

if [ "${TACHIKOMA_UID}" = '' ]; then
    TACHIKOMA_UID=1000
fi

# make user account
useradd -s /bin/bash -u ${TACHIKOMA_UID} -d /home/tachikoma tachikoma

# copy source
cp -rfT /services/tachikoma /usr/src/tachikoma

# make tachikoma log dirs
mkdir -p /home/tachikoma/.tachikoma/log
chown -R ${PUB_ID}:${PUB_ID} /home/tachikoma/.tachikoma/log
cd /home/tachikoma/.tachikoma/log

# install tachikoma
cd /usr/src/tachikoma \
    && bin/install_tachikoma

# install tachikoma user configuration
cd /usr/src/tachikoma/
su ${PUB_ID} -c "bin/install_tachikoma_user ${CONFIG}"

# clear stale pid files
rm -f /home/tachikoma/.tachikoma/run/*

# start service
exec su - ${PUB_ID} -c "/usr/local/bin/tachikoma-server --daemon=no"
