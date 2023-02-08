#!/bin/bash
if [ -n "$1" ] ; then
    exec $@
fi

cd /usr/src/tachikoma/
su tachikoma -c "bin/install_tachikoma_user ${CONFIG}"

rm -f /home/tachikoma/.tachikoma/run/*

exec su - tachikoma -c "/usr/local/bin/tachikoma-server --daemon=no"
