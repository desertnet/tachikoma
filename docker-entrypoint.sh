#!/bin/bash
if [ -n "$1" ] ; then
    exec $@
fi

cd /usr/src/tachikoma
bin/install_tachikoma ${CONFIG}

rm -f /root/.tachikoma/run/*

exec /usr/local/bin/tachikoma-server --daemon=no
