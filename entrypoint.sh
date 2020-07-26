#!/bin/bash

cd /usr/src/tachikoma
bin/dock_tachikoma ${CONFIG}

rm /root/.tachikoma/run/*

exec /usr/local/bin/tachikoma-server --daemon=no
