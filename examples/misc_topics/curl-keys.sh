#!/bin/sh
index=$1
if [ -z "$index" ] ; then
    index=hostname
fi
curl --netrc --tls-max 1.2 --data-binary '{
    "op" : "keys",
    "field" : "server_log.'$index':index"
}' \
-H "Content-Type: application/json" \
-X POST http://localhost:4242/cgi-bin/query.cgi/server_log
