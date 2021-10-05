#!/bin/sh
curl --netrc --tls-max 1.2 --data-binary '{
    "op" : "keys",
    "field" : "server_log.'$1':index"
}' \
-H "Content-Type: application/json" \
-X POST https://localhost:4242/cgi-bin/query.cgi/server_log
