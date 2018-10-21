#!/bin/sh
curl --netrc --data-binary '{
    "op" : "keys",
    "field" : "server_log.'$1':index"
}' \
-H "Content-Type: application/json" \
-X POST http://localhost:4242/cgi-bin/query.cgi/server_log
