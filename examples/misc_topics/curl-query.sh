#!/bin/sh
curl --netrc --tls-max 1.2 --data-binary '[
    {
        "field" : "server_log.timestamp:index",
        "op" : "ge",
        "key" : '$(date --date '1 hour ago' +'%s')'
    },
    {
        "field" : "server_log.hostname:index",
        "op" : "re",
        "key" : "'$1'"
    },
    {
        "field" : "server_log.process:index",
        "op" : "re",
        "key" : "'$2'"
    }
]' \
-H "Content-Type: application/json" \
-X POST http://localhost:4242/cgi-bin/query.cgi/server_log
