#!/bin/sh
curl --netrc --data-binary '[
    {
        "field" : "server_log.timestamp:index",
        "op" : "ge",
        "key" : '$(date -v-1H +'%s')'
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
