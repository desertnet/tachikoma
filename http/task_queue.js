var server_url  = window.location.protocol + "//"
                      + window.location.hostname
                      + ":" + window.location.port
                      + "/fetch/tasks:queue";
var server_name = window.location.hostname;
var xhttp       = new XMLHttpRequest();
var timer       = null;

function start_timer() {
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var msg    = JSON.parse(this.responseText);
            var output = [];
            var footer = "";
            for (var i = 0; i < msg.length && i < 1000; i++) {
                var tr        = "";
                var date      = new Date();
                var next_date = new Date();
                date.setTime(
                    ( msg[i].message_timestamp - date.getTimezoneOffset() * 60 )
                    * 1000
                );
                next_date.setTime(
                    ( msg[i].next_attempt - date.getTimezoneOffset() * 60 )
                    * 1000
                );
                if (msg[i].attempts > 1) {
                    tr = "<tr bgcolor=\"#622\">";
                }
                else if (msg[i].attempts == 1) {
                    tr = "<tr bgcolor=\"#262\">";
                }
                else {
                    tr = "<tr bgcolor=\"#444\">";
                }
                var key_href = "<a href=\"" + window.location.protocol + "//"
                             + server_name + ":4242/task_query.html?key="
                             + msg[i].message_stream + "\">"
                             + msg[i].message_stream + "</a>";
                var payload = msg[i].message_payload;
                var escaped = payload.replace(/</g,"&lt;").replace(/&/g,"&amp;");
                var row = tr + "<td>" + date_string(date)       + "</td>"
                             + "<td>" + key_href                + "</td>"
                             + "<td>" + escaped                 + "</td>"
                             + "<td>" + msg[i].attempts         + "</td>"
                             + "<td>" + date_string(next_date)  + "</td></tr>";
                output.push(row);
            }
            if (msg.length == 0) {
                footer = "<em>-empty-</em>";
            }
            document.getElementById("output").innerHTML
                        = "<table>"
                        + "<tr><th>TIMESTAMP</th>"
                        + "<th>KEY</th>"
                        + "<th>VALUE</th>"
                        + "<th># ATTEMPTS</th>"
                        + "<th>NEXT ATTEMPT</th></tr>"
                        + output.join("")
                        + "</table>"
                        + footer;
        }
        if (this.readyState == 4) {
            timer = setTimeout(tick, 1000);
        }
    };
    timer = setTimeout(tick, 0);
}

function tick() {
    xhttp.open("GET", server_url, true);
    xhttp.send();
}

function date_string(date) {
    return date.toISOString().replace(/T/, "&nbsp;").replace(/Z/, "");
}
