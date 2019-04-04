var server_url  = "http://" + window.location.hostname
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
                    tr = "<tr bgcolor=\"#FF9999\">";
                }
                else if (msg[i].attempts == 1) {
                    tr = "<tr bgcolor=\"#99FF99\">";
                }
                else {
                    tr = "<tr bgcolor=\"#DDDDDD\">";
                }
                var key_href = "<a href=\"http://" + server_name
                             + ":4242/task_query.html?key="
                             + msg[i].message_stream + "\">"
                             + msg[i].message_stream + "</a>";
                var row = tr + "<td>" + date.toISOString()      + "</td>"
                             + "<td>" + key_href                + "</td>"
                             + "<td>" + msg[i].message_payload  + "</td>"
                             + "<td>" + msg[i].attempts         + "</td>"
                             + "<td>" + next_date.toISOString() + "</td></tr>";
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
            timer = setTimeout(tick, 2000);
        }
    };
    timer = setTimeout(tick, 0);
}

function tick() {
    xhttp.open("GET", server_url, true);
    xhttp.send();
}
