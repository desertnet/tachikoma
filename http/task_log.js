var serverUrl = "http://" + window.location.hostname + ":4242/cgi-bin/topic.cgi/tasks/0/recent/500";
var xhttp     = new XMLHttpRequest();
var timer     = null;
var output    = [];

function start_timer() {
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var msg = JSON.parse(this.responseText);
            if (!msg.next_url || msg.next_url == serverUrl) {
                // console.log(serverUrl);
                timer = setTimeout(tick, 2000);
            }
            else {
                for (var i = 0; i < msg.payload.length; i++) {
                    var tr      = "";
                    var date    = new Date();
                    var payload = msg.payload[i].payload || "";
                    date.setTime( msg.payload[i].timestamp * 1000 );
                    if (msg.payload[i].type == "TASK_ERROR") {
                        tr = "<tr bgcolor=\"#FF9999\">";
                    }
                    else if (msg.payload[i].type == "TASK_OUTPUT") {
                        tr = "<tr bgcolor=\"#99FF99\">";
                    }
                    else if (msg.payload[i].type == "TASK_BEGIN"
                          || msg.payload[i].type == "TASK_COMPLETE") {
                        tr = "<tr bgcolor=\"#DDDDDD\">";
                    }
                    else {
                        tr = "<tr>";
                    }
                    var key_href = "<a href=\"task_query.html?key="
                                 + msg.payload[i].key + "\">"
                                 + msg.payload[i].key + "</a>";
                    var row = tr + "<td>" + date.toISOString()  + "</td>"
                                 + "<td>" + msg.payload[i].type + "</td>"
                                 + "<td>" + key_href            + "</td>"
                                 + "<td>" + payload             + "</td></tr>";
                    output.unshift(row);
                }
                while ( output.length > 500 ) {
                    output.pop();
                }
                document.getElementById("output").innerHTML
                            = "<table>"
                            + "<tr><th>TIMESTAMP</th>"
                            + "<th>TYPE</th>"
                            + "<th>KEY</th>"
                            + "<th>VALUE</th></tr>"
                            + output.join("")
                            + "</table>";
                serverUrl = msg.next_url;
                timer = setTimeout(tick, 0);
            }
        }
        else if (this.readyState == 4) {
            timer = setTimeout(tick, 2000);
        }
    };
    timer = setTimeout(tick, 0);
}

function tick() {
    xhttp.open("GET", serverUrl, true);
    xhttp.send();
}
