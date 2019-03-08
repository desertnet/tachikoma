var start_url        = "http://" + window.location.hostname
                     + ":4242/cgi-bin/topic.cgi/tasks/0/recent/500";
var server_url       = start_url;
var xhttp            = new XMLHttpRequest();
var timer            = null;
var cached_msg       = [];
var output           = [];
var dirty            = 1;
var show_task_output = 1;

function start_timer() {
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var msg = JSON.parse(this.responseText);
            if (!msg.next_url || msg.next_url == server_url) {
                // console.log(server_url);
                timer = setTimeout(tick, 2000);
                if (dirty) {
                    display_table();
                }
            }
            else {
                cached_msg = msg;
                server_url = msg.next_url;
                timer      = setTimeout(tick, 0);
                update_table();
            }
        }
        else if (this.readyState == 4) {
            timer = setTimeout(tick, 2000);
        }
    };
    timer = setTimeout(tick, 0);
}

function update_table() {
    for (var i = 0; i < cached_msg.payload.length; i++) {
        var tr      = "";
        var date    = new Date();
        var type    = cached_msg.payload[i].type;
        date.setTime( cached_msg.payload[i].timestamp * 1000 );
        if (cached_msg.payload[i].type == "TASK_ERROR") {
            tr = "<tr bgcolor=\"#FF9999\">";
        }
        else if (cached_msg.payload[i].type == "TASK_OUTPUT") {
            if (show_task_output) {
                tr = "<tr bgcolor=\"#99FF99\">";
            }
            else {
                continue;
            }
        }
        else if (cached_msg.payload[i].type == "TASK_BEGIN"
              || cached_msg.payload[i].type == "TASK_COMPLETE") {
            tr = "<tr bgcolor=\"#DDDDDD\">";
        }
        else {
            tr = "<tr>";
        }
        var key_href = "<a href=\"task_query.html?key="
                     + cached_msg.payload[i].key + "\">"
                     + cached_msg.payload[i].key + "</a>";
        var row = tr + "<td>" + date.toISOString() + "</td>"
                     + "<td>" + type               + "</td>"
                     + "<td>" + key_href           + "</td>";
        if (show_task_output) {
            var payload = cached_msg.payload[i].payload || "";
            row += "<td>" + payload + "</td></tr>";
        }
        else {
            row += "</tr>";
        }
        output.unshift(row);
    }
    while (output.length > 1000) {
        output.pop();
    }
    dirty = 1;
}

function display_table() {
    var row = "<tr><th>TIMESTAMP</th>"
            + "<th>TYPE</th>"
            + "<th>KEY</th>";
    if (show_task_output) {
        row += "<th>VALUE</th></tr>";
    }
    else {
        row += "</tr>";
    }
    document.getElementById("output").innerHTML
                = "<table>"
                + row
                + output.join("")
                + "</table>";
    dirty = 0;
}

function toggle_task_output() {
    if (show_task_output) {
        show_task_output = 0;
        document.getElementById("toggle").innerHTML = "show output";
    }
    else {
        show_task_output = 1;
        document.getElementById("toggle").innerHTML = "hide output";
    }
    server_url = start_url;
    output     = [];
    timer      = setTimeout(tick, 0);
}

function tick() {
    xhttp.open("GET", server_url, true);
    xhttp.send();
}
