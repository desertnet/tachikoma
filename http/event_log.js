var server_host       = window.location.hostname;
var server_port       = window.location.port;
var server_path       = "/cgi-bin/tail.cgi"
var topic             = "event_log";
var parsed_url        = new URL(window.location.href);
var count             = parsed_url.searchParams.get("count")    || 100;
var interval          = parsed_url.searchParams.get("interval") || 33;
var xhttp             = null;
var fetch_timers      = [];
var display_timer     = null;
var output            = [];
var dirty             = 1;
var show_event_output = 1;

function start_timer() {
    var prefix_url = "https://" + server_host + ":" + server_port
                   + server_path + "/"
                   + topic;
    var last_url   = prefix_url  + "/last/"   + count;
    var server_url = last_url;
    xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var msg = JSON.parse(this.responseText);
            if (!msg.next_url || msg.next_url == server_url) {
                fetch_timer = setTimeout(tick, 1000, server_url);
                if (msg.next_url == server_url) {
                    update_table(msg);
                }
            }
            else {
                server_url  = msg.next_url;
                fetch_timer = setTimeout(tick, 0, server_url);
                update_table(msg);
            }
        }
        else if (this.readyState == 4) {
            fetch_timer = setTimeout(tick, 1000, server_url);
        }
    };
    fetch_timer   = setTimeout(tick, 100, server_url);
    display_timer = setInterval(display_table, interval);
}

function update_table(msg) {
    for (var i = 0; i < msg.payload.length; i++) {
        var tr    = "";
        var date  = new Date();
        var queue = msg.payload[i].queue || "";
        var type  = msg.payload[i].type;
        date.setTime(
            ( msg.payload[i].timestamp - date.getTimezoneOffset() * 60 )
            * 1000
        );
        if (msg.payload[i].type == "TASK_ERROR") {
            tr = "<tr bgcolor=\"#FF9999\">";
        }
        else if (msg.payload[i].type == "TASK_OUTPUT") {
            if (show_event_output) {
                tr = "<tr bgcolor=\"#99FF99\">";
            }
            else {
                continue;
            }
        }
        else if (msg.payload[i].type == "TASK_BEGIN"
              || msg.payload[i].type == "TASK_COMPLETE") {
            tr = "<tr bgcolor=\"#DDDDDD\">";
        }
        else {
            tr = "<tr>";
        }
        var key_href = "<a href=\"event_query.html?key="
                     + msg.payload[i].key + "\">"
                     + msg.payload[i].key + "</a>";
        var value = msg.payload[i].value || "";
        var escaped = String(value).replace(/</g,"&lt;").replace(/&/g,"&amp;");
        var row = tr + "<td>" + date_string(date) + "</td>"
                     + "<td>" + queue             + "</td>"
                     + "<td>" + type              + "</td>"
                     + "<td>" + key_href          + "</td>"
                     + "<td>" + escaped           + "</td></tr>";
        output.unshift(row);
    }
    while (output.length > count) {
        output.pop();
    }
    dirty = msg.payload.length;
}

function display_table() {
    if (dirty) {
        while (output.length > count) {
            output.pop();
        }
        document.getElementById("output").innerHTML
                    = "<table>"
                    + "<tr><th>TIMESTAMP</th>"
                    + "<th>QUEUE</th>"
                    + "<th>TYPE</th>"
                    + "<th>KEY</th>"
                    + "<th>VALUE</th></tr>"
                    + output.join("")
                    + "</table>";
        dirty = 0;
    }
}

function toggle_event_output() {
    if (show_event_output) {
        show_event_output = 0;
        document.getElementById("toggle").innerHTML = "show output";
    }
    else {
        show_event_output = 1;
        document.getElementById("toggle").innerHTML = "hide output";
    }
    output = [];
    // update_table();
    dirty = 1;
    display_table();
    // output = [];
}

function tick(server_url) {
    xhttp.open("GET", server_url, true);
    xhttp.send();
}

function date_string(date) {
    return date.toISOString().replace(/T/, "&nbsp;").replace(/Z/, "");
}
