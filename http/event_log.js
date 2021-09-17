var server_host       = window.location.hostname;
var server_port       = window.location.port;
var server_path       = "/cgi-bin/topic.cgi"
var topic             = "event_log";
var _num_partitions   = num_partitions || 1;
var parsed_url        = new URL(window.location.href);
var count             = parsed_url.searchParams.get("count")    || 100;
var interval          = parsed_url.searchParams.get("interval") || 33;
var xhttp             = [];
var fetch_timers      = [];
var display_timer     = null;
var output            = [];
var dirty             = 1;
var show_event_output = 1;

function start_timer() {
    for (var i = 0; i < _num_partitions; i++) {
        start_partition(i);
    }
    display_timer = setInterval(display_table, interval);
}

function start_partition(partition) {
    var prefix_url   = "https://" + server_host + ":" + server_port
                     + server_path + "/"
                     + topic       + "/"
                     + partition   + "/";
    var last_url     = prefix_url  + "/last/"   + count;
    var recent_url   = prefix_url  + "/recent/" + count;
    var server_url   = last_url;
    xhttp[partition] = new XMLHttpRequest();
    xhttp[partition].onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var msg = JSON.parse(this.responseText);
            if (!msg.next_url || msg.next_url == server_url) {
                fetch_timers[partition] = setTimeout(tick,
                                                     1000,
                                                     partition,
                                                     server_url);
                if (msg.next_url == server_url) {
                    update_table(msg);
                }
            }
            else {
                server_url = msg.next_url;
                fetch_timers[partition] = setTimeout(tick,
                                                     0,
                                                     partition,
                                                     server_url);
                update_table(msg);
            }
        }
        else if (this.readyState == 4) {
            fetch_timers[partition] = setTimeout(tick,
                                                 1000,
                                                 partition,
                                                 server_url);
        }
    };
    fetch_timers[partition] = setTimeout(tick,
                                         100,
                                         partition,
                                         server_url);
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
        var escaped = value.replace(/</g,"&lt;").replace(/&/g,"&amp;");
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

function tick(partition, server_url) {
    xhttp[partition].open("GET", server_url, true);
    xhttp[partition].send();
}

function date_string(date) {
    return date.toISOString().replace(/T/, " ").replace(/Z/, " ");
}
