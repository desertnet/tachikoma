var server_path      = "cgi-bin/tail.cgi"
var topic            = "tasks";
var parsed_url       = new URL(window.location.href);
var count            = parsed_url.searchParams.get("count")    || 1000;
var interval         = parsed_url.searchParams.get("interval") || 33;
var prefix_url       = server_path + "/" + topic;
var last_url         = prefix_url  + "/last/"   + count;
var recent_url       = prefix_url  + "/recent/" + count;
var server_url       = last_url;
var xhttp            = new XMLHttpRequest();
var fetch_timer      = null;
var display_timer    = null;
var output           = [];
var dirty            = 1;
var show_task_output = 1;

function start_timer() {
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var msg = JSON.parse(this.responseText);
            if (!msg.next_url || msg.next_url == server_url) {
                fetch_timer = setTimeout(tick, 1000);
                if (msg.next_url == server_url) {
                    update_table(msg);
                }
            }
            else {
                server_url  = msg.next_url;
                fetch_timer = setTimeout(tick, 0);
                update_table(msg);
            }
        }
        else if (this.readyState == 4) {
            fetch_timer = setTimeout(tick, 1000);
        }
    };
    fetch_timer   = setTimeout(tick, 100);
    display_timer = setInterval(display_table, interval);
}

function update_table(msg) {
    for (var i = 0; i < msg.payload.length; i++) {
        var tr   = "";
        var date = new Date();
        var type = msg.payload[i].type;
        date.setTime(
            ( msg.payload[i].timestamp - date.getTimezoneOffset() * 60 )
            * 1000
        );
        if (msg.payload[i].type == "TASK_ERROR") {
            tr = '<tr class="task-error">';
        }
        else if (msg.payload[i].type == "TASK_OUTPUT") {
            if (show_task_output) {
                tr = '<tr class="task-output">';
            }
            else {
                continue;
            }
        }
        else if (msg.payload[i].type == "TASK_BEGIN"
              || msg.payload[i].type == "TASK_COMPLETE") {
            tr = '<tr class="task-begin-complete">';
        }
        else {
            tr = "<tr>";
        }
        var key_href = "<a href=\"task_query.html?key="
                     + msg.payload[i].key + "\">"
                     + msg.payload[i].key + "</a>";
        var value = msg.payload[i].value
                 || msg.payload[i].payload
                 || "";
        var escaped = String(value).replace(/&/g,"&amp;").replace(/</g,"&lt;");
        var row = tr + "<td>" + date_string(date) + "</td>"
                     + "<td>" + type              + "</td>"
                     + "<td>" + key_href          + "</td>"
                     + "<td>" + escaped           + "</td></tr>";
        output.unshift(row);
    }
    while (output.length > count) {
        output.pop();
    }
    dirty = 1;
}

function display_table() {
    if (dirty) {
        while (output.length > count) {
            output.pop();
        }
        document.getElementById("output").innerHTML
                    = "<table>"
                    + "<tr><th>timestamp</th>"
                    + "<th>type</th>"
                    + "<th>key</th>"
                    + "<th>value</th></tr>"
                    + output.join("")
                    + "</table>";
        dirty = 0;
    }
}

function toggle_task_output() {
    if (show_task_output) {
        show_task_output = 0;
        document.getElementById("toggle").innerHTML = "show output";
        clearTimeout(fetch_timer);
        server_url  = recent_url;
        fetch_timer = setTimeout(tick, 0);
    }
    else {
        show_task_output = 1;
        document.getElementById("toggle").innerHTML = "hide output";
        clearTimeout(fetch_timer);
        server_url  = last_url;
        fetch_timer = setTimeout(tick, 0);
    }
    output = [];
    update_table();
    display_table();
    output = [];
}

function tick() {
    xhttp.open("GET", server_url, true);
    xhttp.send();
}

function date_string(date) {
    return date.toISOString().replace(/T/, "&nbsp;").replace(/Z/, "");
}
