var topic       = "tasks";
var field       = "tasks.ID:index";
var serverUrl   = "http://" + window.location.hostname + ":4242/cgi-bin/query.cgi/" + topic;
var xhttp       = new XMLHttpRequest();
var parsed_url  = new URL(window.location.href);
var key         = parsed_url.searchParams.get("key");
var timer       = null;
var data        = {};
if (key) {
    data = {
        "field": field,
        "op":    "eq",
        "key":   key
    };
}
else {
    data = {
        "field": field,
        "op":    "keys",
        "key":   ""
    };
}
_execute_query();

function render_form() {
    var form_html = '<form onsubmit="execute_query(); return false;" id="query_params">'
        + '<input name="key"/>'
        + '<button>search</button>'
        + '</form>';
    document.getElementById("query_form").innerHTML = form_html;
}

function execute_query() {
    data = {};
    var form = document.getElementById("query_params");
    for (var i = 0, l = form.length; i < l; ++i) {
        var input = form[i];
        if (input.name) {
            data[input.name] = input.value;
        }
    }
    data["field"] = field;
    if (data["key"]) {
        data["op"] = "eq";
    }
    else {
        data["op"] = "keys";
    }
    _execute_query();
}

function _execute_query() {
    xhttp.addEventListener("progress", updateProgress);
    if (data["op"] == "keys") {
        xhttp.onreadystatechange = function() {
            if (this.readyState == 4 && this.status == 200) {
                var output = [];
                var msg    = JSON.parse(this.responseText);
                for (var k in msg[0]) {
                    output.push(
                        "<a href=\"task_query.html?key=" + k + "\">"
                        + k + "</a><br>"
                    );
                }
                document.getElementById("output").innerHTML = output.join("");
            }
            else if (this.readyState == 4) {
                timer = setTimeout(tick, 2000);
            }
        };
    }
    else {
        xhttp.onreadystatechange = function() {
            var output = [];
            if (this.readyState == 4 && this.status == 200) {
                if (this.responseText) {
                    var msg     = JSON.parse(this.responseText);
                    var running = 1;
                    if (msg[0].error) {
                        document.getElementById("output").innerHTML = "<em>"
                            + msg[0].error + "</em>";
                    }
                    else {
                        msg.sort(function(a, b) {
                            return a.value.timestamp - b.value.timestamp;
                        });
                        for (var i = 0; i < msg.length; i++) {
                            var tr      = "";
                            var ev      = msg[i].value;
                            var date    = new Date();
                            var payload = ev.payload || "";
                            date.setTime( ev.timestamp * 1000 );
                            if (ev.type == "TASK_ERROR") {
                                tr = "<tr bgcolor=\"#FF9999\">";
                            }
                            else if (ev.type == "TASK_OUTPUT") {
                                tr = "<tr bgcolor=\"#99FF99\">";
                            }
                            else if (ev.type == "TASK_BEGIN"
                                  || ev.type == "TASK_COMPLETE") {
                                tr = "<tr bgcolor=\"#DDDDDD\">";
                            }
                            else {
                                tr = "<tr>";
                            }
                            var row = tr + "<td>" + date.toISOString() + "</td>"
                                         + "<td>" + ev.type            + "</td>"
                                         + "<td>" + ev.key             + "</td>"
                                         + "<td>" + payload            + "</td></tr>";
                            output.push(row);
                            if (ev.type == "MSG_CANCELED") {
                                running = 0;
                            }
                        }
                        while (output.length > 1000) {
                            output.shift();
                        }
                        document.getElementById("output").innerHTML
                                    = "<table>"
                                    + "<tr><th>TIMESTAMP</th>"
                                    + "<th>TYPE</th>"
                                    + "<th>KEY</th>"
                                    + "<th>VALUE</th></tr>"
                                    + output.join("")
                                    + "</table>";
                    }
                    if (running) {
                        timer = setTimeout(tick, 2000);
                    }
                }
                else {
                    document.getElementById("output").innerHTML = "<em>no results</em>";
                }
            }
            else if (this.readyState == 4) {
                timer = setTimeout(tick, 2000);
            }
        };
    }
    timer = setTimeout(tick, 0);
}

function updateProgress (oEvent) {
    document.getElementById("output").innerHTML = "<pre>loaded " + oEvent.loaded + " bytes</pre>";
}

function tick() {
    xhttp.open("POST", serverUrl, true);
    xhttp.setRequestHeader("Content-Type", "application/json; charset=UTF-8");
    var json_data = JSON.stringify(data)
    console.log(json_data);
    xhttp.send(json_data);
}
