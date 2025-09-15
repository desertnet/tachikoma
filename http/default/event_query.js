const parsed_url = new URL(window.location.href);
const topic = "event_log";
const field = "event_log.ID:index";
const server_url = "cgi-bin/query.cgi/" + topic;
const key = parsed_url.searchParams.get("key");
const xhttp = new XMLHttpRequest();
let timer = null;
let data = {};
if (key) {
    data = {
        "field": field,
        "op": "eq",
        "key": key
    };
    _execute_query();
}

function render_form() {
    const form_html = '<form onsubmit="execute_query(); return false;" id="query_params">'
        + '<input name="key" class="uk-input uk-form-width-large uk-margin-top"/>'
        + '<button class="uk-button uk-button-primary uk-margin-top uk-margin-left">search</button>'
        + '</form>';
    document.getElementById("query_form").innerHTML = form_html;
}

function execute_query() {
    const form = document.getElementById("query_params");
    data = {};
    for (let i = 0, l = form.length; i < l; ++i) {
        const input = form[i];
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
    if (data["op"] == "keys") {
        xhttp.onreadystatechange = function () {
            if (this.readyState == 4 && this.status == 200) {
                const msg = JSON.parse(this.responseText);
                let output = [];
                for (const k in msg[0]) {
                    output.push(
                        "<a href=\"event_query.html?key=" + k + "\">"
                        + k + "</a><br>"
                    );
                }
                document.getElementById("output").innerHTML = output.join("");
            }
            else if (this.readyState == 4) {
                timer = setTimeout(tick, 1000);
            }
        };
    }
    else {
        xhttp.onreadystatechange = function () {
            let output = [];
            if (this.readyState == 4 && this.status == 200) {
                if (this.responseText) {
                    const msg = JSON.parse(this.responseText);
                    let running = 1;
                    if (!msg[0]) {
                        document.getElementById("output").innerHTML = "<em> - no results - </em>";
                    }
                    else if (msg[0].error) {
                        document.getElementById("output").innerHTML = "<em>"
                            + msg[0].error + "</em>";
                    }
                    else {
                        msg.sort(function (a, b) {
                            return a.value.timestamp - b.value.timestamp;
                        });
                        for (let i = 0; i < msg.length; i++) {
                            const ev = msg[i].value;
                            const queue = ev.queue || "";
                            const value = ev.value || "";
                            const escaped = String(value).replace(/&/g, "&amp;").replace(/</g, "&lt;");
                            let date = new Date();
                            let tr = "";
                            date.setTime(
                                (ev.timestamp - date.getTimezoneOffset() * 60)
                                * 1000
                            );
                            if (ev.type == "TASK_ERROR") {
                                tr = '<tr class="task-error">';
                            }
                            else if (ev.type == "TASK_OUTPUT") {
                                tr = '<tr class="task-output">';
                            }
                            else if (ev.type == "TASK_BEGIN"
                                || ev.type == "TASK_COMPLETE") {
                                tr = '<tr class="task-begin-complete">';
                            }
                            else {
                                tr = '<tr>';
                            }
                            const row = tr + "<td>" + date_string(date) + "</td>"
                                + "<td>" + queue + "</td>"
                                + "<td>" + ev.type + "</td>"
                                + "<td>" + ev.key + "</td>"
                                + "<td>" + escaped + "</td></tr>";
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
                            + "<tr><th>timestamp</th>"
                            + "<th>queue</th>"
                            + "<th>type</th>"
                            + "<th>key</th>"
                            + "<th>value</th></tr>"
                            + output.join("")
                            + "</table>";
                    }
                    if (running) {
                        timer = setTimeout(tick, 1000);
                    }
                }
                else {
                    document.getElementById("output").innerHTML = "<em>no results</em>";
                }
            }
            else if (this.readyState == 4) {
                timer = setTimeout(tick, 1000);
            }
        };
    }
    timer = setTimeout(tick, 0);
}

function tick() {
    const json_data = JSON.stringify(data)
    xhttp.open("POST", server_url, true);
    xhttp.setRequestHeader("Content-Type", "application/json; charset=UTF-8");
    console.log(json_data);
    xhttp.send(json_data);
}

function date_string(date) {
    return date.toISOString().replace(/T/, "&nbsp;").replace(/Z/, "");
}
