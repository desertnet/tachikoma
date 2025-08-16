const parsed_url = new URL(window.location.href);
const shell_path = window.location.pathname;
const xhttp = new XMLHttpRequest();
const queue = parsed_url.searchParams.get("queue");
const interval = parsed_url.searchParams.get("interval") || 2000;
let server_url = "fetch";
let timer = null;
if (queue) {
    server_url += "/" + queue;
}

function start_timer() {
    xhttp.onreadystatechange = function () {
        if (this.readyState == 4 && this.status == 200) {
            const msg = JSON.parse(this.responseText);
            if (queue) {
                display_queue(msg);
            }
            else {
                display_queues(msg);
            }
        }
        if (this.readyState == 4) {
            timer = setTimeout(tick, interval);
        }
    };
    timer = setTimeout(tick, 0);
}

function tick() {
    xhttp.open("GET", server_url, true);
    xhttp.send();
}

function display_queue(msg) {
    let output = [];
    let footer = "";
    for (let i = 0; i < msg.length && i < 1000; i++) {
        let next_date = new Date();
        let date = new Date();
        let tr = "";
        date.setTime(
            (msg[i].message_timestamp - date.getTimezoneOffset() * 60)
            * 1000
        );
        next_date.setTime(
            (msg[i].next_attempt - date.getTimezoneOffset() * 60)
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
        const payload = msg[i].message_payload || "";
        const escaped = payload.replace(/&/g, "&amp;").replace(/</g, "&lt;");
        const row = tr + "<td>" + date_string(date) + "</td>"
            + "<td>" + msg[i].message_stream + "</td>"
            + "<td>" + escaped + "</td>"
            + "<td>" + msg[i].attempts + "</td>"
            + "<td>" + date_string(next_date) + "</td></tr>";
        output.push(row);
    }
    if (msg.length == 0) {
        footer = "<em>-empty-</em>";
    }
    document.getElementById("output").innerHTML
        = "<table>"
        + "<tr><th>timestamp</th>"
        + "<th>key</th>"
        + "<th>value</th>"
        + "<th># attempts</th>"
        + "<th>next attempt</th></tr>"
        + output.join("")
        + "</table>"
        + footer;
}

function display_queues(msg) {
    let output = [];
    let footer = "";
    msg.sort(function (a, b) {
        return b.size - a.size;
    });
    for (let i = 0; i < msg.length && i < 1000; i++) {
        const key_href = "<a href=\"" + shell_path + "?queue="
            + msg[i].name + "\">"
            + msg[i].name + "</a>";
        let tr = "";
        if (msg[i].size > 1000) {
            tr = "<tr bgcolor=\"#622\">";
        }
        else if (msg[i].size > 0) {
            tr = "<tr bgcolor=\"#262\">";
        }
        else {
            tr = "<tr bgcolor=\"#444\">";
        }
        const row = tr + "<td>" + key_href + "</td>"
            + "<td>" + msg[i].size + "</td></tr>";
        output.push(row);
    }
    if (msg.length == 0) {
        footer = "<em>-none-</em>";
    }
    document.getElementById("output").innerHTML
        = "<table>"
        + "<tr><th>QUEUE</th>"
        + "<th>SIZE</th></tr>"
        + output.join("")
        + "</table>"
        + footer;
}

function date_string(date) {
    return date.toISOString().replace(/T/, "&nbsp;").replace(/Z/, "");
}
