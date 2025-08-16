const server_url = "fetch/tasks:queue";
const xhttp = new XMLHttpRequest();
const timer = null;

function start_timer() {
    xhttp.onreadystatechange = function () {
        if (this.readyState == 4 && this.status == 200) {
            const msg = JSON.parse(this.responseText);
            let output = [];
            let footer = "";
            for (let i = 0; i < msg.length && i < 1000; i++) {
                const next_date = new Date();
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
                const key_href = "<a href=\"task_query.html?key="
                    + msg[i].message_stream + "\">"
                    + msg[i].message_stream + "</a>";
                const payload = msg[i].message_payload;
                const escaped = payload.replace(/&/g, "&amp;").replace(/</g, "&lt;");
                const row = tr + "<td>" + date_string(date) + "</td>"
                    + "<td>" + key_href + "</td>"
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
