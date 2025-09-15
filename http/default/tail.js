const parsed_url = new URL(window.location.href);
const server_path = "cgi-bin/tail.cgi"
const _topic = parsed_url.searchParams.get("topic") || topic;
const _offset = parsed_url.searchParams.get("offset") || offset;
const _count = parsed_url.searchParams.get("count") || count;
const _interval = parsed_url.searchParams.get("interval") || interval;
let xhttp = null;
let fetch_timer = null;
let display_timer = null;
let output = [];
let dirty = 1;

function init() {
    document.getElementById("toggle").addEventListener("click", playOrPause);
    start_timer();
}

function start_timer() {
    if (_topic) {
        start_tail();
        display_timer = setInterval(display_table, _interval);
    }
    else {
        document.getElementById("toggle").innerHTML = "error";
        document.getElementById("output").innerHTML = '<pre class="uk-dark">no topic</pre>';
    }
}

function start_tail() {
    const prefix_url = server_path + "/" + _topic;
    let server_url = prefix_url + "/" + _offset + "/" + _count;
    if (double_encode) {
        server_url += "/1";
    }
    xhttp = new XMLHttpRequest();
    // xhttp.timeout = 15000;
    xhttp.onreadystatechange = function () {
        if (this.readyState == 4) {
            if (this.status == 200) {
                const msg = JSON.parse(this.responseText);
                if (msg.next_url) {
                    if (msg.next_url == server_url) {
                        update_table(msg);
                    }
                    else {
                        server_url = msg.next_url;
                        fetch_timer = setTimeout(tick, 0, server_url);
                        update_table(msg);
                        return;
                    }
                }
            }
            fetch_timer = setTimeout(tick, 1000, server_url);
        }
    };
    fetch_timer = setTimeout(tick, 100, server_url);
}

function update_table(msg) {
    if (msg.payload.length) {
        output.unshift(msg.payload.reverse().join(''));
        while (output.length > _count) {
            output.pop();
        }
        dirty = 1;
    }
}

function display_table() {
    if (dirty) {
        document.getElementById("output").innerHTML = '<pre class="uk-dark">'
            + output.join('') + "</pre>";
        dirty = 0;
    }
}

function tick(server_url) {
    xhttp.open("GET", server_url, true);
    xhttp.send();
}

function playOrPause() {
    const toggleBtn = document.getElementById("toggle");
    const state = toggleBtn.getAttribute("data-state");

    if (state === "pause") {
        clearTimeout(fetch_timer);
        fetch_timer = null;
        clearInterval(display_timer);
        display_timer = null;
        xhttp.abort();
        xhttp = null;

        toggleBtn.setAttribute("data-state", "play");
    }
    else {
        start_timer();

        toggleBtn.setAttribute("data-state", "pause");
    }
}
