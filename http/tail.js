var parsed_url  = new URL(window.location.href);
var server_host = window.location.hostname;
var server_port = window.location.port;
var server_path = "/cgi-bin/topic.cgi"
var _topic      = parsed_url.searchParams.get("topic")     || topic;
var partition   = parsed_url.searchParams.get("partition") || partition || 0;
var _count      = parsed_url.searchParams.get("count")     || count;
var prefix_url  = "https://" + server_host + ":" + server_port
                + server_path + "/"
                + _topic      + "/"
                + partition   + "/";
var server_url  = prefix_url  + "/" + offset + "/" + _count;
var xhttp       = new XMLHttpRequest();
var timer       = null;

function start_timer() {
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var msg = JSON.parse(this.responseText);
            if (!msg.next_url || msg.next_url == server_url) {
                if (timer != null) {
                    timer = setTimeout(tick, 100);
                }
            }
            else {
                document.getElementById("output").innerHTML = "<pre>"
                    + msg.payload.join("") + "</pre>";
                server_url = msg.next_url;
                if (timer != null) {
                    timer = setTimeout(tick, 0);
                }
            }
        }
    };
    document.getElementById("toggle").innerHTML = "pause";
    timer = setTimeout(tick, 0);
}

function tick() {
    xhttp.open("GET", server_url, true);
    xhttp.send();
}

function playOrPause() {
    var state = document.getElementById("toggle").innerHTML;
    if (state == "pause") {
        clearTimeout(timer);
        timer = null;
        document.getElementById("toggle").innerHTML = "play";
    }
    else {
        document.getElementById("toggle").innerHTML = "pause";
        timer = setTimeout(tick, 0);
    }
}
