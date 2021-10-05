var parsed_url      = new URL(window.location.href);
var server_host     = window.location.hostname;
var server_port     = window.location.port;
var server_path     = "/cgi-bin/topic.cgi"
var _topic          = parsed_url.searchParams.get("topic")          || topic;
var _num_partitions = parsed_url.searchParams.get("num_partitions") || num_partitions || 1;
var _count          = parsed_url.searchParams.get("count")          || count;
var _interval       = parsed_url.searchParams.get("interval")       || interval;
var xhttp           = [];
var fetch_timers    = [];
var display_timer   = null;
var output          = [];
var dirty           = 1;

function start_timer() {
    if (_topic) {
        for (var i = 0; i < _num_partitions; i++) {
            start_partition(i);
        }
        display_timer = setInterval(display_table, _interval);
        document.getElementById("toggle").innerHTML = "pause";
    }
    else {
        document.getElementById("toggle").innerHTML = "error";
        document.getElementById("output").innerHTML = "<pre>no topic</pre>";
    }
}

function start_partition(partition) {
    var prefix_url  = "https://" + server_host + ":" + server_port
                    + server_path + "/"
                    + _topic      + "/"
                    + partition   + "/";
    var server_url  = prefix_url  + "/" + offset + "/" + _count;
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
                server_url  = msg.next_url;
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
    if (msg.payload.length) {
        output.unshift(msg.payload.join(''));
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
        document.getElementById("output").innerHTML = "<pre>"
            + output.join("") + "</pre>";
        dirty = 0;
    }
}

function tick(partition, server_url) {
    xhttp[partition].open("GET", server_url, true);
    xhttp[partition].send();
}

function playOrPause() {
    var state = document.getElementById("toggle").innerHTML;
    if (state == "pause") {
        for (var i = 0; i < _num_partitions; i++) {
            clearTimeout(fetch_timers[i]);
        }
        fetch_timers = [];
        clearInterval(display_timer);
        display_timer = null;
        document.getElementById("toggle").innerHTML = "play";
    }
    else {
        start_timer();
    }
}
