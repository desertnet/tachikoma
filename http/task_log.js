var xhttp     = new XMLHttpRequest();
var serverUrl = "http://" + window.location.hostname + ":4242/cgi-bin/topic.cgi/tasks/0/recent/500";
var timer     = null;
var output    = [];

function start_timer() {
  xhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
      var msg = JSON.parse(this.responseText);
      if (!msg.next_url || msg.next_url == serverUrl) {
        // console.log(serverUrl);
        timer = setTimeout(tick, 2000);
      }
      else {
        for (var i = 0; i < msg.payload.length; i++) {
          var msg_payload = msg.payload[i].payload || "";
          var tr = "";
          if (msg.payload[i].type == "TASK_ERROR") {
            tr = "<tr bgcolor=\"#FF9999\">";
          }
          else if (msg.payload[i].type == "TASK_OUTPUT") {
            tr = "<tr bgcolor=\"#99FF99\">";
          }
          else if (msg.payload[i].type == "TASK_BEGIN"
                || msg.payload[i].type == "TASK_COMPLETE") {
            tr = "<tr bgcolor=\"#DDDDDD\">";
          }
          else {
            tr = "<tr>";
          }
          var row = tr + "<td>" + msg.payload[i].timestamp + "</td>"
                       + "<td>" + msg.payload[i].type      + "</td>"
                       + "<td>" + msg.payload[i].key       + "</td>"
                       + "<td>" + msg_payload              + "</td></tr>";
          output.unshift(row);
        }
        while ( output.length > 500 ) {
          output.pop();
        }
        document.getElementById("output").innerHTML
                    = "<table>"
                    + "<tr><th>TIMESTAMP</th>"
                    + "<th>TYPE</th>"
                    + "<th>KEY</th>"
                    + "<th>VALUE</th></tr>"
                    + output.join("")
                    + "</table>";
        serverUrl = msg.next_url;
        timer = setTimeout(tick, 0);
      }
    }
    else if (this.readyState == 4) {
      timer = setTimeout(tick, 2000);
    }
  };
  timer = setTimeout(tick, 0);
}

function tick() {
  xhttp.open("GET", serverUrl, true);
  xhttp.send();
}
