var xhttp     = new XMLHttpRequest();
var serverUrl = "http://" + window.location.hostname + ":2501/fetch/tasks:queue";
var timer     = null;

function start_timer() {
  xhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
      var msgs   = JSON.parse(this.responseText);
      var output = [];
      var footer = "";
      for (var i = 0; i < msgs.length && i < 1000; i++) {
        var tr = "";
        if (msgs[i].attempts > 1) {
          tr = "<tr bgcolor=\"#FF9999\">";
        }
        else if (msgs[i].attempts == 1) {
          tr = "<tr bgcolor=\"#99FF99\">";
        }
        else {
          tr = "<tr bgcolor=\"#DDDDDD\">";
        }
        var row = tr + "<td>" + msgs[i].message_timestamp + "</td>"
                     + "<td>" + msgs[i].message_stream    + "</td>"
                     + "<td>" + msgs[i].message_payload   + "</td>"
                     + "<td>" + msgs[i].attempts          + "</td>"
                     + "<td>" + msgs[i].next_attempt      + "</td></tr>";
        output.push(row);
      }
      if (msgs.length == 0) {
        footer = "<em>-empty-</em>";
      }
      document.getElementById("output").innerHTML
                  = "<table>"
                  + "<tr><th>TIMESTAMP</th>"
                  + "<th>KEY</th>"
                  + "<th>VALUE</th>"
                  + "<th># ATTEMPTS</th>"
                  + "<th>NEXT ATTEMPT</th></tr>"
                  + output.join("")
                  + "</table>"
                  + footer;
    }
    if (this.readyState == 4) {
      timer = setTimeout(tick, 2000);
    }
  };
  timer = setTimeout(tick, 0);
}

function tick() {
  xhttp.open("GET", serverUrl, true);
  xhttp.send();
}
