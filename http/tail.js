var xhttp     = new XMLHttpRequest();
// var serverUrl = "http://" + window.location.hostname + ":4242/cgi-bin/topic.cgi/topic1/0/end/1";
var serverUrl = "http://" + window.location.hostname + ":4242/cgi-bin/topic.cgi/tasks/0/recent/100";
var timer     = null;
// console.log(serverUrl);

function start_timer() {
  xhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
      var msg = JSON.parse(this.responseText);
      if (!msg.next_url || msg.next_url == serverUrl) {
        // console.log(serverUrl);
        if (timer != null) {
          timer = setTimeout(tick, 100);
        }
      }
      else {
        document.getElementById("output").innerHTML = "<pre>" + JSON.stringify(msg, null, 2) + "</pre>";
        serverUrl = msg.next_url;
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
  xhttp.open("GET", serverUrl, true);
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
