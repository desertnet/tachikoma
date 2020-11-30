var server_url  = "https://" + window.location.hostname
                      + ":" + window.location.port
                      + "/cgi-bin/query.cgi/" + topic;
var xhttp       = new XMLHttpRequest();
var num_queries = 1;
var is_storable = /TM_STORABLE/;

function add_query() {
    num_queries++
    render_form();
}
function rm_query() {
    if (num_queries > 1) {
        num_queries--;
        render_form();
    }
}

function render_form() {
    var form_html = '<button onclick="add_query()">+</button>';
    if (num_queries > 1) {
        form_html += '<button onclick="rm_query()">-</button>'
    }
    form_html += '<form onsubmit="execute_query(); return false;" id="query_params">';
    for (var i = 0; i < num_queries; i++) {
      form_html += '<select name="' + i + '.field">';
      for (var j = 0, l = indexes.length; j < l; j++) {
          form_html += '  <option value="' + topic + '.' + indexes[j] + ':index">' + indexes[j] + '</option>';
      }
      form_html += '</select>'
          + '<select name="' + i + '.op">'
          + '  <option value="keys">keys</option>'
          + '  <option value="eq">eq</option>'
          + '  <option value="ne">ne</option>'
          + '  <option value="re">re</option>'
          + '  <option value="nr">nr</option>'
          + '  <option value="ge">ge</option>'
          + '  <option value="le">le</option>'
          + '</select>'
          + '<input name="' + i + '.key"/>'
          + '<br>';
    }
    form_html += '<button>search</button>'
        + '</form>';
    document.getElementById("query_form").innerHTML = form_html;
}

function execute_query() {
    var data = {};
    var form = document.getElementById("query_params");
    for (var i = 0, l = form.length; i < l; ++i) {
        var input = form[i];
        if (input.name) {
            data[input.name] = input.value;
        }
    }
    xhttp.addEventListener("progress", updateProgress);
    if (data["0.op"] == "keys") {
        xhttp.onreadystatechange = function() {
            if (this.readyState == 4 && this.status == 200) {
                // var msg = JSON.parse(this.responseText);
                document.getElementById("output").innerHTML = "<pre>" + this.responseText + "</pre>";
            }
        };
    }
    else {
        xhttp.onreadystatechange = function() {
            var output = [];
            if (this.readyState == 4 && this.status == 200) {
                if (this.responseText) {
                    var msg = JSON.parse(this.responseText);
                    msg.sort(function(a, b) {
                        return a.timestamp - b.timestamp;
                    });
                    for (var i = 0; i < msg.length; i++) {
                        if (msg[i].type.match(is_storable)) {
                            output.push(JSON.stringify(msg[i].value, null, 2) + "\n");
                        }
                        else {
                            output.push(msg[i].value);
                        }
                    }
                    while ( output.length > 500 ) {
                        output.shift();
                    }
                    document.getElementById("output").innerHTML = "<pre>" + output.join("") + "</pre>";
                }
                else {
                    document.getElementById("output").innerHTML = "<em>no results</em>";
                }
            }
        };
    }
    var data = {};
    var form = document.getElementById("query_params");
    for (var i = 0, l = form.length; i < l; ++i) {
        var input = form[i];
        if (input.name) {
            data[input.name] = input.value;
        }
    }
    var query_data;
    if (num_queries > 1) {
        query_data = [];
        for (var i = 0; i < num_queries; i++) {
            query_data[i] = {
                "field" : data[i + ".field"],
                "op" : data[i + ".op"],
                "key" : data[i + ".key"]
            };
        }
    }
    else {
        query_data = {
            "field" : data["0.field"],
            "op" : data["0.op"],
            "key" : data["0.key"]
        };
    }
    xhttp.open("POST", server_url, true);
    xhttp.setRequestHeader("Content-Type", "application/json; charset=UTF-8");
    var json_data = JSON.stringify(query_data)
    xhttp.send(json_data);
    document.getElementById("output").innerHTML = "<pre>" + json_data + "</pre>";
}

function updateProgress (oEvent) {
    document.getElementById("output").innerHTML = "<pre>loaded " + oEvent.loaded + " bytes</pre>";
}
