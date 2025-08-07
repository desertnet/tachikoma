/**
 * Escape a string for safe insertion into HTML context.
 */
function escapeHTML(str) {
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

var params      = getQueryParams();
var server_url  = window.location.protocol + "//"
                      + window.location.hostname
                      + ":" + window.location.port
                      + "/cgi-bin/query.cgi/" + topic;
var xhttp       = new XMLHttpRequest();
var num_queries = params["num_queries"] || 1;
var is_storable = /TM_STORABLE/;
var operators   = ["eq", "ne", "re", "nr", "ge", "le"];

function add_query() {
    num_queries++
    render_form(true);
}

function rm_query() {
    if (num_queries > 1) {
        num_queries--;
        render_form(true);
    }
}

function render_form(quiet)  {
    var form_html = '<button onclick="add_query()"'
        + ' class="uk-button uk-button-default">+</button>';
    if (num_queries > 1) {
        form_html += '<button onclick="rm_query()"'
            + ' class="uk-button uk-button-default">-</button>'
    }
    form_html += '<form method="GET" onsubmit="execute_query(); return false;" id="query_params">';

    // track num_queries in a hidden input
    form_html += '<input type="hidden" name="num_queries" value="' + num_queries + '"/>';

    for (var i = 0; i < num_queries; i++) {
        // Get the values from the URL's query parameters
        var field = params[i + '.field'] || '';
        var op    = params[i + '.op']    || '';
        var key   = params[i + '.key']   || '';

        // replace + with space
        key = key.replace(/\+/g, ' ');

        // Set the selected option and input value based on the query parameters
        form_html += '<select name="' + i + '.field" onchange="updateURL()"'
            + ' class="uk-select uk-width-1-5 uk-margin-top">';
        for (var j = 0, l = indexes.length; j < l; j++) {
            var selected = field === (topic + '.' + indexes[j] + ':index') ? ' selected' : '';
            form_html += '  <option value="' + topic + '.' + indexes[j] + ':index"' + selected + '>' + indexes[j] + '</option>';
        }
        form_html += '</select>'
            + '<select name="' + i + '.op" onchange="updateURL()"'
            + ' class="uk-select uk-width-1-5 uk-margin-top uk-margin-left">';
        if (num_queries == 1) {
            var selected = op === 'keys' ? ' selected' : '';
            form_html += '  <option value="keys"' + selected + '>keys</option>';
        }
        for (var j = 0, l = operators.length; j < l; j++) {
            var selected = op === operators[j] ? ' selected' : '';
            form_html += '  <option value="' + operators[j] + '"' + selected + '>' + operators[j] + '</option>';
        }
        form_html += '</select>'
            + '<input name="' + i + '.key" id="' + i + '.key" value="' + key + '" oninput="updateURL()"'
            + ' class="uk-input uk-form-width-large uk-margin-top uk-margin-left"/>'
            + '<br>';
    }
    form_html += '<button'
        + ' class="uk-button uk-button-primary uk-margin-top">'
        + 'search</button>'
        + '</form>';
    document.getElementById("query_form").innerHTML = form_html;
    if (params["num_queries"] && ! quiet) {
        execute_query();
    }
    updateURL();
}

function getQueryParams() {
    var params = {};
    var queryString = window.location.search.substring(1);
    var pairs = queryString.split('&');
    for (var i = 0; i < pairs.length; i++) {
      var pair = pairs[i].split('=');
      if (pair[0]) {
        params[decodeURIComponent(pair[0])] = decodeURIComponent(pair[1] || '');
      }
    }
    return params;
}

function updateURL() {
    var form = document.getElementById('query_params');
    var formData = new FormData(form);
    var newParams = new URLSearchParams();
    if (num_queries == 1 && formData.get('0.op') == 'keys') {
        document.getElementById("0.key").value = "";
    }
    for (var pair of formData.entries()) {
      newParams.append(pair[0], pair[1]);
    }
    var newURL = window.location.protocol + '//' + window.location.host + window.location.pathname + '?' + newParams.toString();
    window.history.replaceState(null, null, newURL);
    params = getQueryParams();
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
                var msg = JSON.parse(this.responseText);
                var field = params['0.field'] || topic + '.' + indexes[0] + ':index';
                var table = '<table>';
                for (var key in msg[0]) {
                    table += '<tr><td><a href="?num_queries=1&' +
                        '0.field=' + escapeHTML(field) + '&' +
                        '0.op=eq&0.key=' + encodeURIComponent(key) + '">' + escapeHTML(key) + '</a></td>' +
                        '<td>' + escapeHTML(String(msg[0][key])) + '</td></tr>';
                }
                table += '</table>';
                document.getElementById("output").innerHTML = table;
                document.getElementById("graph").innerHTML = "";
                document.getElementById("table").innerHTML = "";
                document.getElementById("log").innerHTML = "";
            }
        };
        document.getElementById("0.key").value = "";
    }
    else {
        xhttp.onreadystatechange = function() {
            if (this.readyState == 4 && this.status == 200) {
                if (this.responseText) {
                    var results = [];
                    var msg = JSON.parse(this.responseText);
                    var isTable = false;
                    msg.sort(function(a, b) {
                        return a.timestamp - b.timestamp;
                    });
                    for (var i = 0; i < msg.length; i++) {
                        if (msg[i].type.match(is_storable)) {
                            isTable ||= true;
                        }
                        results.push(msg[i].value);
                    }
                    if (isTable) {
                        displayTableResults(results);
                    }
                    else {
                        displayFlatResults(results);
                    }
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
    document.getElementById("output").innerHTML = '<pre class="uk-dark">' + escapeHTML(json_data) + "</pre>";
}

function updateProgress (oEvent) {
    document.getElementById("output").innerHTML = '<pre class="uk-dark">loaded ' + oEvent.loaded + " bytes</pre>";
}

function displayTableResults(results) {
    // analyze results
    analyzeResults(results);
    // display table
    displayTable(results, results);
}

function displayFlatResults(results) {
    // display log entries
    displayLogEntries(results);
}

function analyzeResults(results) {
    // analyze results
    var field_counts = {};
    var field_values = {};
    var field_values_counts = {};
    var field_values_counts_sorted = {};
    var field_values_counts_sorted_keys = {};
    for (var i = 0; i < results.length; i++) {
        var result = results[i];
        for (var field in result) {
            // skip log_entry
            if (field == "log_entry") {
                continue;
            }
            if (field_counts[field] == undefined) {
                field_counts[field] = 0;
                field_values[field] = {};
                field_values_counts[field] = {};
            }
            field_counts[field]++;
            var value = result[field];
            if (field_values[field][value] == undefined) {
                field_values[field][value] = 0;
                field_values_counts[field][value] = 0;
            }
            field_values[field][value]++;
            field_values_counts[field][value]++;
        }
    }
    for (var field in field_values_counts) {
        var values = field_values_counts[field];
        var values_sorted = [];
        for (var value in values) {
            values_sorted.push([value, values[value]]);
        }
        values_sorted.sort(function(a, b) {
            return b[1] - a[1];
        });
        field_values_counts_sorted[field] = values_sorted;
        var values_sorted_keys = [];
        for (var i = 0; i < values_sorted.length; i++) {
            values_sorted_keys.push(values_sorted[i][0]);
        }
        field_values_counts_sorted_keys[field] = values_sorted_keys;
    }
    // display graphs
    var graph = document.getElementById("graph");
    graph.innerHTML = "";
    for (var field in field_values_counts_sorted) {
        var values = field_values_counts_sorted[field].slice(0, 100); // Get the top 100 values
        var values_keys = field_values_counts_sorted_keys[field].slice(0, 100); // Get the top 100 labels
    
        // Create a container div
        var containerDiv = document.createElement("div");
        containerDiv.style.height = "300px";
        containerDiv.style.width = "100%";
        containerDiv.style.display = "block";
    
        // Create the canvas element
        var canvas = document.createElement("canvas");
        canvas.setAttribute("id", "graph_" + field);
        containerDiv.appendChild(canvas);
        graph.appendChild(containerDiv);
    
        var ctx = canvas.getContext("2d");
        var chart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: values_keys,
                datasets: [{
                    label: field,
                    data: values.map(function(x) { return x[1]; }),
                    backgroundColor: values.map(function(x) { return randomColor(); })
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false, // Maintain aspect ratio
            }
        });
    }
}

function displayTable(results) {
    function sortTable(columnIndex, ascending) {
        var sortedResults = results.slice().sort(function(a, b) {
            var aValue = a[indexes[columnIndex]];
            var bValue = b[indexes[columnIndex]];
            if (!isNaN(parseFloat(aValue)) && !isNaN(parseFloat(bValue))) {
                aValue = parseFloat(aValue);
                bValue = parseFloat(bValue);
            }
            return (aValue > bValue ? 1 : (aValue < bValue ? -1 : 0)) * (ascending ? 1 : -1);
        });

        // Limit the sortedResults array to the first 2500 rows
        sortedResults = sortedResults.slice(0, 2500);

        tbody.innerHTML = ''; // Clear the tbody before appending rows

        sortedResults.forEach(function(result) {
            var tr = document.createElement("tr");
            for (var j = 0, l = indexes.length; j < l; j++) {
                var field = indexes[j];
                var td = document.createElement("td");

                // Create an anchor element and set the href attribute to the desired URL
                var a = document.createElement("a");
                var fieldValue = result[field];
                a.innerHTML = fieldValue;
                a.href = '?num_queries=1&0.field=' + topic + '.' + field + ':index&0.op=eq&0.key=' + encodeURIComponent(fieldValue);
                td.appendChild(a);
                tr.appendChild(td);
            }
            tbody.appendChild(tr);
        });
    }

    var table = document.getElementById("table");
    table.innerHTML = "";
    var thead = document.createElement("thead");
    var tr = document.createElement("tr");
    for (var j = 0, l = indexes.length; j < l; j++) {
        var th = document.createElement("th");
        th.innerHTML = indexes[j];
        th.dataset.sortOrder = "asc";
        th.addEventListener('click', (function(columnIndex) {
            return function() {
                var ascending = this.dataset.sortOrder === "asc";
                sortTable(columnIndex, ascending);
                this.dataset.sortOrder = ascending ? "desc" : "asc";
            };
        })(j));
        tr.appendChild(th);
    }
    thead.appendChild(tr);
    table.appendChild(thead);
    var tbody = document.createElement("tbody");
    table.appendChild(tbody);
    sortTable(0, true);
}

function displayLogEntries(results) {
    var log = document.getElementById("output");
    log.innerHTML = '<h3 class="uk-heading">Log Entries</h3>';

    // at most 5000 log entries
    var start = Math.max(0, results.length - 5000);
    for (var i = start; i < results.length; i++) {
        var div = document.createElement("div");
        div.innerHTML = results[i];
        log.appendChild(div);
    }
}

function randomColor () {
    return '#' + Math.floor(Math.random()*16777215).toString(16);
}
