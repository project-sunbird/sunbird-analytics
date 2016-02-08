var cassandra = require('cassandra-driver');
var fs = require('fs');
var client = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    keyspace: 'learner_db'
});
var query = "SELECT * FROM learnerproficiency";

/*client.eachRow(query, [], function(n, row) {
    console.log('row', row)
    var learnerProf = row;
    var prof = learnerProf.proficiency;
    var modelParams = learnerProf.model_params;
    var precisions = {};
    for (k in modelParams) {
        var mpString = modelParams[k];
        var mp = JSON.parse(mpString);

        var alpha = mp.alpha;
        var beta = mp.beta;
        var precision =  Math.log(Math.pow((alpha + beta), 2) * (alpha + beta + 1)/(alpha * beta));
        precisions[k] = Math.round(precision * 100) / 100;
    }
    var graphString = fs.readFileSync('/Users/Santhosh/Downloads/numeracy.json');
    var graph = JSON.parse(graphString);
    var nodes = graph.result.subgraph.nodes;
    nodes.forEach(function(node) {
        if(prof[node.identifier]) {
            node.proficiency = prof[node.identifier];
            node.precision = precisions[node.identifier]
        }
    });
    fs.writeFileSync('/Users/Santhosh/Downloads/numeracy_prof.json', JSON.stringify(graph, 2));
    client.shutdown();
});*/

var learnersProfs = {};

function processRow(learnerProf) {
    var prof = learnerProf.proficiency;
    var modelParams = learnerProf.model_params;
    var concepts = {};
    for (k in modelParams) {
        var mpString = modelParams[k];
        var mp = JSON.parse(mpString);

        var alpha = mp.alpha;
        var beta = mp.beta;
        var precision =  Math.pow((alpha + beta), 2) * (alpha + beta + 1)/(alpha * beta);
        concepts[k] = {
            proficiency: prof[k],
            precision: Math.round(precision * 100) / 100
        }
    }
    learnersProfs[learnerProf.learner_id]=concepts;
    /*if(learnerProf.learner_id == 'c16820ca-195c-48ab-a9b0-1138f7a9cf20') {
        console.log('concepts', concepts);
    }*/
}

function sendOutput() {
    fs.writeFileSync('/Users/Santhosh/Downloads/learner_prof.json', JSON.stringify(learnersProfs, 2));
}

client.stream(query, [])
    .on('readable', function() {
        var row;
        while (row = this.read()) {
            processRow(row);
        }
    })
    .on('end', function() {
        sendOutput();
        client.shutdown();
    })
    .on('error', function(err) {
        console.log('err', err);
    });


