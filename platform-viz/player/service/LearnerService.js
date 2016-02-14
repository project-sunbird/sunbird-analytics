var cassandra = require('cassandra-driver');
var fs = require('fs');
var async = require('async');
var ds = require('./DomainService');
var client = new cassandra.Client({
    contactPoints: ['52.77.223.20'],
    keyspace: 'learner_db'
});
var LEARNER_SNAPSHOT = "SELECT * FROM learnersnapshot where learner_id = ':learnerId'";
var LEARNER_PROFICIENCY = "SELECT * FROM learnerproficiency where learner_id = ':learnerId'";
var LEARNER_CONCEPT_RELEVANCE = "SELECT * FROM learnerconceptrelevance where learner_id = ':learnerId'";
var LEARNER_CONTENT_SUMMARY = "SELECT * FROM learnercontentsummary where learner_id = ':learnerId'";

uniqueRecord = function(query, learnerId, cb) {
	client.execute(query.replace(':learnerId', learnerId), [], function (err, result) {
		if(err || null == result.first()) {
			cb('unable to fetch data. Err - ' + err);
		} else {
			cb(null, result.first());
		}
	});
}

multipleRecords = function(query, learnerId, cb) {
	client.execute(query.replace(':learnerId', learnerId), [], function (err, result) {
		if(err || null == result.rows) {
			cb('unable to fetch data. Err - ' + err);
		} else {
			cb(null, result.rows);
		}
	});
}

getLearnerProficiency = function(learnerProf) {
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
    return concepts;
}

getConceptTS = function(contentSummaries) {
	
}

getLearnerRecos = function(relevance, contentSummaries, proficiency) {
	console.log('ds.contents.length', ds.contents.length);
}

exports.getLearnerInfo = function(req, res) {
	var learnerId = req.params.id;
	async.parallel({
		snapshot: function(callback) {
			uniqueRecord(LEARNER_SNAPSHOT, learnerId, callback);
		},
		proficiency: function(callback) {
			uniqueRecord(LEARNER_PROFICIENCY, learnerId, callback);
		},
		relevance_scores: function(callback) {
			uniqueRecord(LEARNER_CONCEPT_RELEVANCE, learnerId, callback);
		},
		content_summaries: function(callback) {
			multipleRecords(LEARNER_CONTENT_SUMMARY, learnerId, callback);
		}
	}, function(err, results) {
		var data = {};
		if(results) {
			data.snapshot = results.snapshot;
			data.proficiency = getLearnerProficiency(results.proficiency);
			data.recos = getLearnerRecos(results.relevance_scores, results.contentSummaries, results.proficiency);
		}
		res.send(data);
	});
}