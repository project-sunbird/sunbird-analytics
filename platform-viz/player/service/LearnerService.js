var cassandra = require('cassandra-driver');
var fs = require('fs');
var async = require('async');
var _ = require('underscore');
var ds = require('./DomainService');
var config = require('../conf/appConfig.json')
var client = new cassandra.Client({
    contactPoints: [config.CASSANDRA_HOST],
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
	var contentTS = {};
	_.each(contentSummaries, function(c) {
		contentTS[c.content_id] = c.time_spent
	});
	var concepts = [];
	for (k in ds.conceptConverage) {
		var cts = _.map(ds.conceptConverage[k], function(content) {
			return (contentTS[content] || 0)
		})
		concepts[k] = _.reduce(cts, function(ts, agg) { return (ts + agg)}, 0);
	}
	return concepts[k];
}

sortFn = function(a,b) {
	if (a.prof < b.prof) {
		return -1;
	} else if (a.prof > b.prof) {
		return 1;
	} else if (a.ts < b.ts) {
		return -1;
	} else if (a.ts > b.ts) {
		return 1;
	} else {
		return 0;
	}
}

getLearnerRecos = function(conceptRelevance, contentSummaries, cp) {
	var conceptTS = getConceptTS(contentSummaries);
	var concepts = [];
	for(k in cp.proficiency) {
		concepts.push({cid: k, ts: conceptTS[k] || 0, prof: cp.proficiency[k]});
	}
	var rootConcept = _.filter(concepts, function(c) {
		return (ds.num_concepts.indexOf(c.cid) != -1)
	}).sort(sortFn);
	var cr = [];
	for (k in conceptRelevance.relevance) {
		cr.push({cid: k, r: conceptRelevance.relevance[k]});
	}
	var numeracyCR = _.filter(cr, function(c) {
		return (ds.num_concepts.indexOf(c.cid) != -1)
	})
	var sortedCR = _.first(numeracyCR.sort(function(a, b) {
    	return b.r - a.r;
	}), 5);
	return {startNode: rootConcept[0], endNodes: sortedCR.reverse()}
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
		if(err) {
			res.send({error: err});
		} else {
			var data = {};
			if(results) {
				data.snapshot = results.snapshot;
				data.proficiency = getLearnerProficiency(results.proficiency);
				data.recos = getLearnerRecos(results.relevance_scores, results.contentSummaries, results.proficiency);
			}
			res.send(data);
		}
	});
}