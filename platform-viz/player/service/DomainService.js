var restUtils = require('./RestUtils'),
	_ = require('underscore'),
	async = require('async'),
	baseUrl = "http://lp-sandbox.ekstep.org:8080/taxonomy-service";
exports.graphs = {};
exports.conceptConverage = {};
exports.contents = {};

exports.refreshDomainModel = function(req, res) {
	exports.cacheDomainModel(function(err, msg) {
		res.send({err: err, msg: msg});
	})
}

exports.getDomainGraph = function(req, res) {
	res.send(exports.graphs[req.params.id])
}

exports.cacheDomainModel = function(cb) {
	async.parallel({
		numeracy: function(callback) {
			var args = {
				path: {},
				parameters: {}
			}
			restUtils.getCall(baseUrl + '/v2/domain/graph/numeracy', args, callback);
		},
		literacy: function(callback) {
			var args = {
				path: {},
				parameters: {}
			}
			restUtils.getCall(baseUrl + '/v2/domain/graph/literacy', args, callback);
		},
		contentList: function(callback) {
			var args = {
				path: {},
				parameters: {}
			}
			restUtils.getCall(baseUrl + '/v2/analytics/content/list', args, callback);
		}
	}, function(err, results) {
		if(results) {
			exports.graphs['numeracy'] = results.numeracy;
			exports.graphs['literacy'] = results.literacy;
			populateConceptCoverage(results.contentList);
		}
		console.log('Domain model populated/refreshed in cache');
		if(cb) {
			cb(null, 'Graph refreshed');
		}
	});
}

function populateConceptCoverage(res) {
	_.each(res.contents, function(content) {
		exports.contents[content.identifier] = content.identifier;
	});
	var contentConcepts = _.flatten(_.map(res.contents, function(content) {
		if(content.concepts && content.concepts.length > 0) {
			return _.map(content.concepts, function(cid) {
				return {'content': content.identifier, 'concept': cid};
			})
		} else {
			return {'content': content.identifier, 'concept': undefined};
		}
	}));
	var contentWithConcepts = _.groupBy(_.filter(contentConcepts, function(c) { return c.concept}), function(x) {return x.concept});
	for(k in contentWithConcepts) {
		exports.conceptConverage[k] = _.map(contentWithConcepts[k], function(c) { return c.content});
	}
}