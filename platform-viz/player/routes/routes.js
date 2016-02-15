/**
 * Defines all Rest Routes. This is a framework component that can be used to
 * configure deployable services at runtime from their orchestrator. This can
 * also provide authentication, interceptor capabilities.
 *
 * @author Santhosh
 */
var learnerService = require('../service/LearnerService');
var domainService = require('../service/DomainService');

module.exports = function(app, dirname) {
	
	/** Content List Routes */
	app.get('/learner/info/:id', learnerService.getLearnerInfo);
	app.get('/domain/graph/:id', domainService.getDomainGraph);
	app.get('/domain/model/refresh', domainService.refreshDomainModel);
};

