using domainUtils

url="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/domain/map"

cd("/Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/domain-model")

# try reading the domainModel
status,domainModelData = domainUtils.readDomainModel(url)

if(!status)
  error("Aborting: can not process domain model")
end

# read the concepts
concepts, conceptHash = domainUtils.getConceptsFromConceptMap(domainModelData)

# read the parent-of relationship graph
cGraph = domainUtils.getConceptGraph(domainModelData,"isParentOf")

# from the parent-child graph, get concept-concept similarity matrix
B = domainUtils.getConceptSimilarityByRelation(cGraph)

# get concept-grade coverage matrix
cGraph,grades,gradeHash,concepts,conceptHash = domainUtils.getConceptGradeCoverageVec(domainModelData)
