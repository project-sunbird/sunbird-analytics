using domainUtils

url="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/domain/map"

cd("/Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/domain-model")

# try reading the domainModel
status,domainModelData = domainUtils.readDomainModel(url);

if(!status)
  error("Aborting: can not process domain model")
end

# read the concepts
concepts, conceptHash = domainUtils.getConceptsFromConceptMap(domainModelData);

# read the parent-of relationship graph
A = domainUtils.getConceptGraph(domainModelData,"isParentOf");

# from the parent-child graph, get concept-concept similarity matrix
C1 = domainUtils.getConceptSimilarityByRelation(cGraph)

# get concept-grade coverage matrix
cGraph,grades,gradeHash,concepts,conceptHash = domainUtils.getConceptGradeCoverageVec(domainModelData);

# try computing grade similarity
x1 = [1 0 0 1 200]
x2 = [1 1 1 1 0]
x3 = [1 0 1 1 1]

w = [0 1 2 3 4;1 0 1 2 3;2 1 0 1 2;3 2 1 0 1;4 3 2 1 0]
wext = [1 1 1 1 1]
println("Source: ",x)
println("Target: ",y)
status, d1, mdl, flow, cost = domainUtils.getMinCostDistance(x1,x2,w,wext,false);
println("dist: ",d1)
status, d2, mdl, flow, cost = domainUtils.getMinCostDistance(x2,x3,w,wext,false);
println("dist: ",d2)
status, d3, mdl, flow, cost = domainUtils.getMinCostDistance(x1,x3,w,wext,false);
println("dist: ",d3)
println("triangle: ",[d1+d2,d3])


# read content and get its attributes
url="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/content/org.ekstep.money.worksheet"
using Requests
url="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/content/org.ekstep.daysoftheweek"
resp = get(url)


