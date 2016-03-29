using PyCall
using Requests
using domainUtils

# set-up
url="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/domain/map"
baseURL = "http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/content/"

# connect to neo4j via pycall

# connect to cassandra
@pyimport cassandra.cluster as cluster
REcluster=cluster.Cluster()

# connect to neo4j
@pyimport neo4jrestclient.client as n4c
gdb = n4c.GraphDatabase("http://localhost:7474/db/data/", username="neo4j", password="soma")
n = gdb.nodes.create(color="Red", widht=16, height=32)



get("http://httpbin.org/get"; query = Dict("title" => "page1"))
url="http://localhost:7474/user/neo4j/password"
resp = get(url,Dict("neo4j" => "soma"))

# using pycall and py2neo

@pyimport py2neo












