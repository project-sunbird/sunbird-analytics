rm(list=ls(all=TRUE))

setwd('/Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/Funtoot')

library(igraph)
library(sna)
library(dplyr)



#infile = "testFile.graphml"
#infile = "funtoot_numeracy.graphml"
infile = "funtoot_numeracy_1.2.xml"
outfile = "funtoot_graph_metrics.csv"
simulate.graph = FALSE

# whether to compute global features
global.features = FALSE

if(simulate.graph){
  
  # create a random graph
  n = 10
  A <- matrix(round(runif(n*n)),n,n)
  g <- graph.adjacency(A,mode="directed",weight=T)
  write.graph(g, infile, "graphml")
  } else {
  # read the graph
  # read edge list file
  g = read.graph(infile, "graphml")
  A = as.matrix(as_adjacency_matrix(g))
  n = ncol(A)
}

# generate the following features
features <- c("indegree","outdegree","centrality","closeness",
                     "pagerank","is.conn","clust")
k = length(features)
cmapFeatures <- matrix(NA,n,k)
colnames(cmapFeatures) <- features

# create an igraph object
#g <- graph.adjacency(A,mode="directed",weight=T)

# per node features

# in degree
cmapFeatures[,"indegree"] <- sna:::degree(A,"indegree")
# out degree
cmapFeatures[,"outdegree"] <- sna:::degree(A,"outdegree")

# is the graph isolte w.r.t to each node (ego)
cmapFeatures[,"is.conn"] <- sna:::is.isolate(A,1:n)

# local clustering
cmapFeatures[,"clust"] <- igraph:::transitivity(g,type="barrat")

# page-rank
cmapFeatures[,"pagerank"] <- igraph:::page.rank(g)$vector

# closness
cmapFeatures[,"closeness"] <- sna:::closeness(A)

# betweenness centrality of each node
cmapFeatures[,"centrality"] <- sna:::betweenness(A)

# structure w.r.t to each node
#cmapFeatures[ii,strct.names] <- structure.statistics(A,1:env_n)

if (global.features){
  # graph level features
  
  # degree centrality
  tmp <-sna:::centralization(A,FUN=degree)
  # flow centrality
  tmp <- sna:::centralization(A,FUN=flowbet)
  # transitivity
  tmp <- igraph:::transitivity(g_igraph,type="global")
  # efficiency of a graph
  tmp <- sna:::efficiency(A)
  # hierarchy
  tmp <- sna:::hierarchy(A)
  # connectedness
  tmp <- sna:::connectedness(A)
  # lubness
  tmp <- sna:::lubness(A)
  # graph density 
  tmp <- sna:::gden(A)
  # is connected
  tmp <-sna:::is.connected(A)
  # mutuality (# of dyands in the di-graph)
  tmp <-sna:::mutuality(A)
  # number of ties in a graph
  tmp <- sna:::nties(A)
  # number of cuts in a graph
  tmp <- length(sna:::cutpoints(A))
  # other centrality functions are possible
  tmp <- sna:::centralization(A,FUN=evcent)
  tmp <- sna:::centralization(A,FUN=loadcent)
  tmp <- sna:::centralization(A,FUN=betweenness)
}

# verify the edges and properties
set_vertex_attr(g,"label", value = LETTERS[1:n])
vids <- as_ids(V(g))
eids <- as_ids(E(g))
ind = sapply(eids,function(tmp) pmatch(vids[894],tmp))  
edge_attr(g,"category",index=which(ind==1))

rownames(cmapFeatures)=vertex_attr(g,"id",V(g))
df <- data.frame(node.id=vertex_attr(g,"id",V(g)))
df2 <- as.data.frame(cmapFeatures)
df <- cbind(df,df2)
write.table(df,outfile,col.names=T,row.names = F,sep=',')
