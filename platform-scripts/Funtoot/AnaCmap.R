
setwd('/Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/Funtoot')
rm(list=ls(all=TRUE))

suppressPackageStartupMessages(require(reshape2, quietly=TRUE))
suppressPackageStartupMessages(require(ggplot2, quietly=TRUE))
suppressPackageStartupMessages(require(plyr, quietly=TRUE))
suppressPackageStartupMessages(require(dplyr, quietly=TRUE))
suppressPackageStartupMessages(require(cluster, quietly=TRUE))
suppressPackageStartupMessages(require(gridExtra, quietly=TRUE))
suppressPackageStartupMessages(require(scales, quietly=TRUE))
suppressPackageStartupMessages(require(rpart, quietly=TRUE))
suppressPackageStartupMessages(require(party, quietly=TRUE))
suppressPackageStartupMessages(require(rpart.plot, quietly=TRUE))
suppressPackageStartupMessages(require(randomForest, quietly=TRUE))
suppressPackageStartupMessages(require(colorspace, quietly=TRUE))
suppressPackageStartupMessages(require(ComplexHeatmap, quietly=TRUE))
suppressPackageStartupMessages(require(RColorBrewer, quietly=TRUE))
suppressPackageStartupMessages(require(circlize, quietly=TRUE))
suppressPackageStartupMessages(require(colorspace, quietly=TRUE))
suppressPackageStartupMessages(require(GetoptLong, quietly=TRUE))
suppressPackageStartupMessages(require(moments, quietly=TRUE))
suppressPackageStartupMessages(require(bnlearn, quietly=TRUE))
#suppressPackageStartupMessages(require(gRain, quietly=TRUE))
suppressPackageStartupMessages(require(Rgraphviz, quietly=TRUE))
suppressPackageStartupMessages(require(tm, quietly=TRUE))
set.seed(123)


# outfile = "funtoot_graph_metrics_undirected.csv"
# df <- read.table(outfile,header = T,sep=',')
# 
# p <- ggplot(df, aes(objectType, pagerank))
# p + geom_boxplot()
# 
# p <- ggplot(df, aes(objectType, centrality))
# p + geom_boxplot()
# 
# #df.sub  = filter(df, objectType=="MicroConcepts")
# #area = sapply(df.sub$id,FUN=function(tmp) unlist(strsplit(as.character(tmp),":"))[3],simplify=T)
# #dimension = sapply(df.sub$id,FUN=function(tmp) unlist(strsplit(as.character(tmp),":"))[4],simplify=T)
# #df.sub$area = area()
# #df.sub$dim = dimension
# 
# #p <- ggplot(df.sub, aes(area, centrality))
# #p + geom_boxplot()
# 
# #p <- ggplot(df.sub, aes(dim, centrality))
# #p + geom_boxplot()
# 
# #p <- ggplot(df.sub, aes(dim, indegree))
# #p + geom_boxplot()
# 
# #p <- ggplot(df.sub, aes(dim, outdegree))
# #p + geom_boxplot()


outfile = "MicroConceptMetrics.csv"
outfile = "microMetricsFuntoot.csv"
df <- read.table(outfile,header = T,sep=',')
df <- df[,-1]
# rearrange the order
df <- df[,c(1:3,10,4:9,11)]

# normalize all
ind = c(5:11)
tmp <- df[,ind]
tmp <- scale(tmp,center=FALSE,scale=apply(tmp,2,max))
df[,ind]<-tmp

# p <- ggplot(df, aes(x = reorder(Dimension, In.Degree, FUN=median), y = In.Degree))
# p <-p + geom_boxplot()+labs(title = "Dim vs InDegree",x="Dimension",y="In Degree")
# print(p)
# 
# p <- ggplot(df, aes(x = reorder(Dimension, Out.Degree, FUN=median), y = Out.Degree))
# p<- p + geom_boxplot()+labs(title = "Dim vs OutDegree",x="Dimension",y="Out Degree")
# print(p)
# 
# p <- ggplot(df, aes(x = reorder(Dimension, Reachability, FUN=median), y = Reachability))
# p<- p + geom_boxplot()+labs(title = "Dim vs Reachability",x="Dimension",y="Reachability")
# print(p)
# 
# p <- ggplot(df, aes(x = reorder(Dimension, Betweenness.Centrality, FUN=median), y = Betweenness.Centrality))
# p<- p + geom_boxplot()+labs(title = "Dim vs Betweenness.Centrality",x="Dimension",y="Betweenness.Centrality")
# print(p)
# 
# p <- ggplot(df, aes(x = reorder(Dimension, Closeness.Centrality, FUN=median), y = Closeness.Centrality))
# p<- p + geom_boxplot()+labs(title = "Dim vs Closeness.Centrality",x="Dimension",y="Closeness.Centrality")
# print(p)
# 
# p <- ggplot(df, aes(x = reorder(Dimension, Page.Rank, FUN=median), y = Page.Rank))
# p<- p + geom_boxplot()+labs(title = "Dim vs Page.Rank",x="Dimension",y="Page.Rank")
# print(p)

for (ii in seq(5,11)){
  names = colnames(df)
  y = df[,ii]
  Dimension = df[,"Dimension"]
  df.sub <- data.frame(y=y,Dimension=Dimension)
  p <- ggplot(df.sub, aes(x = reorder(Dimension, y, FUN=median), y = y))
  p<- p + geom_boxplot()+labs(title = paste("Dim vs",names[ii]),x="Dimension",y=names[ii])
  print(p)
}


#library(GGally)
#ggscatmat(df, columns = 5:9, color="species", alpha=0.8)
#pairs(df[,5:9],col=df$Dimension)


for (ii in seq(5,10))
{
  names=colnames(df)
  x=df[,ii]
  for (jj in seq(ii+1,11)){
    y=df[,jj]
    dimension = df[,"Dimension"]
    #dimension = reorder(Dimension, names[ii], FUN=median)
    df.sub = data.frame(x=x,y=y,dim=dimension)
    #ggplot(df.sub,aes(x,y))+geom_point(aes(colour=dim),alpha=0.8)
    p<-ggplot(df.sub,aes(x,y))+geom_point(aes(colour=reorder(dim,x,FUN=median)),alpha=0.8)+
      labs(title="corr plot",x=names[ii],y=names[jj],col="Dimension")
      #scale_colour_brewer(name = "Dimension")
    print(p)
    
  }
}






