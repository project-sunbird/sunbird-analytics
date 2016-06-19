#!/usr/bin/Rscript

setwd("/Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/Funtoot")

df <- read.csv('studentVec.csv')
df<- scale(t(df[,-1]))
df <- as.data.frame(df)
names(df) <- c("Foundational","Centrality","Complexity")
library(scatterplot3d)
#png("studentProjections.png",width=10,height=12,paper='special')
png("studentProjections.png")
scatterplot3d(df$Foundational,df$Centrality,df$Complexity,main="Projecting Learners",
              xlab="Foundatinal",ylab="Centrality",zlab="Comeplxity")
dev.off()
