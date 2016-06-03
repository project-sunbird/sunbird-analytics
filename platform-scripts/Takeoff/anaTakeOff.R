#!/usr/bin/Rscript


cat('Begin executing R script on:',format(Sys.Date(), "%B %d, %Y"), '\n')
require(jsonlite)
require(rmarkdown)


cat(' Reading Json Sream\n')
readJsonLineStreams <- function(filename)
{
  tryCatch(
    {
      stream_in(file("stdin"))
      #stream_in(file("sample.txt"))
    },
    error = function(err){
      cat(' Could not read streaming data\n')
      message(err,'\n')
      cat(' Terminating R script on:',format(Sys.Date(), "%B %d, %Y"), '\n')
      quit(save="no",status=1,runLast=FALSE)
    }
  )
}
setwd("/Users/soma/github/ekStep/Learning-Platform-Analytics/platform-scripts/Takeoff")
cat(' Reading Json Stream ....\n')
df <- stream_in(file("DeltaDumpLadakhMay.txt","r"))
cat(' Done reading JSON Sream\n')
cat(' Read',length(df$uid),' events \n')


tag = "08664a0d8217401a02ce315a747cb2210d98d13b"
filterTags <- function(tmp)
{
  flag = 0; 
  if(length(tmp)!=0)  
    {
    flag = 1 #(tmp==tags)
    }
  return (flag)
}
ind = lapply(df$tags,FUN=function(tmp) {flag=FALSE; if(length(tmp)!=0) {flag = (tmp==tags)} return tmp} )
ind = lapply(df$tags,function(tmp) ,simplify=TRUE)
ind = which



