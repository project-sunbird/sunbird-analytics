# read libsvm format data
library(methods)
library(SparseM)

# basic read and write to libsvm format
read.libsvm <- function(filename,target.name=NULL,features.name=NULL,fac=FALSE){
  csr <- read.matrix.csr(filename,fac=fac)
  y <- csr$y
  x <- as.matrix(csr$x)
  df <- data.frame(y=y,data.frame(x))
  if(!is.null(target.name)){
    colnames(df)[1] <- "target"
  }
  if(!is.null(features.name)){
    if(length(features.name==ncol(x))){
      colnames(df)[-1] <- features.name  
    }
  }
  return(df)
}
# read data in libsvm data (this could be slow)

write.libsvm <- function(df,filename,target.index=1,fac=FALSE){
  # except the target, all other variables have to be numeric
  msg = tryCatch(
    {
      y <- df[,target.index]
      x <- as.matrix.csr(as.matrix(df[,-target.index]))
      write.matrix.csr(x,filename,y,fac=fac)
      return('wrote file')
    },
    error=function(err){
      return(err)
    })
  return (msg)
}
# example
# data(iris)
# fname = 'test.libfm'
# write.libsvm(iris,fname,target.index=5,fac=FALSE)
# df <- read.libsvm(fname)