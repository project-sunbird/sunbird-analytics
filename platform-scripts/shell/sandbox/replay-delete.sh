#!/bin/bash
date1=$1
date2=$2
modelCode=$3
start_date=`date -j -f %Y-%m-%d $date1 +%s`
end_date=`date -j -f %Y-%m-%d $date2 +%s`
while [ $start_date -le $end_date ] 
do
 date=`date -j -f %s $start_date +%Y-%m-%d`
 echo $date
 path="s3://lpdev-ekstep/back-up/$modelCode/$date-*"
 count=`s3cmd ls $path | wc -l`
 echo "path $path"
 if [ $count -gt 0 ]
 then
  echo "file is there in S3 in the path $path and deleting it...."
  s3cmd rm $path
 else
  echo "file is not there in the path $path"
 fi
 let start_date+=86400
done