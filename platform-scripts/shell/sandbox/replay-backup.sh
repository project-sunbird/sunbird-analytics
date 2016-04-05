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
 src="s3://lpdev-ekstep/$modelCode/$date-*"
 dst="s3://lpdev-ekstep/back-up/$modelCode/" 
 count=`s3cmd ls $src | wc -l`
 echo "path $src"
 if [ $count -gt 0 ]
 then
  echo "file is there in S3 and copy to $dst"
  s3cmd cp $src $dst 
 else
  echo 'file is not there in S3'
 fi
 let start_date+=86400
done