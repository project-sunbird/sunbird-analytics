#!/bin/bash
date1=$1
date2=$2
modelCode=$3
bucket_name=$4
backup_dir=$5

start_date=`date -j -f %Y-%m-%d $date1 +%s`
end_date=`date -j -f %Y-%m-%d $date2 +%s`

while [ $start_date -le $end_date ] 
do
	date=`date -j -f %s $start_date +%Y-%m-%d`
 	echo $date
 	src="s3://$bucket_name/$modelCode/"
 	dst="s3://$bucket_name/$backup_dir/"
	aws s3 cp $src $dst --recursive --exclude "*" --include "$date-*"
 	let start_date+=86400
done