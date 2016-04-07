#!/bin/bash
date1=$1
date2=$2
modelCode=$3
bucket_name=$4
backup_dir=$5

start_date=$(date -d $date1 +%s)
end_date=$(date -d $date2 +%s)

while [ $start_date -le $end_date ] 
do
	date=`date -d @$start_date +%F`
	echo $date
    src="s3://$bucket_name/$modelCode/"
    dst="s3://$bucket_name/$backup_dir/"
    aws s3 mv $src $dst --recursive --exclude "*" --include "$date-*"
    let start_date+=86400
done