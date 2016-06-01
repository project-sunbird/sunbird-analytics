#!/bin/bash
date1=$1
date2=$2
prefix=$4
bucket_name=$3
backup_dir=$5

start_date=$(date -d $date1 +%s)
end_date=$(date -d $date2 +%s)

while [ $start_date -le $end_date ] 
do
	date=`date -d @$start_date +%F`
    src="s3://$bucket_name/$prefix/"
    dst="s3://$bucket_name/$backup_dir/"
    aws s3 mv $src $dst --recursive --exclude "*" --include "$date-*"
    let start_date+=86400
done