#!/bin/bash
bucket_name=$1
prefix=$2
backup_dir=$3

src="s3://$bucket_name/$prefix/"
dst="s3://$bucket_name/$backup_dir/"

aws s3 cp $dst $src --recursive --include "*"