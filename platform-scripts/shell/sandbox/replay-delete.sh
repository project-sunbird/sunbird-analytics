#!/bin/bash
bucket_name=$1
backup_dir=$2

path="s3://$bucket_name/$backup_dir/"
aws s3 rm $path --recursive
