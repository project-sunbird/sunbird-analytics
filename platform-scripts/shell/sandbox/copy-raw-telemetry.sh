#!/usr/bin/env bash

from="s3://sandbox-ekstep-telemetry"
to="s3://sandbox-data-store/raw"

echo "--- Downloading files from $from"
aws s3 cp "$from" s3-dump --recursive > /dev/null
echo "--- Files downloaded from $from"

echo "--- Renaming files"
for filename in s3-dump/*.json.gz; do
	f0=`stat -c %Y $filename`
	f1=`date -d@$f0 +%Y%m%d`
	f2=`echo "$filename" | cut -c34-43`
	f3="s3-dump/$f2-$f1.json.gz"
	mv $filename $f3
done
echo "--- Files rename completed"

echo "--- Uploading files to $to"
aws s3 cp s3-dump "$to" --recursive > /dev/null
echo "--- Files uploaded to $to"
rm -rf s3-dump
echo "--- Cleanup completed"