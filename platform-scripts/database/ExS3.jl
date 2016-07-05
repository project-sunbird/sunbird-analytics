# read/write from s3
using AWSS3
using AWSCore

aws = AWSCore.aws_config(creds=AWSCore.AWSCredentials("AWS_ID","AWS_KEY"))
list= s3_list_buckets(aws)
files = s3_list_objects(aws,"prod-data-store","")
s3_get_file(aws,"prod-data-store","as/2016-04-23-20160424.json.gz","local_file.json.gz")
