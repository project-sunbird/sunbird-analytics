#!/bin/sh


#This script is to delete the index
#Before running this script please make sure existing index is already copied or not.  

#Configurations
index_name="" # Please mention the index name here : ex:cbatchstats
host="localhost" #Change this to Elastic search IP use: groups['core-es'][0]

echo "Delete of $index_name index is started.."

curl -X DELETE "$host:9200/$index_name"

echo "Delete of $index_name index is success"

