#!/bin/sh


# This script is used to copy the data from one index to another index

#Configuration
src_index="cbatchstats"
des_index="cbatchstats_duplicate"
host="localhost" # Change it to ES IP


echo "Copy index from $src_index to $des_index"

curl -X POST "$host:9200/_reindex" -H 'Content-Type: application/json' -d'                            
{
  "source": {
    "index": "'"$src_index"'"
  },
  "dest": {
    "index": "'"$des_index"'"
  }
}
'
