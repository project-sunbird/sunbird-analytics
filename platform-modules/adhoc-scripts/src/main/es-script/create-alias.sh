#!/usr/bin/env bash

index_name="cbatchstats_duplicate"
alias_name="cbatchstats"
host="localhost" # Change it to Elastic IP use: groups['core-es'][0]

echo "Creating alias to index is started"

curl -X POST "$host:9200/_aliases" -H 'Content-Type: application/json' -d'
{
    "actions" : [
        { "add" : { "index" :"'"$index_name"'", "alias" : "'"$alias_name"'" } }
    ]
}
'