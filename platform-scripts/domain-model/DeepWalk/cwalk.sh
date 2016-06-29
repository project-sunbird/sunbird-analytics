#!/bin/bash

# create adjacency list from sandbox concept map

url="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/domain/map"
vertexfile="cmap_vertexids.txt"
edgelistfile="cmap_edgelist.txt"
vertexvecfile="cmap_vertex2vec.txt"

# read concept map, create edgelist graph
python cmap2edgelist.py $url $vertexfile $edgelistfile

# run deepwalk on the conceptmap
python ./deepwalk-master/deepwalk --input $edgelistfile --output $vertexvecfile

# do a viz of the graph
# r script goes here


