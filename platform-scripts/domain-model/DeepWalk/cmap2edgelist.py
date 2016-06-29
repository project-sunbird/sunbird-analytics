# Authors: Soma S Dhavala
# Module Dependencies:
#   Cassandra - pip install cassandra-driver
#   Neo4j - pip install py2neo
# functions to populate GraphDB (Neo4j)

import csv
import sys
import collections
import os.path
import requests

# on exit clean-ups
import atexit

import sys

#print 'Number of arguments:', len(sys.argv), 'arguments.'
#print 'Argument List:', str(sys.argv)
#print 'first arg is', str(sys.argv[1])

#url="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/domain/map"
url = str(sys.argv[1])
vertexfile = str(sys.argv[2])
edgelistfile = str(sys.argv[3])

f1 = open(vertexfile, 'w')
f2 = open(edgelistfile, 'w')

resp = requests.get(url).json()


vDict=dict()

# move all concepts
conceptList = resp["result"]["concepts"]
index=0
for conceptDict in conceptList:
    identifier=None
    if(not conceptDict.has_key('identifier')):
        continue
    identifier = conceptDict['identifier']
    if (not vDict.has_key(identifier) ):
        index+=1
        vDict[identifier]=index
        f1.write(str(index)+','+identifier+'\n')
f1.close()

# write vertex ids to file

    
# move all relations
relationList = resp["result"]["relations"]
for relationDict in relationList:
    if (not relationDict.has_key('startNodeId') ):
        continue
    if (not relationDict.has_key('endNodeId') ):
        continue
    if (not relationDict.has_key('relationType') ):
        continue
    startNodeId = relationDict['startNodeId']
    endNodeId = relationDict['endNodeId']
    relationType = relationDict['relationType']
    f2.write(str(vDict[startNodeId])+'\t'+ str(vDict[endNodeId])+'\n')
    #print('A:',startNodeId,'relationType',relationType,'B:',endNodeId)
    #print startNodeId+","+endNodeId

f2.close()
