
# coding: utf-8
# Authors: Adarsa
# functions to populate GraphDB (Neo4j) for content usage in production


import csv
import sys
import collections
import os.path
import requests

# on exit clean-ups
import atexit

# cassandra libs
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


# neo4j libs
from py2neo import Graph
from py2neo import Node, Relationship
from py2neo import authenticate

# neo4j graph connector
authenticate("localhost:7474", "neo4j", "1sTep123")
graph = Graph()
# delete entire graph
graph.delete_all()

# bool flag database connections
cassandraDbOn=False
neo4jDbOn=False

def dbCleanUP(cassandraDbOn,neo4jDbOn):
    if cassandraDbOn:
    	print 'cleaning Cassandra state'
    	session.shutdown();
    	cluster.shutdown();

atexit.register(dbCleanUP,True,True)

# setup cassandra connection
cassandraDbOn=True
cluster = Cluster()
session = cluster.connect('learner_db')

# set response schema to Dictionaries
session.row_factory = dict_factory


def moveConceptMap():
    # neo4j graph connector
    graph = Graph()
    # load concept map from production

    import requests

   
    url = "https://api.ekstep.in/learning/v2/domains/numeracy/concepts"

    payload = "-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\n\r\n-----011000010111000001101001--"
    headers = {
        'content-type': "multipart/form-data; boundary=---011000010111000001101001",
        'user-id': "rayuluv",
        'cache-control': "no-cache",
        'postman-token': "96bc4304-3f9b-6de5-6143-4c14507fe0a5"
        }

    resp = requests.request("GET", url, data=payload, headers=headers).json()

    # move all concepts
    conceptList = resp["result"]["concepts"]
    for conceptDict in conceptList:
        identifier=None
    
        if(not conceptDict.has_key('identifier')):
            continue

        identifier = conceptDict['identifier']
        # create/find node
        node = graph.merge_one("Concept","id",identifier)

        if(conceptDict.has_key('subject')):
            subject = conceptDict['subject']
            node.properties["subject"]=subject
            node.push()

        if(conceptDict.has_key('objectType')):
            objectType = conceptDict['objectType']
            node.properties["objectType"]=objectType
            node.push()
    
        if(conceptDict.has_key('children')):
            relationList=conceptDict['children']
            for relationDict in relationList:
                if (not relationDict.has_key('identifier') ):
                    continue
                if (not relationDict.has_key('relation') ):
                    continue
                node1 = graph.merge_one("Concept","id",relationDict['identifier'])
                relationType=relationDict['relation']
                graph.create(Relationship(node, relationType, node1))
# concept map
print('*******************')
print('1: populating Neo4js with Concept Map')
print('*******************')
#moveConceptMap();

#move sandbox content model
def moveContentModel():
    listURL= "https://api.ekstep.in/learning/v2/content/list"

    payload = "{\n  \"request\": { \n      \"search\": {\n          \"fields\": [\"name\", \"contentType\"],\n          \"status\":[\"Live\", \"Draft\", \"Retired\"],\n          \"contentType\":[\"Game\", \"Worksheet\", \"Story\"],\n          \"limit\":2000\n          \n      }\n  }\n}"
    headers = {
        'content-type': "application/json",
        'user-id': "mahesh",
        'cache-control': "no-cache",
        'postman-token': "cec63279-346d-a452-4b13-e3cc0a0c2e4d"
        }

    resp = requests.request("POST", listURL, data=payload, headers=headers).json()
    # neo4j graph connector
    graph = Graph()
  
    # no of content
    contentList = resp["result"]["content"]
    for contentListDict in contentList:
        # check if there is an identifier for this content
        if(not contentListDict.has_key('identifier')):
            continue
    
        # check if there is an identifier for this content
        identifier = contentListDict['identifier']

        # create a node for this Content
        node = graph.merge_one("Content","id",identifier)

        createdOn=None
        languageCode=None
        gradeLevel=None
        identifier=None
        ageGroup=None
        concept=None
        owner=None

        contentDict = contentListDict

        if(contentDict.has_key('languageCode')):
            languageCode = contentDict['languageCode']
            node.properties['languageCode'] = languageCode
            node.push()
    
        if(contentDict.has_key('createdOn')):
            createdOn = contentDict['createdOn']
            node.properties['createdOn'] = createdOn
            node.push()
    

        if(contentDict.has_key('ageGroup')):
            ageGroup = contentDict['ageGroup'][0]
            node.properties['ageGroup'] = ageGroup
            node.push()

        if(contentDict.has_key('gradeLevel')):
            gradeLevel = contentDict['gradeLevel'][0]
            node.properties['gradeLevel'] = gradeLevel
            node.push()
    
        if(contentDict.has_key('owner')):
            owner = contentDict['owner']
            node.properties['owner'] = owner
            node.push()
    
        if(contentDict.has_key('concepts')):
            # this forms a "relationship" in the graph
            concepts = contentDict['concepts']
            
        for concept in concepts:
              if(concept.has_key('identifier')):
                node2 = graph.merge_one("Concept","id",concept['identifier'])

                graph.create(Relationship(node2, "COVERED_IN", node))


# content model
print('*******************')
print('2: populating Neo4js with Content Model')
print('*******************')
moveContentModel();


def moveProficiencyTable():
    # get a list of all unique learners
    # neo4j graph connector
    graph = Graph()
    
    lids = session.execute("SELECT DISTINCT learner_id from learnerproficiency")
    for lid in lids:
        # get the knowledge state for this guy
        # <concept-id>,<socre> in schema
            
        uid = lid['learner_id']
        # create a learner node
        node = graph.merge_one("Learner","id",uid)

        profDict = session.execute("SELECT proficiency from learnerproficiency WHERE learner_id='" + uid + "'")[0]['proficiency']
        paramDict= session.execute("SELECT model_params from learnerproficiency WHERE learner_id='" + uid + "'")[0]['model_params']

        for cid, score in profDict.items():
            # create/find concept node
            node2 = graph.merge_one("Concept","id",cid)
            alpha=float(paramDict[cid][9:12])
            beta=paramDict[cid][20]
            # add a relationship with property score
            graph.create(Relationship(node, "ASSESSED_IN", node2,score=score,alpha=alpha,beta=beta))

# learner-prof
print('*******************')
print('3: populating Neo4js with Knowledge State')
print('*******************')
moveProficiencyTable();

# move content summary table
def moveContentSummaryTable():
    graph = Graph()

    lids = session.execute("SELECT DISTINCT learner_id from learnercontentsummary")
    for lid in lids:
        uid = lid['learner_id']
        node = graph.merge_one("Learner","id",uid)

        
        contentDict = session.execute("SELECT * from learnercontentsummary WHERE learner_id='" + uid + "'")[0]
        cid = contentDict['content_id']
        tsp = contentDict['time_spent']
        ipm = contentDict['interactions_per_min']

        node2 = graph.merge_one("Content","id",cid)
        # add a relationship with property score
        graph.create(Relationship(node, "INTERACTED_WITH", node2,timeSpent=tsp,ipm=ipm))
        
# learner-content
print('*******************')
print('4: populating Neo4js with Content Summarizer')
print('*******************')
moveContentSummaryTable();

#sample queries

cid="org.ekstep.delta"  #content id
# proficiency across all learners for content
query="MATCH (c:Content)<-[r1:INTERACTED_WITH]-()-[r2:ASSESSED_IN]->(d:Concept) WHERE c.id='"+cid+"'OPTIONAL MATCH (c)<-[covered_in]-() RETURN avg(r2.score) AS ProficiencyScore"
resp= graph.cypher.execute(query)
print("Average proficiency across all learners for content '"+cid+"':")
print( resp[0])


# In[ ]:

# get missing concepts
#query="MATCH (n) WHERE has(n.`missing-concepts`) RETURN DISTINCT "node" as element, n.`missing-concepts` AS `missing-concepts` UNION ALL MATCH ()-[r]-() WHERE has(r.`missing-concepts`) RETURN DISTINCT "relationship" AS element, r.`missing-concepts` AS `missing-concepts`"
#resp= graph.cypher.execute(query)
#print("Missing Concepts:")
#print( resp[0])

