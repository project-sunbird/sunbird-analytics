# Authors: Soma S Dhavala
# functions to populate GraphDB (Neo4j)

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

# neo4j graph connector
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

# # read-csv learner proficiency table
# read_file = './data/CassandraLearnerProficiency.csv'

# # first pass to get the list of students and graders
# with open(read_file,'rb') as grade_file:
# 	for line in grade_file:
# 		line = line.rstrip()
# 		names = line.split(',')
		
# for name in names:
# 	print(name)


# setup cassandra connection
cassandraDbOn=True
cluster = Cluster()
session = cluster.connect('learner_db')

# set response schema to Dictionaries
session.row_factory = dict_factory

# # create a connection to neo4js
# graph = Graph()
# # delete entire graph
# graph.delete_all()

# # set uniqueness constraints
# # Concept needs to have a unique id
# graph.schema.create_uniqueness_constraint("Concept", "id")
# # Learner needs to have a unique id
# graph.schema.create_uniqueness_constraint("Learner", "id")
# # Content needs to have a unique id
# graph.schema.create_uniqueness_constraint("Content", "id")



# process learner-db


# move proficiency table
def moveRelevancyTableAll():

    graph = Graph()
    
    # get a list of all unique learners
    lids = session.execute("SELECT DISTINCT learner_id from learnerconceptrelevance")

    for lid in uids:
        # get the knowledge state for this guy
        # <concept-id>,<score> in schema
        
        uids = [ lid['learner_id'] for lid in lids]
        node = graph.merge_one("Learner","id",uid)
        

        print("** learner:",uid)

        relDict = session.execute("SELECT relevance from learnerconceptrelevance WHERE learner_id='" + uid + "'")[0]['relevance']
        for cid, score in relDict.items():
            #print("concept:",cid,"score",score)
            # create a node, if it does not exist
            # else, merge with it
            node2 = graph.merge_one("Concept","id",cid)
            # add a relationship with property score
            graph.create(Relationship(node2, "RELEVANT_TO", node,score=score))


# move content summary table
def moveContentSummaryTable():
    graph = Graph()

    lids = session.execute("SELECT DISTINCT learner_id from learnercontentsummary")
    for lid in lids:
        uid = lid['learner_id']
        print("** learner:",uid)
        # content_id text, interactions_per_min double,
        #num_of_sessions_played int,
        #time_spent double,
        node = graph.merge_one("Learner","id",uid)

        contentDict = session.execute("SELECT * from learnercontentsummary WHERE learner_id='" + uid + "'")[0]
        cid = contentDict['content_id']
        tsp = contentDict['time_spent']
        ipm = contentDict['interactions_per_min']

        node2 = graph.merge_one("Content","id",cid)
        # add a relationship with property score
        graph.create(Relationship(node, "INTERACTED_WITH", node2,timeSpent=tsp,ipm=ipm))
        print('content: ', cid, 'tsp: ',tsp, 'ipm', ipm)



# move proficiency table
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

        print("** learner:",uid)

        profDict = session.execute("SELECT proficiency from learnerproficiency WHERE learner_id='" + uid + "'")[0]['proficiency']
        for cid, score in profDict.items():
            print("concept:",cid,"score",score)

            # create/find concept node
            node2 = graph.merge_one("Concept","id",cid)
            # add a relationship with property score
            graph.create(Relationship(node, "ASSESSED_IN", node2,score=score))


# move domain model graphs
def moveContentModel():
    baseURL = "http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/getContent/"
    listURL = "http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/content/list"

    # neo4j graph connector
    graph = Graph()
    
    url = listURL
    resp = requests.get(url).json()
    # no of content
    contentList = resp["result"]["contents"]
    for contentListDict in contentList:
        # check if there is an identifier for this content
        if(not contentListDict.has_key('identifier')):
            continue
    
        # check if there is an identifier for this content
        identifier = contentListDict['identifier']

        # create a node for this Content
        node = graph.merge_one("Content","id",identifier)

        url = baseURL + identifier
        resp = requests.get(url)

        if(resp.status_code!=200):
            continue

        resp =  resp.json()

        createdOn=None
        languageCode=None
        gradeLevel=None
        identifier=None
        ageGroup=None
        concept=None
        owner=None

        contentDict = resp["result"]["content"]

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
            
    
        print('** id',identifier,'ageGroup:',ageGroup,'owner',owner)
        print('created:',createdOn,'lang:',languageCode,'grade:',gradeLevel,'**')
    
        for concept in concepts:
            print('concept:',concept)
            node2 = graph.merge_one("Concept","id",concept)
            graph.create(Relationship(node2, "COVERED_IN", node))



# move concept map 
def moveConceptMap():
    # neo4j graph connector
    graph = Graph()
    # delete entire graph

    url="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/domain/map"
    resp = requests.get(url).json()

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

        if(conceptDict.has_key('gradeLevel')):
            gradeLevel = conceptDict['gradeLevel']
            node.properties["gradeLevel"]=gradeLevel
            node.push()

        if(conceptDict.has_key('objectType')):
            objectType = conceptDict['objectType']
            node.properties["objectType"]=objectType
            node.push()
    

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
        print('A:',startNodeId,'relationType',relationType,'B:',endNodeId)
        node1 = graph.merge_one("Concept","id",startNodeId)
        node2 = graph.merge_one("Concept","id",endNodeId)
        graph.create(Relationship(node1, relationType, node2))



# move proficiency table
def moveRelevancyTable(n=10):
    # get a list of all unique learners
    # filepath = "batch-models/src/test/resources/concept-similarity/ConceptSimilarity.json"
    # neo4j graph connector
    graph = Graph()
    # only compute bottom "n" and top "n" relevent concepts
    
    lids = session.execute("SELECT DISTINCT learner_id from learnerconceptrelevance")
    for lid in lids:
        # get the knowledge state for this guy
        # <concept-id>,<rel score> in schema
        uid = lid['learner_id']
        # create a learner node
        node = graph.merge_one("Learner","id",uid)

        print("** learner:",uid)

        relDict = session.execute("SELECT relevance from learnerconceptrelevance WHERE learner_id='" + uid + "'")[0]['relevance']
        rawScores = relDict.values()
        qU = round(sorted(rawScores,reverse=True)[n-1]*1e4)/1e4
        qL = round(sorted(rawScores)[n-1]*1e4)/1e4

        for cid, rawscore in relDict.items():
            score = round(rawscore*1e4)/1e4
            if(score >= qU):
                
                print("concept:",cid,"score",score)
                # create/find concept node
                node2 = graph.merge_one("Concept","id",cid)
                # add a relationship with property score
                graph.create(Relationship(node2, "RELEVENT_FOR", node,score=score))
            elif(score <= qL):
                print("concept:",cid,"score",score)
                # create/find concept node
                #node2 = graph.merge_one("Concept","id",cid)
                # add a relationship with property score
                #graph.create(Relationship(node2, "NOT_RELEVENT_FOR", node,score=score))
                pass;
            else:
                pass;


# concept map
print('*******************')
print('1: populating Neo4js with Concept Map')
print('*******************')
moveConceptMap();
# content model
print('*******************')
print('2: populating Neo4js with Content Model')
print('*******************')
moveContentModel();
# learner-prof
print('*******************')
print('3: populating Neo4js with Knowledge State')
print('*******************')
moveProficiencyTable();
# learner-content
print('*******************')
print('4: populating Neo4js with Content Summarizer')
print('*******************')
moveContentSummaryTable();
# learner-rel
print('*******************')
print('5: populating Neo4js with Relevancy Score')
print('*******************')
#moveRelevancyTable(20);