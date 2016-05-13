# Authors: Soma S Dhavala
# functions to populate GraphDB (Neo4j)

import csv
import sys
import collections
import os.path
import requests
import random
# on exit clean-ups
import atexit

# cassandra libs
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


# neo4j libs
from py2neo import Graph
from py2neo import Node, Relationship
from py2neo import authenticate




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
def mockConceptCoverage():

    # neo4j graph connector
    authenticate("localhost:7474", "neo4j", "1sTep123")
    graph = Graph()

    
    cypher = graph.cypher
    # get a list of all content
    conceptDict = cypher.execute("MATCH (x:Concept) RETURN x.id as concept")
    contentDict = cypher.execute("MATCH (x:Content) RETURN x.id as content")
    n = len(contentDict)

    for concept in conceptDict:
        id = concept.concept
        node = Node("Concept","id",id)
        graph.merge(node,"id",id)

        i = random.randint(0,n-1)
        id = contentDict[i].content
        node2 = Node("Content","id",id)
        graph.merge(node2,,"id",id)
        graph.create(Relationship(node, "COVERED_IN", node2))


# move proficiency table
def mockMisConcepts():

    # neo4j graph connector
    authenticate("localhost:7474", "neo4j", "1sTep123")
    graph = Graph()

    
    cypher = graph.cypher
    # get a list of all content
    learnerDict = cypher.execute("MATCH (x:Learner) RETURN x.id as learner")
    conceptDict = cypher.execute("MATCH (x:Concept) RETURN x.id as concept")
    n = len(conceptDict)

    for learner in learnerDict:
        id = learner.learner
        node = Node("Learner","id",id)
        graph.merge("id",id)

        i = random.randint(0,n-1)
        id = conceptDict[i].concept
        node2 = Node("Concept","id",id)
        graph.merge("id",id)
        graph.create(Relationship(node, "HAS_MISCONCEPTION_IN", node2))

def mockInteractions():

    # neo4j graph connector
    authenticate("localhost:7474", "neo4j", "1sTep123")
    graph = Graph()

    
    cypher = graph.cypher
    # get a list of all content
    learnerDict = cypher.execute("MATCH (x:Learner) RETURN x.id as learner")
    contentDict = cypher.execute("MATCH (x:Content) RETURN x.id as content")
    n = len(contentDict)

    for learner in learnerDict:
        id = learner.learner
        node = Node("Learner","id",id)
        graph.merge(node,"id",id)

        i = random.randint(0,n-1)
        id = contentDict[i].content
        node2 = Node("Content","id",id)
        graph.merge(node2,"id",id)
        graph.create(Relationship(node, "INTERACTED_WITH", node2))

# learner-rel
print('*******************')
print(' populating mock concept-content ')
print('*******************')
mockConceptCoverage();

print('*******************')
print(' populating mock mis-conceptions ')
print('*******************')
mockMisConcepts()

print('*******************')
print(' populating mock interactions ')
print('*******************')

mockInteractions()