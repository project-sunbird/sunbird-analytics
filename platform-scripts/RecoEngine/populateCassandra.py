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



# try computing distances based on concept-similarity from neo4j db




if 1:
    # insert into cDB
    concept1="x1"
    concept2="x5"
    session.execute("INSERT INTO conceptsimilaritymatrix (concept1,concept2,relation_type,sim) VALUES (%s,%s,%s,%s)",(concept1,concept2,'parentOf',10))
    # delete from cDB
    str = "DELETE FROM conceptsimilaritymatrix  WHERE concept1='" + concept1 +"'"
    print str
    session.execute(str)
    # NSERT INTO conceptsimilaritymatrix ('concept1','concept2','relation_type','sim') VALUES ('x1','x2','parentOf',-1);



