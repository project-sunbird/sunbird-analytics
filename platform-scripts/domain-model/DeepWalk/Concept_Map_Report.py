
import networkx as nx
import re
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#get edgelist
f = open('cmap_edgelist.txt')
text_edge = f.readlines()

#get vertexids
f = open('cmap_vertexids.txt')
text_node = f.readlines()

dict_node = {}

#define directional graph
G=nx.DiGraph()

#get all nodes
for line in text_node:
    line  = re.split(r',', line)
    nodeID = line[0]
    G.add_node(nodeID)
    identifier =  re.split(r'\n',line[1])[0]
    dict_node[identifier] = nodeID
    

G.number_of_nodes()

lst_graph = []
for line in text_edge:
    line  = re.split(r'\t+', line)
    first = line[0]
    second = re.split(r'\n',line[1])[0]
    lst_graph.append([first,second])

# adding edges to graph
G.add_edges_from(lst_graph)

n_edges = G.number_of_edges()
print "Number of edges in graph:" 
print n_edges
#a= nx.adjacency_matrix(G)
#print(a.todense())

#creating numpy matrix from graph
n = nx.to_numpy_matrix(G)
#(n.transpose() == n).all()

#no. of self loops
n_selfLoops = sum(np.diagonal(n))
print "Number of self loops in graph:" 
print n_selfLoops
print "self loops in graph:" 
print G.selfloop_edges()
#G.selfloop_edges()

#no.of dangling nodes
n_dangling = 0
for nodeID in G.nodes():
    #print nodeID
    if G.out_degree(nodeID) == 0 and G.in_degree(nodeID) == 0:
        n_dangling += 1

print "Number of Dangling nodes in graph: " 
print n_dangling

# concepts not linked to any other concepts
lst_colSum = np.sum(n, axis = 0)
lst_colSum = np.array(lst_colSum).tolist()
len(lst_colSum)
lst_colSum.count(0)

lst_rowSum = np.sum(n, axis = 1)
lst_rowSum = np.array(lst_rowSum).tolist()
lst_rowSum.count(0)


# Read concept dataframe

concept_mapDF = pd.read_csv(open('/Users/ajitbarik/Ilimi/github/Learning-Platform-Analytics/platform-scripts/object2Vec/content2vec/conceptDF.csv','rU'), 
                            sep ='\t', index_col=0, encoding='utf-8', engine='c')

concept_mapDF.is_copy = False

# no of concepts
n_concepts = concept_mapDF.shape
n_concepts = n_concepts[0]
print "Number of Concepts: %d " % n_concepts

# to check if num is NaN
def isNaN(num):
    return num != num

import unicodedata
#temp = unicodedata.normalize('NFKD', temp).encode('ascii','ignore')

def checkMisConcept(x):
    if isNaN(x):
        return 0
    x = x.encode('utf-8')
    #x = unicodedata.normalize('NFKD', x).encode('ascii','ignore')
    if re.search('misconcept', x, re.IGNORECASE):
        return 1
    else:
        return 0

concept_mapDF['Misconception'] = concept_mapDF['tags'].apply(lambda x: checkMisConcept(x))

# no of misconceptions 
n_misconceptions = concept_mapDF['Misconception'].sum()
print "Number of Misconception: %d " % n_misconceptions

lst_misconception = []
#concepts identifier which are misconception
for i in range(len(concept_mapDF['Misconception'])):
    if concept_mapDF['Misconception'][i] == 1:
        lst_misconception.append(concept_mapDF['identifier'][i])

concept_mapDF.head()
concept_mapDF['DescLen'] = concept_mapDF['description'].map(lambda x: 0 if isNaN(x) else len(x.encode('utf-8')))


# no. of concepts without description
n_conceptsWDesc = 0

for i in range(len(concept_mapDF['DescLen'])):
    if concept_mapDF.DescLen[i] == 0:
        n_conceptsWDesc += 1
print "Number of concepts without description: %d " % n_conceptsWDesc

# adding desc (whether desciption exist or not)
concept_mapDF['desc'] = concept_mapDF['DescLen'].map(lambda x: x if x == 0 else 1)

print concept_mapDF.head()


# # adding in-degree and out-degree column
# concept_mapDF['in_degree'] = 0
# concept_mapDF['out_degree'] = 0
# concept_mapDF['dangling'] = 0
# concept_mapDF['MisConceptDangling'] = 0

# for i in range(len(concept_mapDF['desc'])):
#     identifier = concept_mapDF['identifier'][i]
#     if dict_node.has_key(identifier):
#         nodeID = dict_node[identifier]
#         #print identifier
#         in_degree = G.in_degree(nodeID)
#         out_degree = G.out_degree(nodeID)
#         concept_mapDF['in_degree'][i] = in_degree
#         concept_mapDF['out_degree'][i] = out_degree
#         if in_degree == 0 and out_degree == 0:
#             concept_mapDF['dangling'][i] = 1
# #    else:
# #        concept_mapDF['in_degree'][i] = 'NA'
# #        concept_mapDF['out_degree'][i] = 'NA'

# for i in range(len(concept_mapDF['MisConceptDangling'])):
#     if concept_mapDF['Misconception'][i] == 1 and concept_mapDF['dangling'][i] == 1:
#         concept_mapDF['MisConceptDangling'][i] = 1


# # no of dangling misconception
# sum(concept_mapDF['MisConceptDangling'])
# concept_mapDF.head()
# pd.crosstab(concept_mapDF.Misconception, concept_mapDF.subject)
# pd.crosstab(concept_mapDF.Misconception, concept_mapDF.desc)
# pd.crosstab(concept_mapDF.Misconception, concept_mapDF.in_degree)
# pd.crosstab(concept_mapDF.Misconception, concept_mapDF.out_degree)

# # Misconceptions having child (out degree > 0)
# lst_index = []
# for i in range(len(concept_mapDF['out_degree'])):
#     if concept_mapDF['out_degree'][i] > 0 and concept_mapDF['Misconception'][i] == 1:
#             identifier = concept_mapDF['identifier'][i]
#             lst_index.append(i)
#             #print identifier
#             if dict_node.has_key(identifier):
#                 nodeID = dict_node[identifier]
#                 #print nodeID

# concept_mapDF.iloc[lst_index,]
# concept_mapDF['desc'][lst_index]

# import seaborn as sns
# sns.set(style="darkgrid")
# sns.boxplot(x="subject", y="DescLen", hue="Misconception", data=concept_mapDF, palette="Set1")
# #plt.ylim(0, 400)
# sns.despine(offset=10, trim=True)

# sns.boxplot(x= 'subject', y="DescLen", hue="dangling", data=concept_mapDF, palette="Set2")
# #plt.ylim(0, 300)
# sns.despine(offset=10, trim=True)
