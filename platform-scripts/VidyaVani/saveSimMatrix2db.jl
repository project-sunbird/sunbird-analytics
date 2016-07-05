# Author: Soma S Dhavala
# Given a <concept,concept,score>, and <score type>, populate
# conceptSimilarity table  in learnder_db



using JSON
using Requests
using JuMP

if Pkg.installed("CQL") ==  nothing
  Pkg.clone("https://github.com/somasdhavala/CQL.jl.git")
end
using CQL
#import CQL: connect, disconnect, command, query, asyncCommand

server = "localhost";
con = CQL.connect(server);


#concept1 text,
#concept2 text,
#relation_type text,
#sim double
str_prefix = "insert into learner_db.conceptsimilaritymatrix (concept1,concept2,relation_type, sim) values "
n = 2
relation_type = "mock_rel"
for i in 1:n
  concept_i = "test_concept_$i"
  for j in 1:n
    concept_j = "test_concept_$j"
    sim = rand()
    req = str_prefix * "('$concept_i','$concept_j','$relation_type',$sim);"
    # insert the record to cassandra table
    println(req)
    query = CQL.query(con,req)
  end
end
#
query = CQL.disconnect(con)
# generate concept-ids
