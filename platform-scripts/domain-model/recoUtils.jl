using PyCall
using Requests
using domainUtils

# set-up
url="http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/analytics/domain/map"
baseURL = "http://lp-sandbox.ekstep.org:8080/taxonomy-service/v2/content/"

# try reading the domainModel
status,domainModelData = domainUtils.readDomainModel(url);

if(!status)
  error("Aborting: can not process domain model")
end

# get concept-grade coverage matrix
cGraph,grades,gradeHash,concepts,conceptHash = domainUtils.getConceptGradeCoverageVec(domainModelData);


#connect to cassandra via PyCall
@pyimport cassandra.cluster as cluster

# connect to cassandra
REcluster=cluster.Cluster()
REsession = REcluster[:connect]("learner_db")


# get a list of learners from the "learnercontentsummary and learnerproficiency tables"
# n = collect(session[:execute]("SELECT COUNT(*) from learnercontentsummary"))[1][1]

# get list of unique number of learners from learnercontentsummary db
# convert array of tuples to list
lids1 = collect(REsession[:execute]("SELECT DISTINCT learner_id from learnercontentsummary"))
lidstmp = [lid[1] for lid in lids1]

# get list of unique number of learners from learnerproficiency db
lids2 = collect(REsession[:execute]("SELECT DISTINCT learner_id from learnerproficiency"))
lids2 = [lid[1] for lid in lids2]
# get sorted list of lids
lids = sort(unique([lids1;lids2]))

n = length(lids)
println("# of learners: ",n)
# get 1st content id
cids = collect(REsession[:execute]("SELECT content_id from learnercontentsummary"))
cids = [cid[1] for cid in cids]
# get unique content ids

# create unique Content list
cD = Dict();
index=1;
for i in 1:length(cids)
  cid = cids[i]
  if(!haskey(cD,cid))
    cD[cid]=index
    index+=1
  end
end

# m no of unique content available
m = index-1;
println("# of contents: ",m)
k = length(grades)

# create an m by k content to grade coverage vec
content2grade = Array{Int64,2}(zeros(m,k))

# create an n by k learner to grade coverage vec
learner2grade = Array{Int64,2}(zeros(n,k))

# create content2grade vec
i=1
for cid in keys(cD)
  println("proc. content #: ",i);
  # call Domain Model API to get concept ids
  contentURL = string(baseURL,cid)
  resp = get(contentURL)
  if(statuscode(resp)==200)
    conceptData = JSON.parse(readall(resp))
    # check if concepts tags are availble for this content
    if(haskey(conceptData["result"]["content"],"concepts"))
      tmp = conceptData["result"]["content"]["concepts"]
      for j in 1:length(tmp)
        if(hasKey(tmp[j],"identifier"))
          # get the grade distribution
          grade = conceptData["result"]["content"]["concepts"][1]["identifier"]
          content2grade[i,gradeHash[grade]]+=1
        end
      end
      # All Concepts in a Content are covered
    end
    # Concepts in Content are processed
  end
  # Content Model is processed
  i+=1
end

# loop through learner-content pairs
for i in 1:n
  # get the 1st learner record
  println("proc. learner #: ",i)
  lid = lids[i][1]
  query=string("SELECT content_id FROM learnercontentsummary WHERE learner_id= \'",lid,"\'")
  cids = collect(REsession[:execute](query))
  cn = length(cids)

  # loop through each content
  for j in 1:cn
    cid = cids[j][1]
    learner2grade[i,:]+=content2grade[cD[cid],:]
  end
end

# close db session
REsession[:shutdown](); REcluster[:shutdown]()








