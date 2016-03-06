# Author: Soma S Dhavala

module domainUtils
# Functions defined in this file can be imported as modules


using JSON
using LightGraphs
using Requests
using DataStructures
using Distances

# given the file/url containing domain map extract all the concepts
# in the domain model - in particular read the relationships and  definitions
# create various graphs based on relationships and node attributes in the Domain Model

# Domain Model V2, 4th March, 2016
# Ref: https://github.com/ekstep/Learning-Platform/tree/master/docs/domain_model_v2


# read the domain model (concepts, relationships)
# returns status and the domain model object (Julia dictonary)

function readDomainModel(strname, src="url")
  status=false
  domainModelData = []
  try
    if(src=="url")
      resp = get(strname)
      if(statuscode(resp)==200)
        domainModelData = readall(resp)
      else
        error("domain model url response failed")
      end
    elseif(src=="file")
      if(!isfile(filename))
        error("File name specifying Domain Model is not a valid file")
      elseif (!isreadable(filename))
        error("File containing Domain Model is not readable")
      else
        # read the file containing the domain model
        f=open(filename)
        domainModelData=readall(f)
        close(f)
      end
    else
      error("Neither an url nor a file name is supplied to read Domain Model")
    end

    domainModelData=JSON.parse(domainModelData)

    # all the data is stored in the "result" key
    # concept definitions on are in "concepts" nested under "results"

    if(!haskey(domainModelData,"result"))
      error("invalid schema. \"result\" attribute not found.")
    elseif (!haskey(domainModelData["result",],"concepts"))
      error("invalid schema. \"concepts\" attribute not found.")
    end

    #  check concept definition
    dC = domainModelData["result",]["concepts",]

    if(!haskey(dC[1],"identifier"))
      error("invalid schema. \"result\" attribute not found. invalid schema")
    end

    #  check relation definition
    if (!haskey(domainModelData["result",],"relations"))
      error("invalid schema. \"relations\" attribute not found.")
    end

    # check relation schema
    dR = domainModelData["result",]["relations",]
    if (!haskey(dR[1],"startNodeId"))
      error("invalid schema. \"startNodeId\" attribute not found.")
    elseif (!haskey(dR[1],"endNodeId"))
      error("invalid schema. \"endNodeId\" attribute not found.")
    elseif (!haskey(dR[1],"relationType"))
      error("invalid schema. \"relationType\" attribute not found.")
    end

    status=true

  finally
    return status,domainModelData
  end
end


function getConceptsFromConceptMap(domainModelData)
  # read sorted unique list of concepts

  #  read all concepts
  dC = domainModelData["result",]["concepts",]
  concepts = []

  nC = length(dC)
  for i in 1:nC
    x = dC[i]
    ln = x["identifier",]
    #println(ln)
    push!(concepts,ln)
  end
  sort!(unique(concepts))

  # create a look-up from concept-to-index
  conceptHash = Dict()
  ind=1
  for c in concepts
    conceptHash[c]=ind
    ind=ind+1
  end

#  in-case s are preesent in the relationships, but not in the  definitions
#   # relationship dictionary
#   dR = domainModelData["result",]["relations",]
#   nR = length(dR)
#   for i in 1:nR
#     x = dR[i]
#     ln = x["startNodeId",]
#     rn = x["endNodeId",]
#     val = x["relationType",]
#     if val == "isParentOf"
#       push!(s,ln)
#       push!(s,rn)
#     end
#   end
#   s = union(s)
  return concepts, conceptHash
end


function getConceptGraph(domainModelData,relationType="isParentOf")

  # create a graph where nodes are concepts and edges satisfy "relationType"
  # property as defined in the Domain Model

  concepts, conceptHash = getConceptsFromConceptMap(domainModelData)
  n=length(concepts)
  cGraph=Array{Int64,2}(zeros(n,n))

  #  read relation data
  cDict = Dict()
  dR = domainModelData["result",]["relations",]
  nR = length(dR)

  for i in 1:nR
    x = dR[i]
    ln = x["startNodeId",]
    rn = x["endNodeId",]
    val = x["relationType",]
    if val == relationType[1]
      cDict[ln,rn] = 1
    end
  end

  for ci in concepts
    i=conceptHash[ci]
    for cj in concepts
      j=conceptHash[cj]
      if (haskey(cDict,(ci,cj)))
        cGraph[i,j]=1
      end
    end
  end
  return cGraph
end

function getGraphFromAdj(cGraph,directed=true)
  # create an LighGraph graph object from adj matrix
  n,m=size(cGraph)
  if(n!=m)
    error("graph has to be a square matrix")
  end

  if(directed)
    g = DiGraph(n)
  else
    g = Graph(n)
    # make the adj symmetric for undirected graph
    cGraph = (cGraph+cGraph')
    cGraph[cGraph.>1]=1
  end
  for i in 1:n
    for j in 1:n
      if (cGraph[i,j]==1)
        add_edge!(g,i,j)
      end
    end
  end
  return g
end

function getConceptSimilarityByRelation(cGraph,directed=true,dist="geodesic",weight=1,maxDepth=6)

  # given an graph (adj matrix), compute a concept similarity matrix (between concepts)
  g = getGraphFromAdj(cGraph*weight,directed)
  n = size(cGraph)[1]

  B=Array{Float64,2}(zeros(n,n))

  # compute shortes-distance between every pair of concepts (Dijkstra's shortest path/ Geodesic distance)
  for i in 1:n
    d = dijkstra_shortest_paths(g, i).dists
    d[d.>=maxDepth]=maxDepth
      B[i,:]=d
  end
  # turn "normalized" distance into a simialrity matrix
  B = 1 - (B/maximum(B))
  return B
end

function getConceptGradeCoverageVec(domainModelData,impute=true,directed=false,gradeTag="gradeLevel",weight=1)

  # grade is an attribute at concept level. each concept can have multple grades
  # prepare, for each concept, count of each grade (for concepts, the count can be either 0 or 1)
  # but at dimension level, they can vary

  concepts, conceptHash = getConceptsFromConceptMap(domainModelData)

  n=length(concepts)

  dC = domainModelData["result",]["concepts",]
  # get all the unique grade levels
  grades = []

  # in the 1st pass, look-up the grades to create unique grades levels
  for i in 1:n
    x = dC[i]
    if(haskey(x,"gradeLevel"))
      for val in x["gradeLevel"]
        push!(grades,val)
      end
    end
  end
  # need to figure out how to sort strings (crazy)
  #sort!(unique(grades))
  grades = (unique(grades))

  # create a hash for grades (grade-to-index)
  gradeHash = Dict()
  ind=1
  for grade in grades
    gradeHash[grade]=ind
    ind=ind+1
  end

  # 2nd pass
  # create  by grade count matrix
  # rows are s, cols are grade levels
  cGraph=Array{Int64,2}(zeros(n,ind-1))

  # default assume it is grade 1
  cGraph[:,1] = 1

  for i in 1:n
    x = dC[i]
    c = x["identifier",]
    if(haskey(x,"gradeLevel"))
      vals = x["gradeLevel",]
        for val in vals
          cGraph[conceptHash[c],gradeHash[val]]+=1
        end
    end
  end

  # if grades need to be imputed at dimension and domain level (as of Domain Model v2)
  # using the parent-child relationship to aggregate the children's grade-count-vecs
  if(impute)
    rGraph = getConceptGraph(domainModelData)
    for c in concepts
      i = conceptHash[c]
      # get the parents of this list
      parList = find(rGraph[i,:])
      m = length(parList)
      if(m>0)
        for parent in parList
          cGraph[parent,:] += cGraph[i,:]
        end
      end
    end

  end
  return cGraph,grades,gradeHash,concepts,conceptHash
end


function getGradeDiffWeights(grades,method="ordinal")
  # park - need to develop
  m=length(grades)
  W=Array{Int64,2}(zeros(m,m))
  return W
end

function getConceptSimilarityByGrade(cGraph)
  # compute similarity between each pair of concepts (nodes) based on grade coverage
  # for future use: possibly consider that distances need to be weighed by grade levels
  # eg: grade-1 is farther from grade-5 than grade-1 is from grade-3
  d = pairwise(Euclidean(), cGraph)
  return(d)
end

end
