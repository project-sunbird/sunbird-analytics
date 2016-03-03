module domainUtils
# Following functions can be imported as modules


using JSON
using LightGraphs
using Requests
using DataStructures
using Distances

# given the file containing domain map extract all the s
# in the domain model - in particular read the relationships and  definitions


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
      error("File name specifying Domain Model is not a valid file")
    end
    domainModelData=JSON.parse(domainModelData)

    if(!haskey(domainModelData,"result"))
      error("invalid schema. \"result\" attribute not found.")
    elseif (!haskey(domainModelData["result",],"s"))
      error("invalid schema. \"s\" attribute not found.")
    end

    #  dictionary
    dC = domainModelData["result",]["s",]

    if(!haskey(dC[1],"identifier"))
      error("invalid schema. \"result\" attribute not found. invalid schema")
    end

    if (!haskey(domainModelData["result",],"relations"))
      error("invalid schema. \"relations\" attribute not found.")
    end

    # relationship dictionary
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

function getsFromMap(domainModelData)

  #  dictionary
  dC = domainModelData["result",]["s",]
  concepts = []

  nC = length(dC)
  for i in 1:nC
    x = dC[i]
    ln = x["identifier",]
    #println(ln)
    push!(concepts,ln)
  end

  sort!(concepts)

  # create a look-up from index to
  conceptHash = Dict()
  ind=1
  for c in concepts
    conceptHash[ind]=c
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

  concepts, conceptHash = getsFromMap(domainModelData)
  n=length(s)
  cGraph=Array{Int64,2}(zeros(n,n))


  #  dictionary
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

  for i in 1:n
    for j in 1:n
      if (haskey(cDict,(conceptHash[i],conceptHash[j])))
        cGraph[i,j]=1
      end
    end
  end
  return cGraph
end

function getGraphFromAdj(cGraph,directed=true)
  n,m=size(cGraph)
  if(n!=m)
    error("graph has to be a square matrix")
  end

  if(directed)
    g = DiGraph(n)
  else
    g = Graph(n)
    # make the adj symmetric
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

  g = getGraphFromAdj(cGraph*weight,directed)
  n = size(cGraph)[1]

  B=Array{Float64,2}(zeros(n,n))

  for i in 1:n
    d = dijkstra_shortest_paths(g, i).dists
    d[d.>=maxDepth]=maxDepth
      B[i,:]=d
  end
  B = 1 - (B/maximum(B))
  return B
end

function getConceptGradeCoverageVec(domainModelData,impute=true,directed=false,gradeTag="gradeLevel",weight=1)

  concepts, conceptHash = getConceptsFromConceptMap(domainModelData)
  n=length(s)

  dC = domainModelData["result",]["s",]
  # get all the unique grade levels
  grades = []

  # in the 1st pass, look-up the grades
  for i in 1:n
    x = dC[i]
    ln = x["identifier",]
    Dict[ln]=
    if(haskey(x,"gradeLevel"))
      push!(grades,x["gradeLevel"])
    end
  end
  sort!(unique(grades))

  # create a hash for grades
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
    rGraph = getConceptGraph(domainModelData,relationType="isParentOf")
    for c in s
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

function getConceptSimilarityByGrade(cGraph,)
  d = pairwise(Euclidean(), cGraph)
  return(d)
end

end
