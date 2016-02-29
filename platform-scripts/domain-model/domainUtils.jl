module domainUtils
# Following functions can be imported as modules


import JSON
using LightGraphs
# given the file containing domain map extract all the concepts
# in the domain model - in particular read the relationships and concept definitions

function validateDomainModelSchema(filename)
  status=false
  try
    status=false
    if(!isfile(filename))
      error("File name specifying Domain Model is not a valid file")
    elseif (!isreadable(filename))
      error("File containing Domain Model is not readable")
    end

    f=open(filename)
    s=readall(f)
    close(f)
    s=JSON.parse(s)

    if(!haskey(s,"result"))
      error("invalid schema. \"result\" attribute not found.")
    elseif (!haskey(s["result",],"concepts"))
      error("invalid schema. \"concepts\" attribute not found.")
    end

    dC = s["result",]["concepts",]

    if(!haskey(dC[1],"identifier"))
      error("invalid schema. \"result\" attribute not found. invalid schema")
    end

    if (!haskey(s["result",],"relations"))
      error("invalid schema. \"relations\" attribute not found.")
    end

    dR = s["result",]["relations",]
    if (!haskey(dR[1],"startNodeId"))
      error("invalid schema. \"startNodeId\" attribute not found.")
    elseif (!haskey(dR[1],"endNodeId"))
      error("invalid schema. \"endNodeId\" attribute not found.")
    elseif (!haskey(dR[1],"relationType"))
      error("invalid schema. \"relationType\" attribute not found.")
    end

    status=true

  finally
    return status
  end
end

function getConceptsFromConceptMap(conceptFileName)
  if(!validateDomainModelSchema(conceptFileName))
    error("can not process the domain model file")
  end
  f=open(conceptFileName)
  s=readall(f)
  close(f)
  s=JSON.parse(s)

  dC = s["result",]["concepts",]
  concepts = []

  nC = length(dC)
  for i in 1:nC
    x = dC[i]
    ln = x["identifier",]
    #println(ln)
    push!(concepts,ln)
  end

  dR = s["result",]["relations",]
  nR = length(dR)
  for i in 1:nR
    x = dR[i]
    ln = x["startNodeId",]
    rn = x["endNodeId",]
    val = x["relationType",]
    if val == "isParentOf"
      push!(concepts,ln)
      push!(concepts,rn)
    end
  end
  return union(concepts)
end

function getConceptSimilarity(conceptFileName)

  if(!validateDomainModelSchema(conceptFileName))
    error("can not process the domain model file")
  end
  # get the list of unique concept names from the domain model

  f=open(conceptFileName)
  s=readall(f)
  close(f)
  s=JSON.parse(s)


  c = getConceptsFromConceptMap(conceptFileName)
  n=length(c)

  # create a look-up from index to concept
  indMap = Dict()
  ind=1
  for c1 in c
    indMap[ind]=c1
    ind=ind+1
  end

  cDict = Dict()
  dR = s["result",]["relations",]
  nR = length(dR)

  for i in 1:nR
    x = dR[i]
    ln = x["startNodeId",]
    rn = x["endNodeId",]
    val = x["relationType",]
    if val == "isParentOf"
      cDict[ln,rn] = 1
    end
  end

  gu = Graph(n)
  for i in 1:n
    for j in 1:n
      if (haskey(cDict,(indMap[i],indMap[j])))
        add_edge!(gu,i,j)
      end
    end
  end

  B=zeros(n,n)
  M=6 # depth of the tree
  for i in 1:n
    d = dijkstra_shortest_paths(gu, i).dists
    #println("shortest distance is: ",d)
    for j in 1:n
      x=min(d[j],M)/M
      B[i,j]=round((1-x)*1e2)/1e2 #round((1-x)*1e4)/1e4
    end
  end
  return B
end

end
