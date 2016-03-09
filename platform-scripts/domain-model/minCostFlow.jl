# find distance between two vectors

using JuMP

function convertFloatToInt64(x,digits=2)
  # given a vec, normalize it, convert to an interger
  x = x/sum(x);
  M = 10^digits;
  x = round(x*10^digits);
  x = vec(map(tmp -> convert(Int64,round(tmp)),x));
  return x,M;
end


function getMinCostDistance(x,y,w,wext,verbose=true)
  # lables
  # x: source or supply
  # y: sink or demand
  n = length(x);
  if(n!=length(y))
    error("two vectors must of same size")
  end

  if((size(w)[1]!=size(w)[2]) | size(w)[1]!=n)
    error("cost matrix must be square")
  end
  # convert the vecs to integers
  x,M = convertFloatToInt64(x,2);
  y,M = convertFloatToInt64(y,2);

  # create exraneous source and sink nodes
  # in the case where supply is not equal to demand
  dif = sum(x)-sum(y);
  if(dif>0)
    # if supply is more, create extraneous demand node
    push!(x,0);
    push!(y,dif);
  else
    # if demand is more, create extraneous supply node
    push!(x,-dif);
    push!(y,0);
  end
  dif = sum(x)-sum(y);
  println("x: ",x)
  println("y: ",y)
  @assert dif == 0;

  N = n+1;
  M = sum(x);
  cost = Array{Int64,2}(zeros(N,N));
  # cost metric for given vec
  cost[1:n,1:n] = w;
  # cost to reach from ext.source to any sink node
  cost[N,1:n] = wext;
  # cost to reach ext.sink to any source node
  cost[1:n,N] = wext;

  mdl = Model();

  # flow is non-negative
  @defVar(mdl, flow[i=1:N, j=1:N] >= 0);
  # objective function:
  # minimize the overall flow from source to sink
  @setObjective(mdl, Min, sum{cost[i,j] * flow[i,j], i=1:N, j=1:N});
  # constraint: net out flow at source node should be equal its specified supply
  @addConstraint(mdl, xyconstr[i=1:1:N], sum{flow[i,j], j=1:N} == x[i]);
    # constraint: net out flow at source node should be equal its specified demand
  @addConstraint(mdl, xyconstr[j = 1:N], sum{flow[i,j], i=1:N} == y[j]);

  if(verbose)
    println("Solving original problem...")
  end
  status = solve(mdl);


  if status == :Optimal
    d = getObjectiveValue(mdl)/M/maximum(cost);
    flow = getValue(flow)
    if(verbose)
      @printf("Optimal!\n");
      @printf("Objective value: %d\n", d);
      @printf("Source:\n")
      println(x);
      @printf("Target:\n")
      println(y);
      @printf("Flow:\n");
      for j = 1:n
        @printf("\t%s", string("d",j));
      end
      @printf("\t%s", string("d.ext"));
      @printf("\n");
      for i = 1:n
        @printf("%s", string("s",i));
        for j = 1:N
          @printf("\t%d", (flow[i,j]));
        end
        @printf("\n");
      end
      @printf("%s", string("s.ext"));
      for j = 1:N
        @printf("\t%d", (flow[N,j]));
      end
      @printf("\n");
    end
  else
    d = [];
    if(verbose)
      @printf("No solution\n");
    end
  end
  return status, d, mdl, flow,cost;
end

x1 = [1 0 0 1 200]
x2 = [1 1 1 1 0]
x3 = [1 0 1 1 1]

w = [0 1 2 3 4;1 0 1 2 3;2 1 0 1 2;3 2 1 0 1;4 3 2 1 0]
wext = [1 1 1 1 1]
println("Source: ",x)
println("Target: ",y)
status, d1, mdl, flow, cost = getMinCostDistance(x1,x2,w,wext,false);
println("dist: ",d1)
status, d2, mdl, flow, cost = getMinCostDistance(x2,x3,w,wext,false);
println("dist: ",d2)
status, d3, mdl, flow, cost = getMinCostDistance(x1,x3,w,wext,false);
println("dist: ",d3)
println("triangle: ",[d1+d2,d3])








#


