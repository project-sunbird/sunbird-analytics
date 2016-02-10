package require java

java::import -package java.util ArrayList List
java::import -package java.util HashMap Map
java::import -package com.ilimi.graph.dac.model Filter
java::import -package com.ilimi.graph.dac.model Node
java::import -package com.ilimi.graph.dac.model Relation

proc getNodesByObjectType {graphId type} {
	set map [java::new HashMap]
	$map put "nodeType" "DATA_NODE"
	$map put "objectType" $type
	$map put "status" "Live"
	set search_criteria [create_search_criteria $map]
	set response [searchNodes $graphId $search_criteria]
	set check_error [check_response_error $response]
	if {$check_error} {
		java::throw [java::new Exception "Error response from getDataNode"]
	}
	
	set nodes [get_resp_value $response "node_list"]
	return $nodes
}

proc relationsExist {relations} {
	set exist false
	set hasRelations [java::isnull $relations]
	if {$hasRelations == 0} {
		set relationsSize [$relations size] 
		if {$relationsSize > 0} {
			set exist true
		}
	}
	return $exist
}

proc getNodes {graphId type} {

	set nodeList [java::new ArrayList]
	set relationList [java::new ArrayList]
	set nodes [getNodesByObjectType $graphId $type]
	java::for {Node node} $nodes {
		set metadata [java::prop $node "metadata"]
		$metadata put "objectType" $type
		$metadata put "identifier" [java::prop $node "identifier"]
		$metadata put "tags" [java::prop $node "tags"]
		$nodeList add $metadata

		set relations [java::prop $node "outRelations"]
		if {[relationsExist $relations]} {
			java::for {Relation relation} $relations {
				set relMetadata [java::prop $relation "metadata"]
				set nodeMetadata [java::prop $relation "endNodeMetadata"]
				set nodeStatus [java::cast {String} [$nodeMetadata get "status"]]
				if {[$nodeStatus equals "Live"]} {
					$relMetadata put "relationType" [java::prop $relation "relationType"]
					$relMetadata put "startNodeId" [java::prop $relation "startNodeId"]
					$relMetadata put "endNodeId" [java::prop $relation "endNodeId"]
					$relMetadata put "startNodeObjectType" [java::prop $relation "startNodeObjectType"]
					$relMetadata put "endNodeObjectType" [java::prop $relation "endNodeObjectType"]
					$relationList add $relMetadata
				}
			}
		}
	}
	set result [java::new HashMap]
	$result put "nodes" $nodeList
	$result put "relations" $relationList
	return $result
}

set resultMap [java::new HashMap]
set nodeList [java::new ArrayList]
set relationList [java::new ArrayList]
set objectTypes [java::new {String[]} 3]
java::try {
	$objectTypes set 0 "Domain"
	$objectTypes set 1 "Dimension"
	$objectTypes set 2 "Concept"
	java::for {String objectType} $objectTypes {
		set result [getNodes "domain" $objectType]
		set nodes [$result get "nodes"]
		set relations [$result get "relations"]
		$nodeList addAll [java::cast {List} $nodes]
		$relationList addAll [java::cast {List} $relations]
	}
} catch {Exception err} {
    puts [$err getMessage]
    $resultMap put "error" [$err getMessage]
}	

$resultMap put "concepts" $nodeList
$resultMap put "conceptsSize" [$nodeList size]
$resultMap put "relations" $relationList
set responseList [create_response $resultMap] 
return $responseList