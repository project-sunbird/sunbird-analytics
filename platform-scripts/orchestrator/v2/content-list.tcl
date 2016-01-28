package require java

java::import -package java.util ArrayList List
java::import -package java.util HashMap Map
java::import -package com.ilimi.graph.dac.model Node
java::import -package com.ilimi.graph.dac.model Relation

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

proc getNodesByObjectType {graphId type} {
	set map [java::new HashMap]
	$map put "nodeType" "DATA_NODE"
	$map put "objectType" $type
	set search_criteria [create_search_criteria $map]
	set response [searchNodes $graphId $search_criteria]
	set check_error [check_response_error $response]
	if {$check_error} {
		java::throw [java::new Exception "Error response from getDataNode"]
	}
	
	set nodes [get_resp_value $response "node_list"]
	return $nodes
}

set resultMap [java::new HashMap]
set contentList [java::new ArrayList]
java::try {
	set nodes [getNodesByObjectType "domain" "Content"]
	java::for {Node node} $nodes {
		set metadata [java::prop $node "metadata"]
		$metadata put "tags" [java::prop $node "tags"]
		$metadata put "identifier" [java::prop $node "identifier"]
		set concepts [java::new ArrayList]

		set relations [java::prop $node "outRelations"]
		if {[relationsExist $relations]} {
			java::for {Relation relation} $relations {
				if {[java::prop $relation "endNodeObjectType"] == "Concept"} {
					$concepts add [java::prop $relation "endNodeId"]
				}
			}
		}
		$metadata put "concepts" $concepts
		$contentList add $metadata
	}
} catch {Exception err} {
    puts [$err getMessage]
    $resultMap put "error" [$err getMessage]
}	

$resultMap put "contents" $contentList
set responseList [create_response $resultMap] 
return $responseList