package require java

java::import -package java.util ArrayList List
java::import -package java.util HashMap Map
java::import -package com.ilimi.graph.dac.model Node
java::import -package com.ilimi.graph.dac.model Filter
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
	$map put "status" "Live"

	set contentTypes [java::new {String[]} 4]
	$contentTypes set 0 "Story"
	$contentTypes set 1 "Worksheet"
	$contentTypes set 2 "Game"
	$contentTypes set 3 "Collection"

	$map put "contentType" $contentTypes
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
		set content [java::prop $node "metadata"]
		$content put "tags" [java::prop $node "tags"]
		$content put "identifier" [java::prop $node "identifier"]
		$content remove "body"
		$content remove "editorState"
		set concepts [java::new ArrayList]

		set relations [java::prop $node "outRelations"]
		if {[relationsExist $relations]} {
			java::for {Relation relation} $relations {
				if {[java::prop $relation "endNodeObjectType"] == "Concept"} {
					$concepts add [java::prop $relation "endNodeId"]
				}
			}
		}
		$content put "concepts" $concepts
		$contentList add $content
	}
} catch {Exception err} {
    puts [$err getMessage]
    $resultMap put "error" [$err getMessage]
}	

$resultMap put "contents" $contentList
set responseList [create_response $resultMap] 
return $responseList