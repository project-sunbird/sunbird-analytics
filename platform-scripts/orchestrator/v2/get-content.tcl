package require java

java::import -package java.util ArrayList List
java::import -package java.util HashMap Map
java::import -package com.ilimi.graph.dac.model Node
java::import -package com.ilimi.graph.dac.model Filter
java::import -package com.ilimi.graph.dac.model Relation

proc getNode {graphId nodeId} {
	set response [getDataNode $graphId $nodeId]
	set check_error [check_response_error $response]
	if {$check_error} {
		java::throw [java::new Exception "Error response from getDataNode"]
	}
	
	set node [get_resp_value $response "node"]
	return $node
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

set resultMap [java::new HashMap]
java::try {
	set node [getNode "domain" $contentId]
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
} catch {Exception err} {
    puts [$err getMessage]
    $resultMap put "error" [$err getMessage]
}	

$resultMap put "content" $content
set responseList [create_response $resultMap] 
return $responseList