package require java

java::import -package java.util ArrayList List
java::import -package java.util HashMap Map
java::import -package com.ilimi.graph.dac.model Filter
java::import -package com.ilimi.graph.dac.model Node
java::import -package com.ilimi.graph.dac.model Relation

proc getNodeRelations {graphId nodeId} {
	set response [getDataNode $graphId $nodeId]
	set check_error [check_response_error $response]
	if {$check_error} {
		java::throw [java::new Exception "Error response from getDataNode"]
	}
	
	set node [get_resp_value $response "node"]
	set outRelations [java::prop $node "outRelations"]
	return $outRelations
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

proc getConcepts {graphId nodeId} {

	set concepts [java::new ArrayList]
	set relations [getNodeRelations $graphId $nodeId]
	set hasRelations [relationsExist $relations]
	if {$hasRelations} {
		java::for {Relation relation} $relations {
			if {[java::prop $relation "endNodeObjectType"] == "Concept"} {
				$concepts add [java::prop $relation "endNodeId"]
			}
		}
	}
	return $concepts
}

set resultMap [java::new HashMap]
java::try {
	set concepts [getConcepts $graphId $itemId]
	if {[$concepts size] == 0} {
		set concepts [getConcepts $graphId $contentId]
	}
} catch {Exception err} {
    puts [$err getMessage]
    $resultMap put "error" [$err getMessage]
}	

$resultMap put "concepts" $concepts
set responseList [create_response $resultMap] 
return $responseList