package require java

java::import -package java.util ArrayList List
java::import -package java.util HashMap Map
java::import -package com.ilimi.graph.dac.model Filter
java::import -package com.ilimi.graph.dac.model Node
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

proc getNodeMetadata {node property} {

	set metadata [java::prop $node "metadata"]
	set propValue [$metadata get $property]
	return $propValue
}

proc getNodeRelationIds {node relationType property} {

	set relationIds [java::new ArrayList]
	set outRelations [java::prop $node "outRelations"]
	set hasRelations [relationsExist $outRelations]
	if {$hasRelations} {
		java::for {Relation relation} $outRelations {
			if {[java::prop $relation "endNodeObjectType"] == $relationType} {
				$relationIds add [java::prop $relation $property]
			}
		}
	}
	return $relationIds
}

proc getItemConcepts {graphId itemId contentId} {

	set resultMap [java::new HashMap]
	set item [getNode $graphId $itemId]
	set maxScore [getNodeMetadata $item "max_score"]
	set concepts [getNodeRelationIds $item "Concept" "endNodeId"]
	if {[$concepts size] == 0} {
		set content [getNode $graphId $contentId]
		set concepts [getNodeRelationIds $item "Concept" "endNodeId"]
	}
	$resultMap put "concepts" $concepts
	$resultMap put "maxScore" $maxScore
	return $resultMap
}

set resultMap [java::new HashMap]
java::try {
	set resultMap [getItemConcepts "numeracy" $itemId $contentId]
	set conceptsObj [$resultMap get "concepts"]
	set concepts [java::cast {List} $conceptsObj]
	if {[$concepts size] == 0} {
		set resultMap [getItemConcepts "literacy_v2" $itemId $contentId]
	}
} catch {Exception err} {
    java::try {
    	set resultMap [getItemConcepts "literacy_v2" $itemId $contentId]
    } catch {Exception err} {
    	puts [$err getMessage]
    	$resultMap put "error" [$err getMessage]
    }
}	

set responseList [create_response $resultMap] 
return $responseList