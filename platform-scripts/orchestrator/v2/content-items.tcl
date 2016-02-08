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

proc getSetMemberIds {graphId setId} {
	set response [getSetMembers $graphId $setId]
	set check_error [check_response_error $response]
	if {$check_error} {
		java::throw [java::new Exception "Error response from getSetMembers"]
	}
	set members [get_resp_value $response "members"]
	return $members
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

proc getItem {graphId itemId} {
	set node [getNode $graphId $itemId]
	set item [java::prop $node "metadata"]
	$item put "tags" [java::prop $node "tags"]
	set concepts [getNodeRelationIds $node "Concept" "endNodeId"]
	$item put "concepts" $concepts
	return $item
}

set graphId "domain"
set resultMap [java::new HashMap]
set items [java::new ArrayList]
java::try {
	set content [getNode $graphId $contentId]
	set itemSetIds [getNodeRelationIds $content "ItemSet" "endNodeId"]
	java::for {String itemSetId} $itemSetIds {
		set itemIds [getSetMemberIds $graphId $itemSetId]
		if {[$itemIds size] > 0} {
			java::for {String itemId} $itemIds {
				$items add [getItem $graphId $itemId]
			}
		}
	}
} catch {Exception err} {
    puts [$err getMessage]
    $resultMap put "error" [$err getMessage]
}	

$resultMap put "items" $items
set responseList [create_response $resultMap] 
return $responseList