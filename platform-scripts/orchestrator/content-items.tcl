package require java

java::import -package java.util ArrayList List
java::import -package java.util HashMap Map
java::import -package com.ilimi.graph.dac.model Filter
java::import -package com.ilimi.graph.dac.model Node
java::import -package com.ilimi.graph.dac.model Relation

proc getOutRelations {graphId nodeId} {
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

proc getNodeRelationIds {graphId nodeId relationType property} {

	set relationIds [java::new ArrayList]
	set outRelations [getOutRelations $graphId $nodeId]
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

set resultMap [java::new HashMap]
set items [java::new ArrayList]
java::try {
	set questionnaires [getNodeRelationIds $graphId $contentId "Questionnaire" "endNodeId"]
	if {[$questionnaires size] > 0} {
		java::for {String questionnaireId} $questionnaires {
			set qm [getNodeRelationIds $graphId $questionnaireId "ItemSet" "endNodeMetadata"]
			if {[$qm size] > 0} {
				set endNodeMetadataObj [$qm get 0]
				set endNodeMetadata [java::cast Map $endNodeMetadataObj]
				set itemIdsObj [$endNodeMetadata get "items"]
				set itemIds [java::cast {String[]} $itemIdsObj]
				if {[$itemIds length] > 0} {
					java::for {String itemId} $itemIds {
						$items add $itemId
					}
				}
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