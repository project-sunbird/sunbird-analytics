package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkSpec
import org.neo4j.spark.Neo4j

class TestContentOwnerRelationModel extends SparkSpec(null) {
  
    it should "create Owner nodes and 'createdBy' relation with contents" in {

        ContentOwnerRelationModel.main("{}")(Option(sc));
    }
}