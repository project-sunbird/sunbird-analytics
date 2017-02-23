package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkSpec
import org.neo4j.spark.Neo4j

class TestContentLanguageRelationModel extends SparkSpec(null) {
  
    it should "create Language nodes and 'belongsTo' relation with contents" in {

        ContentLanguageRelationModel.main("{}")(Option(sc));       
    }
}