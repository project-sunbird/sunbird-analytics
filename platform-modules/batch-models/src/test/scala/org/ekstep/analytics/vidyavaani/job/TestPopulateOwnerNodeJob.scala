package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkSpec

class TestPopulateOwnerNodeJob extends SparkSpec(null) {
  
    it should "Clear all Owner Nodes in Neo4j" in {
        PopulateOwnerNodeJob.main("{}")(Option(sc));
    }
}