package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec

class TestPopulateNeo4jJob extends SparkSpec(null) {

    it should "test update neo4j graph" in {
        PopulateNeo4jJob.main()(Option(sc))
    }
}