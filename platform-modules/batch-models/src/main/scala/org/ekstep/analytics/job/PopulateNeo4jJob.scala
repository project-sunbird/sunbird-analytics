package org.ekstep.analytics.job

import org.neo4j.spark._
import org.apache.spark.SparkContext

object PopulateNeo4jJob {

    def main(config: String)(implicit sc: SparkContext) {
        val neo = Neo4j(sc)
    }
}