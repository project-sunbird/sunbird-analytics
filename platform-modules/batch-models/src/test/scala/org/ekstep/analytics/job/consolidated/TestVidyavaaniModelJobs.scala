package org.ekstep.analytics.job.consolidated

import org.ekstep.analytics.model.SparkSpec

class TestVidyavaaniModelJobs extends SparkSpec(null) {
    
    it should "run Vidyavaani Neo4j Enhancement Jobs and does not throw any exception" in {
        VidyavaaniModelJobs.main("{}")(Option(sc));    
    }
}