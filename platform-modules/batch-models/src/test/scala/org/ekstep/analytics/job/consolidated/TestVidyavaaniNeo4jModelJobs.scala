package org.ekstep.analytics.job.consolidated

import org.ekstep.analytics.model.SparkSpec

class TestVidyavaaniNeo4jModelJobs extends SparkSpec(null) {
    
    VidyavaaniNeo4jModelJobs.main("{}")(Option(sc));
}