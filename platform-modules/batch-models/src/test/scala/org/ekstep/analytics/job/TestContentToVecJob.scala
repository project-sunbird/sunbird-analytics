package org.ekstep.analytics.job

import org.ekstep.analytics.model.SparkSpec

class TestContentToVecJob extends SparkSpec(null) {
  
    ignore should "populate to ContentToVec DB" in {
        ContentToVecJob.main(null);
    }
    
}