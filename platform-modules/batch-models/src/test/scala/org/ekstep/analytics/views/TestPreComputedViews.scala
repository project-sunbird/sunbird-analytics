package org.ekstep.analytics.views

import org.ekstep.analytics.model.SparkSpec

class TestPreComputedViews extends SparkSpec(null) {
    
    "PrecomputedViews" should "output views to a file" in {
        
        PrecomputedViews.execute();
    }
  
}