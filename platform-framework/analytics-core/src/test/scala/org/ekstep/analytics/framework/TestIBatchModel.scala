package org.ekstep.analytics.framework

class TestIBatchModel extends SparkSpec() {
  
  "IBatchModel" should "return json in string format" in {
        
        val rdd = SampleModel.execute(events, None);
        rdd.count should be (247);
        SampleModel.name() should be ("BatchModel")
        
    }
}