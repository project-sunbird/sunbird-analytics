package org.ekstep.analytics.framework

class TestIBatchModelTemplate extends SparkSpec {

    "IBatchModelTemplate" should "group data by did" in {

        val rdd = SampleModelTemplate.execute(events, None);
        rdd.count should be(7);

    }
}