/**
 * @Jitendra Singh Sankhwar
 **/
package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Empty



class TestAPIUsageSummaryModel extends SparkSpec(null) {

    "TestAPIUsageSummaryModel" should "generate 1 app session summary events having CE_START & CE_END" in {

        val rdd1 = loadFile[BEEvent]("src/test/resources/api-usage/test_data.log");
        val rdd2 = APIUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(1);
        val data = me(0)
        
        data should be (Empty())
    }
}