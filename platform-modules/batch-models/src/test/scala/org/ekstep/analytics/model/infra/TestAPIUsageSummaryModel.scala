/**
 * @Jitendra Singh Sankhwar
 **/
package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.util.BEEvent



class TestAPIUsageSummaryModel extends SparkSpec(null) {

    //"TestAPIUsageSummaryModel" should "generate api usage summary" in {
    ignore should "generate api usage summary" in {

        val rdd1 = loadFile[BEEvent]("src/test/resources/api-usage/test_data.log");
        val rdd2 = APIUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(1);
        val data = me(0)
        
        data should be (Empty())
    }
}