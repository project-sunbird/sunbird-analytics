package org.ekstep.analytics.model

import org.ekstep.ilimi.analytics.framework.SparkSpec
import java.io.FileWriter
import org.ekstep.ilimi.analytics.framework.JobContext
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.DataFilter
import org.ekstep.ilimi.analytics.framework.Filter
import org.ekstep.ilimi.analytics.framework.util.JSONUtils

class TestAserScreenSummary extends SparkSpec("src/test/resources/prod.telemetry.unique-2015-12-20-07-47.json") {

    "AserScreenSummary" should "produce aser page session" in {

        val aserScreener = new AserScreenSummary();
        val rdd = DataFilter.filter(events, Filter("eventId", "IN", Option(Array("OE_START","OE_INTERACT","OE_ASSESS","OE_LEVEL_SET","OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        Console.println("rdd2 count", rdd2.count);
        val me = rdd2.collect();
        val fw = new FileWriter("aser_test_output.txt", true);
        for (e <- me) {
            //Console.println(e);
            fw.write(e + "\n");
        }
        fw.close();
    }

    it should "print events to a file" in {
        val rdd = DataFilter.filter(events, Array(Filter("uid", "EQ", Option("63a2c136-eb3e-4d0c-ae12-f552a1fbda3f")), Filter("eventId", "IN", Option(Array("OE_START","OE_INTERACT","OE_ASSESS","OE_LEVEL_SET","OE_END")))));
        println(rdd.count);
        val aserScreener = new AserScreenSummary();
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        for (e <- me) {
            Console.println(e);
        }
    }
}