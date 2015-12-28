package org.ekstep.analytics.model


import java.io.FileWriter
import org.ekstep.ilimi.analytics.framework.JobContext
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.SparkSpec

class TestAserScreenSummary extends SparkSpec("src/test/resources/prod.telemetry.unique-2015-12-20-07-47.json"){
 
  "AserScreenSummary" should "produce aser page session" in {
    
        val aserScreener = new AserScreenSummary();
        val rdd = aserScreener.execute(sc, events, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd.collect();
        val fw = new FileWriter("aser_test_output.txt", true);
        for(e <- me) {
            Console.println(e);
            fw.write(e + "\n");
        }
        fw.close();
    }
}