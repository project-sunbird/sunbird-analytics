package org.ekstep.analytics.model

import org.ekstep.ilimi.analytics.framework.SparkSpec
import java.io.FileWriter

/**
 * @author Santhosh
 */
class TestGenericScreenerSummary extends SparkSpec {
    
    "GenericScreenerSummary" should "produce akshara measured events" in {
        val screener = new GenericScreenerSummary();
        val rdd = screener.execute(sc, events, Option(Map("contentId" -> "numeracy_377")));
        rdd.count() should be (54);
        val me = rdd.collect();
        val fw = new FileWriter("test_output.txt", true);
        for(e <- me) {
            //Console.println(e);
            fw.write(e + "\n");
        }
        fw.close();
    }
  
}