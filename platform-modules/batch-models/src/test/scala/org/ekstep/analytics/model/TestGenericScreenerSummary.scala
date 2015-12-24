package org.ekstep.analytics.model

import org.ekstep.ilimi.analytics.framework.SparkSpec
import java.io.FileWriter
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.MeasuredEvent
import org.ekstep.ilimi.analytics.framework.util.JSONUtils
import org.ekstep.ilimi.analytics.framework.JobContext

/**
 * @author Santhosh
 */
class TestGenericScreenerSummary extends SparkSpec {
    
    "GenericScreenerSummary" should "produce akshara measured events" in {
        JobContext.deviceMapping = events.filter { x => "GE_GENIE_START".equals(x.eid) }.map { x => (x.did, x.edata.eks.loc) }.collect().toMap;
        val screener = new GenericScreenerSummary();
        val rdd = screener.execute(sc, events, Option(Map("contentId" -> "numeracy_377", "modelVersion" -> "1.1", "modelId" -> "GenericContentSummary")));
        rdd.count() should be (54);
        val me = rdd.collect();
        val fw = new FileWriter("test_output.txt", true);
        for(e <- me) {
            Console.println(e);
            fw.write(e + "\n");
        }
        fw.close();
    }
    
    def computeMaxLevelsInAserData(args: Array[String]): Unit = {
        val sc = CommonUtil.getSparkContext(10, "xyz");
        val rdd = sc.textFile("/Users/Santhosh/ekStep/telemetry_processed/aser_lite_summary.log", 1).map { line =>
            {
                try{
                    JSONUtils.deserialize[MeasuredEvent](line);    
                } catch {
                    case ex: Exception => Console.println("line", line);
                    throw ex;
                }
                
            }
        }.cache();
        val levelCounts = rdd.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("currentLevel", Map[String, String]()).asInstanceOf[Map[String, String]].size };
        Console.println(levelCounts.min());
        CommonUtil.closeSparkContext(sc);
    }
  
}