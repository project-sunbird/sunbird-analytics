package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.api.Params
import org.ekstep.analytics.api.ContentSummary
import org.ekstep.analytics.api.Trend
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import org.apache.spark.SparkContext

/**
 * @author Santhosh
 */

object ContentAPIService {
    
    @transient val df: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").withZoneUTC();
 
    def getContentUsageMetrics(contentId: String, requestBody: String)(implicit sc: SparkContext) : String = {
        val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
        JSONUtils.serialize(mockResponse(reqBody));
    }
    
    private def mockResponse(reqBody: RequestBody) : Response = {
        val measures = ContentSummary(23.43, 134, 500.12, 2000, 134.34, None, None);
        val reqTrend:Trend = reqBody.request.trend.getOrElse(Trend(Option(7), Option(5), Option(12)));
        val trend = Map[String, Int] ("day" -> reqTrend.day.getOrElse(0), "week" -> reqTrend.week.getOrElse(0), "month" -> reqTrend.month.getOrElse(0));
        val summaries = reqBody.request.summaries.getOrElse(Array("day","week","month","cumulative"));
        val result = Map[String, AnyRef](
            "ttl" -> 24.0.asInstanceOf[AnyRef],
            "summaries" -> summaries.map { x =>  
                (x, measures);
            }.toMap,
            "trend" -> trend.mapValues { x =>  
                if(x > 0) {
                    for (i <- 1 to x) yield measures   
                } else {
                    Array();
                }
            }
        );
        Response("ekstep.analytics.contentusagesummary", "1.0", df.print(System.currentTimeMillis()), Params("054f3b10-309f-4552-ae11-02c66640967b", "h39r3n32-930g-3822-bx32-32u83923821t", null, "successful", null), result);
    }
    
}