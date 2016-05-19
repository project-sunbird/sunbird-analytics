package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.JSONUtils

/**
 * @author Santhosh
 */

case class Filter(partner_id: String, group_user: Boolean);
case class Trend(day: Int, week: Int, month: Int)
case class Request(filter: Filter, summaries: Array[String], trend: Trend);
case class RequestBody(id: String, ver: String, ts: String, request: Request);

object ContentAPIService {
 
    def getContentUsageMetrics(contentId: String, requestBody: String) : String = {
        val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
        JSONUtils.serialize(reqBody.request);
    }
    
}