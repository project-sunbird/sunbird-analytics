package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.api.Response
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.util.CommonUtil

/**
 * @author mahesh
 */

object RecommendationAPIService {
  def recommendations(requestBody: String)(implicit sc: SparkContext): String = {
    val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
    val result = Map[String, AnyRef](
      "content" -> Array[Map[String, AnyRef]](Map[String, AnyRef]("id" -> "1", "name"-> "Moon and the Cap"), Map[String, AnyRef]("id" -> "2", "name"-> "Sringeri Srinivas")));
    JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.recommendations", result));
  }
}