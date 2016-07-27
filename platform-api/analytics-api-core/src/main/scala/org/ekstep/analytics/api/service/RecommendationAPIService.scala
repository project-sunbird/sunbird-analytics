package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.ekstep.analytics.api._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.util.CommonUtil
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.RestUtil

/**
 * @author mahesh
 */

object RecommendationAPIService {
  def recommendations(requestBody: String)(implicit sc: SparkContext, config: Map[String, String]): String = {
    val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
    val context = reqBody.request.context;
    val device_id = context.get("device_id");
    val dlang = context.get("dlang");
    // TODO: find right way of empty check. May be using StringUtils
    if (null == device_id || "" == device_id || null == dlang || "" == dlang) {
      throw new Exception("device_id or dlang is missing.");
    }

    val deviceRDD = sc.cassandraTable[(List[(String, Double)])](Constants.DEVICE_DB, Constants.DEVICE_RECOS_TABLE).select("scores").where("device_id = ?", device_id).cache();
    val rec = deviceRDD.collect();
    val result = Map[String, AnyRef](
      "content" -> rec);
    JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.recommendations", result));
  }
}