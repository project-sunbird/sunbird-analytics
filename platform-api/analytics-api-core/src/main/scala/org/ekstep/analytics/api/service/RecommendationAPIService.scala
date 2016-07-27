package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.ekstep.analytics.api._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.util.CommonUtil
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.RestUtil
import org.apache.spark.rdd.RDD

/**
 * @author mahesh
 */

object RecommendationAPIService {

    var contentRDD: RDD[(String, Map[String, AnyRef])] = null;

    def initCache()(implicit sc: SparkContext, config: Map[String, String]) {

        val baseUrl = config.get("service.search.url").get;
        val searchUrl = s"$baseUrl/v2/search";
        val request = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "limit" -> 1000));
        val resp = RestUtil.post[Response](searchUrl, JSONUtils.serialize(request));
        val contentList = resp.result.getOrElse(Map("content" -> List())).getOrElse("content", List()).asInstanceOf[List[Map[String, AnyRef]]];
        contentRDD = sc.parallelize(contentList, 4).map(f => (f.get("identifier").get.asInstanceOf[String], f)).cache();
    }

    def recommendations(requestBody: String)(implicit sc: SparkContext, config: Map[String, String]): String = {
        val reqBody = JSONUtils.deserialize[RequestBody](requestBody);
        val context = reqBody.request.context;
        val did = context.get("did").asInstanceOf[String];
        val dlang = context.get("dlang").asInstanceOf[String];
        if (StringUtils.isBlank(did) || StringUtils.isBlank(dlang)) {
            throw new Exception("did or dlang is missing.");
        }

        val deviceRecos = sc.cassandraTable[(List[(String, Double)])](Constants.DEVICE_DB, Constants.DEVICE_RECOS_TABLE).select("scores").where("device_id = ?", did);
        val result = if (deviceRecos.count() > 0) {
            val deviceRDD = sc.parallelize(deviceRecos.first, 4);
            val contentRecos = deviceRDD.join(contentRDD).map(f => f._2._2);
            val rec = contentRecos.collect();
            Map[String, AnyRef]("content" -> rec);
        } else {
            Map[String, AnyRef]("content" -> Array());
        }

        JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.recommendations", result));
    }
}