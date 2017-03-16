package org.ekstep.analytics.api.recommend

import org.ekstep.analytics.api.IRecommendations
import org.ekstep.analytics.api.RequestBody
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.ekstep.analytics.api.util.ContentCacheUtil
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.APIIds
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.ResponseCode
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

object ContentCreationRecommendations extends IRecommendations {

    def isValidRequest(requestBody: RequestBody): Validation = {
        val context = requestBody.request.context.getOrElse(Map());
        val authorid = context.getOrElse("authorid", "").asInstanceOf[String];
        if (StringUtils.isEmpty(authorid))
            Validation(false, Option("authorid should be present"));
        else
            Validation(true);
    }

    def fetch(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String = {
        val validation = isValidRequest(requestBody)
        if (validation.value) {
            val context = requestBody.request.context.getOrElse(Map());
            val authorId = context.getOrElse("authorid", "").asInstanceOf[String];
            //val result1 = GraphQueryDispatcher.dispatch(getGraphDBConfig, "query");
            //val ls = result1.list().toArray().map(x => x.asInstanceOf[org.neo4j.driver.v1.Record]).map { x => x.asInstanceOf[Map[String, AnyRef]] }.toList
            val list = List(Map("langage" -> "Hindi", "concept" -> "LO52", "contenttype" -> "Story", "gradelevel" -> "Grade 1"), Map("langage" -> "Hindi", "concept" -> "LO54", "contenttype" -> "worksheet", "gradelevel" -> "Grade 4"))
            val result = applyLimit(list, list.size, getLimit(requestBody))
            JSONUtils.serialize(CommonUtil.OK(APIIds.CONTENT_CREATION, Map[String, AnyRef]("recommendations" -> result, "context" -> Map("authorid" -> authorId), "count" -> Int.box(list.size))));
        } else {
            CommonUtil.errorResponseSerialized(APIIds.CONTENT_CREATION, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString());
        }
    }

    def applyLimit(contents: List[Map[String, Any]], total: Int, limit: Int)(implicit config: Config): List[Map[String, Any]] = {
        contents.take(limit);
    }

    private def getGraphDBConfig(): Map[String, String] = {
        Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));
    }
}