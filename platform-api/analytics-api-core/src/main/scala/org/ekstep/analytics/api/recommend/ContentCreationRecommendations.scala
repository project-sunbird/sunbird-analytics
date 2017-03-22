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
import org.ekstep.analytics.framework.DataNode
import scala.collection.JavaConverters._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.JobContext
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.Relation
import org.ekstep.analytics.framework.GraphQueryParams._
import org.ekstep.analytics.framework.util.GraphDBUtil
import scala.collection.mutable.ListBuffer
import scala.util.Random

object ContentCreationRecommendations extends IRecommendations {

    def isValidRequest(requestBody: RequestBody): Validation = {
        val context = requestBody.request.context.getOrElse(Map());
        val authorid = context.getOrElse("uid", "").asInstanceOf[String];
        if (StringUtils.isEmpty(authorid))
            Validation(false, Option("authorid should be present"));
        else
            Validation(true);
    }

    def fetch(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String = {
        val validation = isValidRequest(requestBody)
        if (validation.value) {
            val context = requestBody.request.context.getOrElse(Map());
            val authorId = context.getOrElse("uid", "").asInstanceOf[String];

            val language = List("Hindi", "English", "Telugu", "Tamil", "Kannada", "Marati")
            val concept = List("LO52", "LO89", "LO23", "LO18", "LO34", "LO521")
            val contenttype = List("Story", "WorkSheet", "Template", "Game")
            val gradelevel = List("Grade 1", "Grade 2", "Grade 3", "Grade 4", "Grade 5", "Grade 6")
            var contents = new ListBuffer[Map[String, String]]()
            for (a <- 1 to getLimit(requestBody)) {
                contents += Map("langage" -> Random.shuffle(language).head, "concept" -> Random.shuffle(concept).head, "contenttype" -> Random.shuffle(contenttype).head, "gradelevel" -> Random.shuffle(gradelevel).head)
            }
            val result = applyLimit(contents.toList, contents.size, getLimit(requestBody))
            JSONUtils.serialize(CommonUtil.OK(APIIds.CONTENT_CREATION, Map[String, AnyRef]("recommendations" -> result, "context" -> Map("uid" -> authorId), "count" -> Int.box(contents.size))));
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