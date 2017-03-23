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

object CreationRecommendations extends IRecommendations {

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
            val result = getMockResponse(getLimit(requestBody));
            JSONUtils.serialize(CommonUtil.OK(APIIds.CREATION_RECOMMENDATIONS, Map[String, AnyRef]("requests" -> result)));
        } else {
            CommonUtil.errorResponseSerialized(APIIds.CREATION_RECOMMENDATIONS, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString());
        }
    }
    
    private def getMockResponse(limit: Int) : List[Map[String, AnyRef]] = {
    	val languages = List(Map("code" -> "as","name" -> "Assamese"),Map("code" -> "bn","name" -> "Bengali"),Map("code" -> "en","name" -> "English"),Map("code" -> "gu","name" -> "Gujarati"),Map("code" -> "hi","name" -> "Hindi"),Map("code" -> "ka","name" -> "Kannada"),Map("code" -> "mr","name" -> "Marathi"),Map("code" -> "or","name" -> "Odia"),Map("code" -> "ta","name" -> "Tamil"),Map("code" -> "te","name" -> "Telugu"));
        val concepts = List("Num:C2:SC1","Num:C4:SC6","Num:C1:SC2:MC12","Num:C1:SC3:MC5","Num:C2:SC1:MC6","Num:C3:SC6","Num:C1:SC2:MC3","Num:C1:SC2:MC20","Num:C1:SC3:MC13","Num:C2:SC1:MC14","Num:C1:SC3","Num:C1:SC2:MC11","Num:C4:SC7","Num:C2:SC1:MC5","Num:C1:SC3:MC6","Num:C3:SC5","Num:C1:SC2:MC19","Num:C1:SC2:MC4","Num:C2:SC1:MC13","Num:C1:SC3:MC14","Num:C4:SC4","Num:C1:SC2:MC10","Num:C2:SC3","Num:C1:SC3:MC7");
        val contentTypes = List("Story", "WorkSheet", "Game");
        val gradeLevels = List("Grade 1", "Grade 2", "Grade 3", "Grade 4", "Grade 5");
        val random = scala.util.Random
        val mockResponse = for (i <- 1 to limit) yield {
        	Map("type" -> "Content", 
        		"language" -> languages(random.nextInt(languages.size)), 
        		"concepts" -> List(concepts(random.nextInt(concepts.size))),
        		"contentType" -> contentTypes(random.nextInt(contentTypes.size)), 
        		"gradeLevel" -> List(gradeLevels(random.nextInt(gradeLevels.size))));
        };
        mockResponse.toList
    }

    def applyLimit(contents: List[Map[String, Any]], total: Int, limit: Int)(implicit config: Config): List[Map[String, Any]] = {
        contents.take(limit);
    }
}