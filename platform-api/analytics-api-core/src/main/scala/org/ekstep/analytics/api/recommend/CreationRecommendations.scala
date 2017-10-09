package org.ekstep.analytics.api.recommend

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api.APIIds
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.CreationRequest
import org.ekstep.analytics.api.CreationRequestList
import org.ekstep.analytics.api.IRecommendations
import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.DBUtil
import org.ekstep.analytics.api.util.JSONUtils

import com.typesafe.config.Config
import java.util.ArrayList

object CreationRecommendations extends IRecommendations {

    def isValidRequest(requestBody: RequestBody): Validation = {
        val context = requestBody.request.context.getOrElse(Map());
        val authorid = context.getOrElse("uid", "").asInstanceOf[String];
        if (StringUtils.isEmpty(authorid))
            Validation(false, Option("uid should be present"));
        else
            Validation(true);
    }

    def fetch(requestBody: RequestBody)(implicit config: Config): String = {
        val validation = isValidRequest(requestBody)
        if (validation.value) {
            val context = requestBody.request.context.getOrElse(Map());
            val authorId = context.getOrElse("uid", "").asInstanceOf[String];
           	
           	val requestsFromCassandra = DBUtil.cluster.connect(Constants.PLATFORML_DB).execute("select requests from " + Constants.REQUEST_RECOS_TABLE + " where uid = '" + authorId + "';").asScala
           	val getrequests = requestsFromCassandra.map(row => row.getObject("requests").asInstanceOf[ArrayList[Map[String,AnyRef]]]).map(f => f.asScala).flatMap(f => f).toList
            val result = applyLimit(getrequests, getrequests.size, getLimit(requestBody));
            JSONUtils.serialize(CommonUtil.OK(APIIds.CREATION_RECOMMENDATIONS, Map[String, AnyRef]("requests" -> result)));
        } else {
            CommonUtil.errorResponseSerialized(APIIds.CREATION_RECOMMENDATIONS, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString());
        }
    }

    def applyLimit(contents: List[Map[String, Any]], total: Int, limit: Int)(implicit config: Config): List[Map[String, Any]] = {
        contents.take(limit);
    }

    private def getRequestList(list: List[CreationRequest]): List[Map[String, AnyRef]] = {
        val requests = for (creation <- list) yield {
            Map("type" -> creation.`type`,
                "language" -> creation.language,
                "concepts" -> creation.concepts,
                "contentType" -> creation.content_type,
                "gradeLevel" -> creation.grade_level)
        }
        requests.toList
    }
    
}