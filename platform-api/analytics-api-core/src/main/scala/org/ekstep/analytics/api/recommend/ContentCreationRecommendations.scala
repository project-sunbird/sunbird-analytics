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

object ContentCreationRecommendations extends IRecommendations {

    def isValidRequest(requestBody: RequestBody): Validation = {
        Validation(true);
    }
    
    def fetch(requestBody: RequestBody)(implicit sc: SparkContext, config: Config): String = {
        if (hasRequired(requestBody)) {
            val context = requestBody.request.context.getOrElse(Map());
            val authorId = context.getOrElse("authorid", "").asInstanceOf[String];
            val authorContext = getauthorContext(authorId)
            JSONUtils.serialize(CommonUtil.OK(APIIds.CONTENT_CREATION, Map[String, AnyRef]("recommendations" -> List(), "context" -> authorContext, "count" -> Int.box(5))));
        } else {
            CommonUtil.errorResponseSerialized(APIIds.CONTENT_CREATION, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString());
        }
    }

    def applyLimit(contents: List[Map[String, Any]], total: Int, limit: Int)(implicit config: Config): List[Map[String, Any]] = {
        return null
    }

    private def hasRequired(requestBody: RequestBody): Boolean = {
        val context = requestBody.request.context.getOrElse(Map());
        val authorid = context.get("authorid");
        if (authorid.isEmpty) false else true;
    }

    private def getauthorContext(authorId: String): Map[String, Any] = {
        Map("authorid" -> authorId)
    }

}