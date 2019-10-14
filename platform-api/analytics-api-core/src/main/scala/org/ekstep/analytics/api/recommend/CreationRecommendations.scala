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
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.UDTValue

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
            val query = QueryBuilder.select().all().from(Constants.PLATFORM_DB, Constants.REQUEST_RECOS_TABLE).where(QueryBuilder.eq("uid", QueryBuilder.bindMarker())).toString()
			val ps = DBUtil.session.prepare(query)
			val requestsFromCassandra = DBUtil.session.execute(ps.bind(authorId)).asScala
			val getrequests = requestsFromCassandra.map(row => row.getList("requests", classOf[UDTValue]).asInstanceOf[ArrayList[UDTValue]]).map(f => f.asScala).flatMap(f => f).toList
           	val convertedData = getrequests.map(f => changeToMap(f))   
            val result = applyLimit(convertedData, getrequests.size, getLimit(requestBody));
            JSONUtils.serialize(CommonUtil.OK(APIIds.CREATION_RECOMMENDATIONS, Map[String, AnyRef]("requests" -> result)));
        } else {
            CommonUtil.errorResponseSerialized(APIIds.CREATION_RECOMMENDATIONS, "context required data is missing.", ResponseCode.CLIENT_ERROR.toString());
        }
    }

    def applyLimit(contents: List[Map[String, Any]], total: Int, limit: Int)(implicit config: Config): List[Map[String, Any]] = {
        contents.take(limit);
    }
    
    private def changeToMap(row : UDTValue) : Map[String, AnyRef] = {
        Map("type" -> row.getString("type"),
            "contentType" -> row.getString("content_type"),
            "gradeLevel" -> row.getList("grade_level", classOf[String]),
            "concepts" -> row.getList("concepts", classOf[String]),
            "language" -> row.getMap("language", classOf[String], classOf[String]))
    }
}