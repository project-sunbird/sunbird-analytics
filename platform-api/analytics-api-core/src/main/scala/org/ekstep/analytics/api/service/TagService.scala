package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector

case class RegisteredTag(tag_id: String, last_updated: DateTime, active: Boolean);

object TagService {
  
    def registerTag(tagId: String)(implicit sc: SparkContext) : String = {
        val rdd = sc.makeRDD(Seq(RegisteredTag(tagId, DateTime.now(DateTimeZone.UTC), true)));
        rdd.saveToCassandra(Constants.CONTENT_DB, Constants.REGISTERED_TAGS);
        JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.tag-register", Map("message" -> "Tag registered successfully")));
    }
    
    def deleteTag(tagId: String)(implicit sc: SparkContext) : String = {
        
        val rdd = sc.makeRDD(Seq(RegisteredTag(tagId, DateTime.now(DateTimeZone.UTC), false)));
        rdd.saveToCassandra(Constants.CONTENT_DB, Constants.REGISTERED_TAGS);
        JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.tag-delete", Map("message" -> "Tag deleted successfully")));
    }
}