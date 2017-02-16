package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import akka.actor.Actor

case class RegisteredTag(tag_id: String, last_updated: DateTime, active: Boolean);

object TagService {
    case class RegisterTag(tag: String, sc: SparkContext)
    case class DeleteTag(tag: String, sc: SparkContext)
    
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

class TagService extends Actor {
  import TagService._
  
  def receive = {
    case RegisterTag(tag: String, sc: SparkContext) => sender() ! registerTag(tag)(sc)
    case DeleteTag(tag: String, sc: SparkContext) => sender() ! deleteTag(tag)(sc)
  }
}