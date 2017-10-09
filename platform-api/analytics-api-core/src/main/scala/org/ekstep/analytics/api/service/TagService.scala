package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.DBUtil
import org.ekstep.analytics.api.util.JSONUtils
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import akka.actor.Actor

case class RegisteredTag(tag_id: String, last_updated: DateTime, active: Boolean);

object TagService {
    case class RegisterTag(tag: String)
    case class DeleteTag(tag: String)

    def registerTag(tagId: String): String = {
        val query = "INSERT INTO " + Constants.CONTENT_DB +"."+ Constants.REGISTERED_TAGS + " (tag_id, last_updated, active) VALUES('" + tagId +"',"+ DateTime.now(DateTimeZone.UTC).getMillis + "," + true + ")"
        val data = DBUtil.session.execute(query)
        JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.tag-register", Map("message" -> "Tag registered successfully")));
    }

    def deleteTag(tagId: String): String = {
        val query = "INSERT INTO " + Constants.CONTENT_DB +"."+ Constants.REGISTERED_TAGS + " (tag_id, last_updated, active) VALUES('" + tagId +"',"+ DateTime.now(DateTimeZone.UTC).getMillis + "," + false + ")"
        val data = DBUtil.session.execute(query)
        JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.tag-delete", Map("message" -> "Tag deleted successfully")));
    }
}

class TagService extends Actor {
    import TagService._

    def receive = {
        case RegisterTag(tag: String) => sender() ! registerTag(tag)
        case DeleteTag(tag: String)   => sender() ! deleteTag(tag)
    }
}