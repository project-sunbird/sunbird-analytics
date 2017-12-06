package org.ekstep.analytics.updater

import scala.reflect.runtime.universe
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.UpdaterOutput
import org.ekstep.analytics.framework.V3ProfileEvent
import org.ekstep.analytics.framework.exception.DataFilterException
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.LearnerProfile

object UpdateLearnerProfileCreateDB extends IBatchModelTemplate[V3ProfileEvent, V3ProfileEvent, LearnerProfile, UpdaterOutput] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateLearnerProfileCreateDB"
    override def name: String = "UpdateLearnerProfileCreateDB"

    override def preProcess(data: RDD[V3ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[V3ProfileEvent] = {
        DataFilter.filter(data, Array(Filter("eid", "EQ", Option("AUDIT")), Filter("actor.id", "ISNOTEMPTY", None))).filter { x =>
            val objType = x.`object`.get.`type`
            "User".equals(objType) || objType.isEmpty()
        }.cache();
    }

    override def algorithm(data: RDD[V3ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LearnerProfile] = {

        val userEvents = data.map { event =>
            val appId = CommonUtil.getAppDetails(event).id
            val channel = CommonUtil.getChannelId(event)
            LearnerProfile(event.actor.id, appId, channel, event.context.did.getOrElse(""), None, None, None, -1, -1, -1, false, true, Option(new DateTime(event.ets)), Option(new DateTime(event.ets)));
        }
        userEvents.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE, SomeColumns("learner_id", "app_id", "channel", "did", "gender", "language", "loc", "standard", "age", "year_of_birth", "group_user", "anonymous_user", "created_date", "updated_date"));
        userEvents;
    }

    override def postProcess(data: RDD[LearnerProfile], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[UpdaterOutput] = {
        sc.parallelize(Seq(UpdaterOutput("ProfileCreater: Learner Profile database updated - " + data.count())));
    }
}