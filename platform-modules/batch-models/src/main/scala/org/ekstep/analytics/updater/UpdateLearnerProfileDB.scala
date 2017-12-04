package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.joda.time.DateTime
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.LocalDate

/**
 * @author Santhosh
 */
case class LearnerProfile(learner_id: String, app_id: String, channel: String, did: String, gender: Option[String], language: Option[String], loc: Option[String], standard: Int, age: Int, year_of_birth: Int, group_user: Boolean, anonymous_user: Boolean, created_date: Option[DateTime], updated_date: Option[DateTime]) extends Output with AlgoOutput;

object UpdateLearnerProfileDB extends IBatchModelTemplate[V3ProfileEvent, V3ProfileEvent, LearnerProfile, UpdaterOutput] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateLearnerProfileDB"
    override def name: String = "UpdateLearnerProfileDB"

    override def preProcess(data: RDD[V3ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[V3ProfileEvent] = {
        DataFilter.filter(data, Array(Filter("eid", "EQ", Option("AUDIT")), Filter("actor.id", "ISNOTEMPTY", None))).cache();
    }

    override def algorithm(data: RDD[V3ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LearnerProfile] = {

        val userEvents = data.map { event =>
            val appId = CommonUtil.getAppDetails(event).id
            val channel = CommonUtil.getChannelId(event)
            
            val state = event.edata.state.asInstanceOf[Map[String, AnyRef]]
            
            val groupUserData = state.get("is_group_user")
            val groupUser = if(groupUserData.nonEmpty) groupUserData.get.asInstanceOf[Boolean] else false
            
            val anonymousUser = if (groupUserData.nonEmpty) false else true
            val createdDate = if (anonymousUser) Option(new DateTime(event.ets)) else None
            
            val uid = state.get("uid").getOrElse("").asInstanceOf[String]
            val gender = state.get("gender").getOrElse(null).asInstanceOf[String]
            val language = state.get("language").getOrElse(null).asInstanceOf[String]
            val loc = state.get("loc").getOrElse(null).asInstanceOf[String]
            val standard = state.get("standard").getOrElse(-1).asInstanceOf[Number].intValue()
            val age = state.get("age").getOrElse(-1).asInstanceOf[Number].intValue()
            
            LearnerProfile(uid, appId, channel, event.context.did.getOrElse(""), Option(gender), Option(language), Option(loc), standard, age, getYearOfBirth(event.ets, age), groupUser, anonymousUser, createdDate, Option(new DateTime(event.ets)));
        }
        val newUsers = userEvents.filter { x => true == x.anonymous_user }
        val profileUpdates = userEvents.filter { x => false == x.anonymous_user }
        
        newUsers.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE, SomeColumns("learner_id", "app_id", "channel", "did", "gender", "language", "loc", "standard", "age", "year_of_birth", "group_user", "anonymous_user", "created_date", "updated_date"));
        profileUpdates.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE, SomeColumns("learner_id", "app_id", "channel", "did", "gender", "language", "loc", "standard", "age", "year_of_birth", "group_user", "anonymous_user", "created_date", "updated_date"));
        
        userEvents;
    }

    override def postProcess(data: RDD[LearnerProfile], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[UpdaterOutput] = {
        sc.parallelize(Seq(UpdaterOutput("Learner Profile database updated - " + data.count())));
    }

    private def getYearOfBirth(ets: Long, age: Int): Int = {
        val localDate = new LocalDate(ets);
        if (age > 0) {
            localDate.getYear - age;
        } else {
            -1;
        }
    }

}