package org.ekstep.analytics.updater

import org.joda.time.LocalDate
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.ProfileEvent
import org.ekstep.analytics.framework.LearnerProfile
import org.ekstep.analytics.framework.UpdaterOutput
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.joda.time.DateTime

object UpdateLearnerProfileUpdateDB extends IBatchModelTemplate[ProfileEvent, ProfileEvent, LearnerProfile, UpdaterOutput] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateLearnerProfileUpdateDB"
    override def name: String = "UpdateLearnerProfileUpdateDB"

    override def preProcess(data: RDD[ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ProfileEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("GE_UPDATE_PROFILE"))).cache();
    }

    override def algorithm(data: RDD[ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LearnerProfile] = {
        val updProfileEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("GE_UPDATE_PROFILE"))).map { event =>
            val appId = CommonUtil.getAppDetails(event).id
            val channel = CommonUtil.getChannelId(event)
            LearnerProfile(event.edata.eks.uid, appId, channel, event.did, Option(event.edata.eks.gender), Option(event.edata.eks.language), Option(event.edata.eks.loc), event.edata.eks.standard, event.edata.eks.age, getYearOfBirth(event), event.edata.eks.is_group_user, false, None, Option(new DateTime(CommonUtil.getTimestamp(event.ts))));
        }
        updProfileEvents.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE, SomeColumns("learner_id", "app_id", "channel", "did", "gender", "language", "loc", "standard", "age", "year_of_birth", "group_user", "anonymous_user", "updated_date"));
        updProfileEvents;

    }
    override def postProcess(data: RDD[LearnerProfile], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[UpdaterOutput] = {
        sc.parallelize(Seq(UpdaterOutput("ProfileUpdater: Learner Profile database updated - " + data.count())));
    }

    private def getYearOfBirth(event: ProfileEvent): Int = {
        val localDate = CommonUtil.df6.parseLocalDate(event.ts.substring(0, 19));
        if (event.edata.eks.age > 0) {
            localDate.getYear - event.edata.eks.age;
        } else {
            -1;
        }
    }

}