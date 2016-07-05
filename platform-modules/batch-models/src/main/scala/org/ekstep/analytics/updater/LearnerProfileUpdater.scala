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

/**
 * @author Santhosh
 */
case class LearnerProfile(learner_id: String, did: String, gender: Option[String], language: Option[String], loc: Option[String], standard: Int, age: Int, year_of_birth: Int, group_user: Boolean, anonymous_user: Boolean, created_date: Option[DateTime], updated_date: Option[DateTime]) extends Output with AlgoOutput;

object LearnerProfileUpdater extends IBatchModelTemplate[ProfileEvent, ProfileEvent, LearnerProfile, LearnerProfile] with Serializable {

    val className = "org.ekstep.analytics.updater.LearnerProfileUpdater"
    override def name: String = "LearnerProfileUpdater"
    
    override def preProcess(data: RDD[ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ProfileEvent] = {
        DataFilter.filter(data, Filter("eid", "IN", Option(List("GE_CREATE_USER", "GE_CREATE_PROFILE", "GE_UPDATE_PROFILE")))).cache();
    }

    override def algorithm(data: RDD[ProfileEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LearnerProfile] = {

        val userEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("GE_CREATE_USER"))).map { event =>
            LearnerProfile(event.edata.eks.uid, event.did, None, None, Option(event.edata.eks.loc), -1, -1, -1, false, true, Option(new DateTime(CommonUtil.getTimestamp(event.ts))), Option(new DateTime(CommonUtil.getTimestamp(event.ts))));
        }
        userEvents.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE, SomeColumns("learner_id", "did", "gender", "language", "loc", "standard", "age", "year_of_birth", "group_user", "anonymous_user", "created_date", "updated_date"));

        val newProfileEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("GE_CREATE_PROFILE"))).map { event =>
            LearnerProfile(event.edata.eks.uid, event.did, Option(event.edata.eks.gender), Option(event.edata.eks.language), Option(event.edata.eks.loc), event.edata.eks.standard, event.edata.eks.age, getYearOfBirth(event), event.edata.eks.is_group_user, false, None, Option(new DateTime(CommonUtil.getTimestamp(event.ts))));
        }
        newProfileEvents.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE, SomeColumns("learner_id", "did", "gender", "language", "loc", "standard", "age", "year_of_birth", "group_user", "anonymous_user", "updated_date"));

        val updProfileEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("GE_UPDATE_PROFILE"))).map { event =>
            LearnerProfile(event.edata.eks.uid, event.did, Option(event.edata.eks.gender), Option(event.edata.eks.language), Option(event.edata.eks.loc), event.edata.eks.standard, event.edata.eks.age, getYearOfBirth(event), event.edata.eks.is_group_user, false, None, Option(new DateTime(CommonUtil.getTimestamp(event.ts))));
        }
        updProfileEvents.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE, SomeColumns("learner_id", "did", "gender", "language", "loc", "standard", "age", "year_of_birth", "group_user", "anonymous_user", "updated_date"));

        newProfileEvents.union(updProfileEvents);
    }

    override def postProcess(data: RDD[LearnerProfile], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LearnerProfile] = {
        data
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