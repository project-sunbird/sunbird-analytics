/**
 * @author Santhosh Vasabhaktula
 */
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
import org.ekstep.analytics.creation.model._
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.util.CreationEventUtil

/**
 * Case Classes for the data product
 */
case class AppObjectCache(`type`: String, id: String, app_id: String, channel: String, subtype: String, parentid: Option[String], parenttype: Option[String], code: Option[String], name: String, state: String, prevstate: String, updated_date: Option[DateTime]) extends Output with AlgoOutput;
case class UserProfile(user_id: String, app_id: String, channel: String, name: String, email: Option[String], access: String, partners: String, profile: String, updated_date: Option[DateTime]) extends Output with AlgoOutput;

/**
 * @dataproduct
 * @updater
 *
 * UpdateAppObjectCacheDB
 *
 * Functionality
 * 1. Updater to populate/update the app specific object cache. This would be used to denormalize object data when pushing the data to influx or when the metrics are served via the APIs
 * 2. Store user profile sent by the app
 * Events used - BE_OBJECT_LIFECYCLE & CP_UPDATE_PROFILE
 */
object UpdateAppObjectCacheDB extends IBatchModelTemplate[CreationEvent, CreationEvent, AppObjectCache, UpdaterOutput] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateAppObjectCacheDB"
    override def name: String = "UpdateAppObjectCacheDB"

    override def preProcess(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CreationEvent] = {
        DataFilter.filter(data, Filter("eid", "IN", Option(List("BE_OBJECT_LIFECYCLE", "CP_UPDATE_PROFILE")))).cache();
    }

    override def algorithm(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AppObjectCache] = {

        val objectEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("BE_OBJECT_LIFECYCLE")))
            .filter { x => StringUtils.isNotBlank(x.edata.eks.id) && StringUtils.isNotBlank(x.edata.eks.`type`) }
            .map { event =>
                val pdata = CreationEventUtil.getAppDetails(event)
                val channel = CreationEventUtil.getChannelId(event)
                AppObjectCache(event.edata.eks.`type`, event.edata.eks.id, pdata.id, channel, event.edata.eks.subtype, event.edata.eks.parentid, event.edata.eks.parenttype, event.edata.eks.code, event.edata.eks.name, event.edata.eks.state, event.edata.eks.prevstate, Option(new DateTime(event.ets)));
            }
        objectEvents.saveToCassandra(Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE);

        val profileEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("CP_UPDATE_PROFILE"))).map { event =>
            val access = if (event.edata.eks.access.isDefined) event.edata.eks.access.get else List();
            val partners = if (event.edata.eks.partners.isDefined) event.edata.eks.partners.get else List();
            val profile = if (event.edata.eks.profile.isDefined) event.edata.eks.profile.get else List();
            val pdata = CreationEventUtil.getAppDetails(event)
            val channel = CreationEventUtil.getChannelId(event)
            UserProfile(event.uid, pdata.id, channel, event.edata.eks.name, event.edata.eks.email, JSONUtils.serialize(access), JSONUtils.serialize(partners), JSONUtils.serialize(profile), Option(new DateTime(event.ets)));
        }
        profileEvents.saveToCassandra(Constants.CREATION_KEY_SPACE_NAME, Constants.USER_PROFILE_TABLE);
        objectEvents;
    }

    override def postProcess(data: RDD[AppObjectCache], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[UpdaterOutput] = {
        sc.parallelize(Seq(UpdaterOutput("App Object Cache updated - " + data.count())));
    }

}