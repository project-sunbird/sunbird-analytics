package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.Item
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.framework.Content
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.GData
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.updater.LearnerProfile
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.util.DerivedEvent

case class ItemSummaryOutput(uid: String, groupUser: Boolean, anonymousUser: Boolean, sid: String, syncts: Long, gdata: GData, did: String, tags: AnyRef, itemResponse: ItemResponse) extends AlgoOutput

object ItemSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ItemSummaryOutput, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.ItemSummaryModel"
    override def name(): String = "ItemSummaryModel";

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ItemSummaryOutput] = {
        data.map { event =>
            val itemResponses = event.edata.eks.itemResponses;
            if (null != itemResponses && itemResponses.size > 0) {
                itemResponses.map { x =>
                    val ir = JSONUtils.deserialize[ItemResponse](JSONUtils.serialize(x));
                    ItemSummaryOutput(event.uid, event.dimensions.group_user, event.dimensions.anonymous_user, event.mid, event.syncts, event.dimensions.gdata, event.dimensions.did, event.tags, ir)
                }
            } else {
                Array[ItemSummaryOutput]();
            }
        }.filter { x => !x.isEmpty }.flatMap { x => x.map { x => x } }
    }

    override def postProcess(data: RDD[ItemSummaryOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_ITEM_SUMMARY", summary.itemResponse.itemId + summary.uid, "EVENT", DtRange(summary.itemResponse.time_stamp, summary.itemResponse.time_stamp), summary.gdata.id);
            val measures = summary.itemResponse;
            val pdata = PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ItemSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]);
            MeasuredEvent("ME_ITEM_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, summary.uid, Option(summary.gdata.id), None,
                Context(pdata, None, "EVENT", DtRange(summary.itemResponse.time_stamp, summary.itemResponse.time_stamp)),
                Dimensions(None, Option(summary.did), Option(summary.gdata), None, None, None, None, Option(summary.groupUser), Option(summary.anonymousUser), None, None, None, Option(summary.sid)), MEEdata(measures), Option(summary.tags));
        };
    }

}