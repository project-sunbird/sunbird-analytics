package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Event
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

case class ItemSummaryInput(uid: String, event: Event) extends AlgoInput
case class ItemSummaryOutput(uid: String, itemId: String, itype: Option[AnyRef], ilevel: Option[AnyRef], timeSpent: Option[Double], exTimeSpent: Option[AnyRef], res: Array[String], exRes: Option[AnyRef], incRes: Option[AnyRef], mc: Option[AnyRef], score: Int, time_stamp: Option[Long], maxScore: Option[AnyRef], domain: Option[AnyRef], ts: Long, syncts: Long, gameId: String, gameVer: String) extends AlgoOutput

object ItemSummary extends IBatchModelTemplate[Event, ItemSummaryInput, ItemSummaryOutput, MeasuredEvent] with Serializable {

    override def name() : String = "ItemSummarizer";
    
    /**
     * Get item from broadcast item mapping variable
     */
    private def getItem(itemMapping: Map[String, Item], event: Event): Item = {
        val item = itemMapping.getOrElse(event.edata.eks.qid, null);
        if (null != item) {
            return item;
        }
        return Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]()));
    }

    private def getItemData(contents: Array[Content], games: Array[String], apiVersion: String = "v2"): Map[String, Item] = {

        val gameIds = contents.map { x => x.id };
        val codeIdMap: Map[String, String] = contents.map { x => (x.metadata.get("code").get.asInstanceOf[String], x.id) }.toMap;
        val contentItems = games.map { gameId =>
            {
                if (gameIds.contains(gameId)) {
                    (gameId, ContentAdapter.getContentItems(gameId, apiVersion))
                } else if (codeIdMap.contains(gameId)) {
                    (gameId, ContentAdapter.getContentItems(codeIdMap.get(gameId).get, apiVersion))
                } else {
                    null;
                }
            }
        }.filter(x => x != null).filter(_._2 != null).toMap;

        if (contentItems.size > 0) {
            contentItems.map(f => {
                f._2.map { item =>
                    (item.id, item)
                }
            }).reduce((a, b) => a ++ b).toMap;
        } else {
            Map[String, Item]();
        }
    }

    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ItemSummaryInput] = {
        val filteredData = DataFilter.filter(data, Array(Filter("uid", "ISNOTEMPTY", None), Filter("eventId", "EQ", Option("OE_ASSESS"))));
        filteredData.map { x => ItemSummaryInput(x.uid, x) };
    }

    override def algorithm(data: RDD[ItemSummaryInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ItemSummaryOutput] = {

        val gameList = data.map { x => x.event }.map { x => x.gdata.id }.distinct().collect();
        val contents = ContentAdapter.getAllContent();
        val itemData = getItemData(contents, gameList, "v2");
        val itemMapping = sc.broadcast(itemData);

        val itemRes = data.map { x =>
            val event = x.event
            val gameId = event.gdata.id
            val gameVer = event.gdata.ver
            val telemetryVer = event.ver;
            val itemObj = getItem(itemMapping.value, event);
            val metadata = itemObj.metadata;
            val res = telemetryVer match {
                case "2.0" =>
                    if (null == event.edata.eks.resvalues) Array[String](); else event.edata.eks.resvalues.flatten.map { x => (x._1 + ":" + x._2.toString) };
                case _ =>
                    event.edata.eks.res;
            }
            ItemSummaryOutput(event.uid, event.edata.eks.qid, metadata.get("type"), metadata.get("qlevel"), CommonUtil.getTimeSpent(event.edata.eks.length), metadata.get("ex_time_spent"), res, metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, event.edata.eks.score, Option(CommonUtil.getEventTS(event)), metadata.get("max_score"), metadata.get("domain"), CommonUtil.getTimestamp(event.ts), CommonUtil.getEventSyncTS(event), gameId, gameVer);
        }
        itemRes;
    }

    override def postProcess(data: RDD[ItemSummaryOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        data.map { itemData =>
            val mid = CommonUtil.getMessageId("ME_ITEM_SUMMARY", itemData.itemId, itemData.uid, itemData.ts);
            val measures = Map(
                "itemId" -> itemData.itemId,
                "itype" -> itemData.itype,
                "ilevel" -> itemData.ilevel,
                "timeSpent" -> itemData.timeSpent,
                "exTimeSpent" -> itemData.exTimeSpent,
                "res" -> itemData.res,
                "exRes" -> itemData.exRes,
                "incRes" -> itemData.exRes,
                "mc" -> itemData.mc.getOrElse(Array[AnyRef]()),
                "score" -> itemData.score,
                "time_stamp" -> itemData.time_stamp,
                "maxScore" -> itemData.maxScore,
                "domain" -> itemData.domain);
            MeasuredEvent("ME_ITEM_SUMMARY", System.currentTimeMillis(), itemData.syncts, "1.0", mid, itemData.uid, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ItemSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "EVENT", DtRange(itemData.ts, itemData.ts)),
                Dimensions(None, None, Option(new GData(itemData.gameId, itemData.gameVer)), None, None, None), MEEdata(measures));
        };
    }
}