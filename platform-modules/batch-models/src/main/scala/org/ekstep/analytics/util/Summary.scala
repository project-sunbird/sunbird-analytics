package org.ekstep.analytics.util

import org.ekstep.analytics.framework._

import scala.collection.mutable.Buffer
import org.ekstep.analytics.model.{EnvSummary, EventSummary, ItemResponse, PageSummary}
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.CommonUtil

class Summary(val summaryKey: String, val firstEvent: V3Event) {

    val DEFAULT_MODE = "play";
    val sid: String = firstEvent.context.sid.getOrElse("")
    val uid: String = firstEvent.actor.id
    val content_id: Option[String] = if (firstEvent.`object`.isDefined) Option(firstEvent.`object`.get.id) else None;
    val session_type: String = if(firstEvent.edata.`type`.isEmpty) "" else firstEvent.edata.`type`
    val mode: Option[String] = if(firstEvent.edata.mode.isEmpty) Option(DEFAULT_MODE) else Option(firstEvent.edata.mode)
    val telemetry_version: String = firstEvent.ver
    val start_time: Long = firstEvent.ets

    var last_event: V3Event = null
    var item_responses: Buffer[ItemResponse] = Buffer[ItemResponse]()
    var end_time: Long = 0l
    var time_spent: Double = 0.0
    var time_diff: Double = 0.0
    var interact_events_count: Long = 0l
    var env_summary: Iterable[EnvSummary] = Iterable[EnvSummary]()
    var events_summary: Map[String, Long] = Map()
    var page_summary: Iterable[PageSummary] = Iterable[PageSummary]()
    var etags: Option[ETags] = None
    var tmpLastEventEts: Long = start_time
    var lastImpression: V3Event = null
    var impressionMap: Map[V3Event, Double] = Map()

    var CHILD: Buffer[Summary] = null
    var PARENT: Summary = null

    var isClosed: Boolean = false

    def add(event: V3Event, idleTime: Int, itemMapping: Map[String, Item]) {
        val ts = CommonUtil.roundDouble(CommonUtil.getTimeDiff(tmpLastEventEts, event.ets).get, 2)
        this.time_spent += (if (ts > idleTime) 0 else ts)
        if(StringUtils.equals(event.eid, "INTERACT")) this.interact_events_count += 1
        val prevCount = events_summary.get(event.eid).getOrElse(0)
        events_summary += (event.eid -> (prevCount+1) )
        if(lastImpression != null){
            val prevTs = impressionMap.get(lastImpression).getOrElse(0.0)
            impressionMap += (lastImpression -> (prevTs + this.time_spent))
        }
        if(StringUtils.equals(event.eid, "IMPRESSION")) {
            if(lastImpression == null){
                lastImpression = event
                impressionMap += (lastImpression -> 0.0)
            }
            else {
                val prevTs = impressionMap.get(lastImpression).getOrElse(0.0)
                impressionMap += (lastImpression -> (prevTs + this.time_spent))
                lastImpression = event
                impressionMap += (lastImpression -> 0.0)
            }
        }
        this.last_event = event
        this.end_time = this.last_event.ets
        this.time_diff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(this.start_time, this.end_time).get, 2)
        this.page_summary = if(impressionMap.size > 0) {
            impressionMap.map(f => (f._1.edata.pageid, f)).groupBy(x => x._1).map { f =>
                val id = f._1
                val firstEvent = f._2.head._2._1
                val `type` = firstEvent.edata.`type`
                val env = firstEvent.context.env
                val timeSpent = CommonUtil.roundDouble(f._2.map(x => x._2._2).sum, 2)
                val visitCount = f._2.size.toLong
                PageSummary(id, `type`, env, timeSpent, visitCount)
            }
        } else Iterable[PageSummary]()
        this.env_summary = if (page_summary.size > 0) {
            page_summary.groupBy { x => x.env }.map { f =>
                val timeSpent = CommonUtil.roundDouble(f._2.map(x => x.time_spent).sum, 2)
                val count = f._2.map(x => x.visit_count).max;
                EnvSummary(f._1, timeSpent, count)
            }
        } else Iterable[EnvSummary]();

        if(StringUtils.equals(event.eid, "ASSESS")) {
            val itemObj = getItem(itemMapping, event);
            val metadata = itemObj.metadata;
            val resValues = if (null == event.edata.resvalues) Option(Array[Map[String, AnyRef]]().map(f => f.asInstanceOf[AnyRef])) else Option(event.edata.resvalues.map(f => f.asInstanceOf[AnyRef]))
            val res = if (null == event.edata.resvalues) Option(Array[String]()); else Option(event.edata.resvalues.flatten.map { x => (x._1 + ":" + x._2.toString) });
            val item = event.edata.item
            this.item_responses += ItemResponse(item.id, metadata.get("type"), metadata.get("qlevel"), Option(event.edata.duration), Option(Int.box(item.exlength)), res, resValues, metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, Option(item.mmc), event.edata.score, event.ets, metadata.get("max_score"), metadata.get("domain"), event.edata.pass, Option(item.title), Option(item.desc));
        }
    }

    def addChild(child: Summary) {
        CHILD.append(child);
    }

    def setParent(parent: Summary) {
        this.PARENT = parent;
    }

    def getParent(): Summary = {
        return this.PARENT;
    }

    def checkSimilarity(eventKey: String): Boolean = {
        StringUtils.equals(this.summaryKey, eventKey)
    }

    /**
      * Get item from item mapping variable
      */
    private def getItem(itemMapping: Map[String, Item], event: V3Event): Item = {
        val item = itemMapping.getOrElse(event.edata.item.id, null);
        if (null != item) {
            return item;
        }
        return Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]()));
    }

    def computeMetrics(currentSumm: Summary) {

    }

    def rollUpSummary(currentSumm: Summary) {
        if (null == currentSumm.CHILD) {
            computeMetrics(currentSumm)
        } else {
            computeMetrics(currentSumm)
            for (child <- currentSumm.CHILD) {
                if (child.isClosed) {
                    reduce(currentSumm, child)
                } else {
                    rollUpSummary(child)
                }
            }
        }
    }

    private def reduce(current: Summary, child: Summary) {

    }

    def close(idleTime: Int) {
        rollUpSummary(this)
        if(null!=CHILD){
            for (child <- CHILD) {
                if (child.isClosed == false) {
                    child.isClosed = true
                }
            }
        }

        this.isClosed = true
    }
}