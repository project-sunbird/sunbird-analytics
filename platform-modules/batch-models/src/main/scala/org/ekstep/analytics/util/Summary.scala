package org.ekstep.analytics.util

import org.ekstep.analytics.framework._

import scala.collection.mutable.Buffer
import org.ekstep.analytics.model.{ EnvSummary, EventSummary, ItemResponse, PageSummary }
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.CommonUtil

class Summary(val summaryKey: String, val firstEvent: V3Event) {

    val DEFAULT_MODE = "play";
    val sid: String = firstEvent.context.sid.getOrElse("")
    val uid: String = firstEvent.actor.id
    val contentId: Option[String] = if (firstEvent.`object`.isDefined) Option(firstEvent.`object`.get.id) else None;
    val sessionType: String = if (firstEvent.edata.`type`.isEmpty) "" else StringUtils.lowerCase(firstEvent.edata.`type`)
    val mode: Option[String] = if (firstEvent.edata.mode == null) Option(DEFAULT_MODE) else Option(firstEvent.edata.mode)
    val telemetryVersion: String = firstEvent.ver
    val startTime: Long = firstEvent.ets
    val etags: Option[ETags] = Option(CommonUtil.getETags(firstEvent))

    var lastEvent: V3Event = null
    var itemResponses: Buffer[ItemResponse] = Buffer[ItemResponse]()
    var endTime: Long = 0l
    var timeSpent: Double = 0.0
    var timeDiff: Double = 0.0
    var interactEventsCount: Long = 0l
    var envSummary: Iterable[EnvSummary] = Iterable[EnvSummary]()
    var eventsSummary: Map[String, Long] = Map(firstEvent.eid -> 1)
    var pageSummary: Iterable[PageSummary] = Iterable[PageSummary]()
    var prevEventEts: Long = startTime
    var lastImpression: V3Event = null
    var impressionMap: Map[V3Event, Double] = Map()

    var CHILD: Buffer[Summary] = Buffer()
    var PARENT: Summary = null

    var isClosed: Boolean = false

    def add(event: V3Event, idleTime: Int, itemMapping: Map[String, Item]) {
        val ts = CommonUtil.getTimeDiff(prevEventEts, event.ets).get
        prevEventEts = event.ets
        this.timeSpent += CommonUtil.roundDouble((if (ts > idleTime) 0 else ts), 2)
        if (StringUtils.equals(event.eid, "INTERACT")) this.interactEventsCount += 1
        val prevCount = eventsSummary.get(event.eid).getOrElse(0l)
        eventsSummary += (event.eid -> (prevCount + 1))
        if (lastImpression != null) {
            val prevTs = impressionMap.get(lastImpression).getOrElse(0.0)
            impressionMap += (lastImpression -> (prevTs + ts))
        }
        if (StringUtils.equals(event.eid, "IMPRESSION")) {
            if (lastImpression == null) {
                lastImpression = event
                impressionMap += (lastImpression -> 0.0)
            } else {
                val prevTs = impressionMap.get(lastImpression).getOrElse(0.0)
                impressionMap += (lastImpression -> (prevTs + ts))
                lastImpression = event
                impressionMap += (lastImpression -> 0.0)
            }
        }
        this.lastEvent = event
        this.endTime = this.lastEvent.ets
        this.timeDiff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(this.startTime, this.endTime).get, 2)
        this.pageSummary = getPageSummaries();
        this.envSummary = getEnvSummaries();

        if (StringUtils.equals(event.eid, "ASSESS")) {
            val itemObj = getItem(itemMapping, event);
            val metadata = itemObj.metadata;
            val resValues = if (null == event.edata.resvalues) Option(Array[Map[String, AnyRef]]().map(f => f.asInstanceOf[AnyRef])) else Option(event.edata.resvalues.map(f => f.asInstanceOf[AnyRef]))
            val res = if (null == event.edata.resvalues) Option(Array[String]()); else Option(event.edata.resvalues.flatten.map { x => (x._1 + ":" + x._2.toString) });
            val item = event.edata.item
            this.itemResponses += ItemResponse(item.id, metadata.get("type"), metadata.get("qlevel"), Option(event.edata.duration), Option(Int.box(item.exlength)), res, resValues, metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, Option(item.mmc), event.edata.score, event.ets, metadata.get("max_score"), metadata.get("domain"), event.edata.pass, Option(item.title), Option(item.desc));
        }
    }

    def addChild(child: Summary) {
        this.CHILD.append(child);
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

    def getPageSummaries(): Iterable[PageSummary] = {
        if (this.impressionMap.size > 0) {
            this.impressionMap.map(f => (f._1.edata.pageid, f)).groupBy(x => x._1).map { f =>
                val id = f._1
                val firstEvent = f._2.head._2._1
                val `type` = firstEvent.edata.`type`
                val env = firstEvent.context.env
                val timeSpent = CommonUtil.roundDouble(f._2.map(x => x._2._2).sum, 2)
                val visitCount = f._2.size.toLong
                PageSummary(id, `type`, env, timeSpent, visitCount)
            }
        } else Iterable[PageSummary]()
    }

    def getEnvSummaries(): Iterable[EnvSummary] = {
        if (this.pageSummary.size > 0) {
            this.pageSummary.groupBy { x => x.env }.map { f =>
                val timeSpent = CommonUtil.roundDouble(f._2.map(x => x.time_spent).sum, 2)
                val count = f._2.map(x => x.visit_count).max;
                EnvSummary(f._1, timeSpent, count)
            }
        } else Iterable[EnvSummary]()
    }

    private def reduce(child: Summary) {
        // TODO add reduce code here
        this.timeSpent += child.timeSpent
        this.interactEventsCount += child.interactEventsCount
        this.endTime = child.endTime
        this.lastEvent = child.lastEvent
        this.timeDiff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(this.startTime, this.endTime).get, 2)
        val eventsList = this.eventsSummary.toList ++ child.eventsSummary.toList
        this.eventsSummary = eventsList.groupBy (_._1) .map { case (k,v) => k -> v.map(_._2).sum }
        this.impressionMap = this.impressionMap ++ child.impressionMap
        this.pageSummary = getPageSummaries();
        this.envSummary = getEnvSummaries();
    }

    def close() {
        if (this.CHILD != null) {
            for (child <- CHILD) {
                this.reduce(child)
                child.close();
            }
        }
        this.isClosed = true
    }
}