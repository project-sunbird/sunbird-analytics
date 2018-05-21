package org.ekstep.analytics.util

import org.ekstep.analytics.framework._
import scala.collection.mutable.Buffer
import org.ekstep.analytics.model.{EnvSummary, EventSummary, ItemResponse, PageSummary}
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil

class Summary(val firstEvent: V3Event) {

    val defaultPData = V3PData(AppConf.getConfig("default.consumption.app.id"), Option("1.0"))
    val interactTypes = List("touch", "drag", "drop", "pinch", "zoom", "shake", "rotate", "speak", "listen", "write", "draw", "start", "end", "choose", "activate", "scroll", "click", "edit", "submit", "search", "dnd", "added", "removed", "selected")
    val sid: String = firstEvent.context.sid.getOrElse("")
    val uid: String = if (firstEvent.actor.id == null) "" else firstEvent.actor.id
    val contentId: Option[String] = if (firstEvent.`object`.isDefined) Option(firstEvent.`object`.get.id) else None;
    val mode: Option[String] = if (firstEvent.edata.mode == null) Option("") else Option(firstEvent.edata.mode)
    val telemetryVersion: String = firstEvent.ver
    val etags: Option[ETags] = Option(CommonUtil.getETags(firstEvent))
    val channel: String = firstEvent.context.channel
    val did: String = firstEvent.context.did.getOrElse("")
    val pdata: V3PData = firstEvent.context.pdata.getOrElse(defaultPData)
    val context_rollup: Option[RollUp] = firstEvent.context.rollup
    val object_rollup: Option[RollUp] = if(firstEvent.`object`.nonEmpty) firstEvent.`object`.get.rollup else None

    var startTime: Long = firstEvent.ets
    var interactEventsCount: Long = if(StringUtils.equals("INTERACT", firstEvent.eid) && interactTypes.contains(firstEvent.edata.`type`.toLowerCase)) 1l else 0l
    var `type`: String = if (null == firstEvent.edata.`type`) "app" else StringUtils.lowerCase(firstEvent.edata.`type`)
    var lastEvent: V3Event = null
    var itemResponses: Buffer[ItemResponse] = Buffer[ItemResponse]()
    var endTime: Long = 0l
    var timeSpent: Double = 0.0
    var timeDiff: Double = 0.0
    var envSummary: Iterable[EnvSummary] = Iterable[EnvSummary]()
    var eventsSummary: Map[String, Long] = Map(firstEvent.eid -> 1)
    var pageSummary: Iterable[PageSummary] = Iterable[PageSummary]()
    var prevEventEts: Long = startTime
    var lastImpression: V3Event = null
    var impressionMap: Map[V3Event, Double] = Map()
    var summaryEvents: Buffer[MeasuredEvent] = Buffer()

    var CHILDREN: Buffer[Summary] = Buffer()
    var PARENT: Summary = null

    var isClosed: Boolean = false

    def updateType(`type`: String) {
        this.`type` = `type`;
    }

    def getLeafSummary(): Summary = {
        if(this.CHILDREN.size > 0) {
            this.CHILDREN.map { summ =>
                summ.getLeafSummary()
            }.last
        }
        else this
    }

    def deepClone(): Summary = {
        if(this.PARENT == null) {
            return this;
        }
        else {
            return this.PARENT.deepClone();
        }
    }

    def clearAll(): Unit = {
        this.CHILDREN.foreach{summ =>
            summ.clearSummary();
        }
        this.clearSummary()
    }

    def clearSummary(): Unit = {
        this.startTime = 0l
        this.interactEventsCount = 0l
        this.lastEvent = null
        this.itemResponses = Buffer[ItemResponse]()
        this.endTime = 0l
        this.timeSpent = 0.0
        this.timeDiff = 0.0
        this.envSummary = Iterable[EnvSummary]()
        this.eventsSummary = Map()
        this.pageSummary = Iterable[PageSummary]()
        this.lastImpression = null
        this.impressionMap = Map()
        this.summaryEvents = Buffer()
        this.isClosed = false
    }

    def add(event: V3Event, idleTime: Int, itemMapping: Map[String, Item]) {
        if(this.startTime == 0l) this.startTime = event.ets
        val ts = CommonUtil.getTimeDiff(prevEventEts, event.ets).get
        prevEventEts = event.ets
        this.timeSpent += CommonUtil.roundDouble((if (ts > idleTime) 0 else ts), 2)
        if (StringUtils.equals(event.eid, "INTERACT") && interactTypes.contains(event.edata.`type`.toLowerCase)) this.interactEventsCount += 1
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

        if(this.PARENT != null) this.PARENT.add(event, idleTime, itemMapping)
    }

    def addChild(child: Summary) {
        this.CHILDREN.append(child);
    }

    def addParent(parent: Summary, idleTime: Int, itemMapping: Map[String, Item]) {
        this.PARENT = parent;
        // Add first event of child to parent
        this.PARENT.add(this.firstEvent, idleTime, itemMapping)
    }

    def getParent(): Summary = {
        return this.PARENT;
    }

    def checkStart(`type`: String, mode: Option[String], summEvents: Buffer[MeasuredEvent], config: Map[String, AnyRef]): Summary = {
        if(this.`type`.equals(`type`) && this.mode.get.equals(mode.getOrElse(""))) {
            this.close(summEvents, config);
            if(this.PARENT != null) return PARENT else return this;
        }
        else if(this.PARENT == null) {
            return null;
        }
        else {
            return  PARENT.checkStart(`type`, mode, summEvents, config);
        }
    }

    def checkEnd(event: V3Event, idleTime: Int, itemMapping: Map[String, Item], config: Map[String, AnyRef]): Summary = {
        val mode = if(event.edata.mode == null) "" else event.edata.mode
        if(this.`type`.equals(event.edata.`type`) && this.mode.get.equals(mode)) {
            if(this.PARENT == null) return this else return PARENT;
        }
        if(this.PARENT == null) {
            return this;
        }
        val summ = PARENT.checkEnd(event, idleTime, itemMapping, config)
        if (summ == null) {
            return this;
        }
        return summ;
    }

    def close(summEvents: Buffer[MeasuredEvent], config: Map[String, AnyRef]) {

        val tempChildEvents = Buffer[MeasuredEvent]()
        this.CHILDREN.foreach{summ =>
            if(!summ.isClosed) {
                summ.close(summEvents, config);
                tempChildEvents ++= summ.summaryEvents
            }
        }
        if(this.timeSpent > 0) {
            this.summaryEvents ++= tempChildEvents
            this.summaryEvents += this.getSummaryEvent(config)
        };
        this.isClosed = true;
    }

    def getSummaryEvent(config: Map[String, AnyRef]): MeasuredEvent = {
        val meEventVersion = "1.0"
        val dtRange = DtRange(this.startTime, this.endTime)
        val mid = CommonUtil.getMessageId("ME_WORKFLOW_SUMMARY", this.uid, "SESSION", dtRange, this.contentId.getOrElse("NA"), Option(this.pdata.id), Option(this.channel));
        val interactEventsPerMin: Double = if (this.interactEventsCount == 0 || this.timeSpent == 0) 0d
        else if (this.timeSpent < 60.0) this.interactEventsCount.toDouble
        else BigDecimal(this.interactEventsCount / (this.timeSpent / 60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
        val syncts = CommonUtil.getEventSyncTS(if(this.lastEvent == null) this.firstEvent else this.lastEvent)
        val eventsSummary = this.eventsSummary.map(f => EventSummary(f._1, f._2.toInt))
        val measures = Map("start_time" -> this.startTime,
            "end_time" -> this.endTime,
            "time_diff" -> this.timeDiff,
            "time_spent" -> CommonUtil.roundDouble(this.timeSpent, 2),
            "telemetry_version" -> this.telemetryVersion,
            "item_responses" -> this.itemResponses,
            "interact_events_count" -> this.interactEventsCount,
            "interact_events_per_min" -> interactEventsPerMin,
            "env_summary" -> this.envSummary,
            "events_summary" -> eventsSummary,
            "page_summary" -> this.pageSummary);
        MeasuredEvent("ME_WORKFLOW_SUMMARY", System.currentTimeMillis(), syncts, meEventVersion, mid, this.uid, null, None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "WorkflowSummarizer").asInstanceOf[String])), None, "SESSION", dtRange),
            org.ekstep.analytics.framework.Dimensions(None, Option(this.did), None, None, None, None, Option(PData(this.pdata.id, this.pdata.ver.getOrElse("1.0"), None, this.pdata.pid)), None, None, None, None, None, this.contentId, None, None, Option(this.sid), None, None, None, None, None, None, None, None, None, None, Option(this.channel), Option(this.`type`), this.mode, this.context_rollup, this.object_rollup),
            MEEdata(measures), this.etags);
    }

    def checkSimilarity(summ: Summary): Boolean = {
        StringUtils.equals(this.`type`, summ.`type`) && StringUtils.equals(this.mode.get, summ.mode.get) && (this.startTime == summ.startTime)
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

}