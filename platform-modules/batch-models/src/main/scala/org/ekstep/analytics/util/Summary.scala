package org.ekstep.analytics.util

import org.ekstep.analytics.framework._

import scala.collection.mutable.Buffer
import org.ekstep.analytics.model.{EnvSummary, EventSummary, ItemResponse, PageSummary}
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.CommonUtil

class Summary(val summaryKey: String, val event: V3Event) {

    val sid: String = ""
    val uid: String = ""
    val content_id: Option[String] = None
    val session_type: String = ""
    val syncts: Long = 0l
    val dt_range: DtRange = DtRange(0l, 0l)
    val mode: Option[String] = None
    val item_responses: Option[Buffer[ItemResponse]] = None
    val start_time: Long = 0l
    val end_time: Long = 0l
    val time_spent: Double = 0.0
    val time_diff: Double = 0.0
    val interact_events_count: Long = 0l
    val interact_events_per_min: Double = 0.0
    val telemetry_version: String = ""
    val env_summary: Option[Iterable[EnvSummary]] = None
    val events_summary: Option[Iterable[EventSummary]] = None
    val page_summary: Option[Iterable[PageSummary]] = None
    val etags: Option[ETags] = None
    val DEFAULT_MODE = "play";

    val CHILD: Buffer[Summary] = null
    var PARENT: Summary = null

    val events: Buffer[V3Event] = Buffer(event)
    var isClosed: Boolean = false

    def add(event: V3Event) {
        events.append(event)
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
      * Get item from broadcast item mapping variable
      */
    private def getItem(itemMapping: Map[String, Item], event: V3Event): Item = {
        val item = itemMapping.getOrElse(event.edata.item.id, null);
        if (null != item) {
            return item;
        }
        return Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]()));
    }

    def computeMetrics(currentSumm: Summary, idleTime: Int, itemMapping: Map[String, Item]) {

        val events = currentSumm.events
        val session_type = events.head.edata.`type`
        val firstEvent = events.head;
        val lastEvent = events.last;
        val telemetryVer = firstEvent.ver;
        val startTimestamp = firstEvent.ets;
        val endTimestamp = lastEvent.ets;
        val pdata = CommonUtil.getAppDetails(firstEvent)
        val channelId = CommonUtil.getChannelId(firstEvent)
        val contentId = if (firstEvent.`object`.isDefined) firstEvent.`object`.get.id else null;
        val uid = if (lastEvent.actor.id.isEmpty()) "" else lastEvent.actor.id
        val timeDiff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(startTimestamp, endTimestamp).get, 2);

        var tmpLastEvent: V3Event = null;
        val eventsWithTs = events.map { x =>
            if (tmpLastEvent == null) tmpLastEvent = x;
            val ts = CommonUtil.getTimeDiff(tmpLastEvent.ets, x.ets).get;
            tmpLastEvent = x;
            (x, if (ts > idleTime) 0 else ts)
        }
        var lastEventTs: Long = eventsWithTs.last._1.ets;
        var tempEvents = Buffer[(V3Event, Double)]();
        var eventsBuffer: Buffer[(V3Event, Double)] = Buffer();
        eventsWithTs.foreach { f =>
            f._1.eid match {
                case "IMPRESSION" =>
                    if (tempEvents.isEmpty) {
                        tempEvents += f
                    } else if (lastEventTs == f._1.ets) {
                        val ts = tempEvents.map { x => x._2 }.sum
                        val tuple = (tempEvents.head._1, ts)
                        eventsBuffer += tuple
                        tempEvents = Buffer[(V3Event, Double)]();
                        tempEvents += f
                        val lastTs = tempEvents.map { x => x._2 }.sum
                        val lastTuple = (tempEvents.head._1, ts)
                        eventsBuffer += lastTuple
                    } else {
                        val ts = tempEvents.map { x => x._2 }.sum
                        val tuple = (tempEvents.head._1, ts)
                        eventsBuffer += tuple
                        tempEvents = Buffer[(V3Event, Double)]();
                        tempEvents += f
                    }
                case _ =>
                    if (lastEventTs == f._1.ets && !tempEvents.isEmpty) {
                        tempEvents += f
                        val ts = tempEvents.map { x => x._2 }.sum
                        val tuple = (tempEvents.head._1, ts)
                        eventsBuffer += tuple
                    }
                    tempEvents += f
            }
        }
        // only for player session
        val itemResponses = if (StringUtils.equals(session_type, "player")) {
            val assessEvents = events.filter { x => "ASSESS".equals(x.eid) }.sortBy { x => x.ets };
            assessEvents.map { x =>
                val itemObj = getItem(itemMapping, x);
                val metadata = itemObj.metadata;
                val resValues = if (null == x.edata.resvalues) Option(Array[Map[String, AnyRef]]().map(f => f.asInstanceOf[AnyRef])) else Option(x.edata.resvalues.map(f => f.asInstanceOf[AnyRef]))
                val res = if (null == x.edata.resvalues) Option(Array[String]()); else Option(x.edata.resvalues.flatten.map { x => (x._1 + ":" + x._2.toString) });
                val item = x.edata.item
                ItemResponse(item.id, metadata.get("type"), metadata.get("qlevel"), Option(x.edata.duration), Option(Int.box(item.exlength)), res, resValues, metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, Option(item.mmc), x.edata.score, x.ets, metadata.get("max_score"), metadata.get("domain"), x.edata.pass, Option(item.title), Option(item.desc));
            }
        } else null;
        val mode = if (StringUtils.equals(session_type, "player")) {
            if (null == firstEvent.edata.mode || firstEvent.edata.mode.isEmpty()) DEFAULT_MODE else firstEvent.edata.mode
        } else null;

        val timeSpent = CommonUtil.roundDouble(eventsWithTs.map(f => f._2).sum, 2);
        val interactEventsCount = events.filter { x => "INTERACT".equals(x.eid) }.size.toLong
        val interactEventsPerMin: Double = if (interactEventsCount == 0 || timeSpent == 0) 0d
        else if (timeSpent < 60.0) interactEventsCount.toDouble
        else BigDecimal(interactEventsCount / (timeSpent / 60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;

        val eventSummaries = events.groupBy { x => x.eid }.map(f => EventSummary(f._1, f._2.length));

        val impressionEventsWithTs = eventsBuffer.filter { x => "IMPRESSION".equals(x._1.eid) }.map(x => (x._1.edata.pageid, x))
        val pageSummaries = if (impressionEventsWithTs.length > 0) {
            impressionEventsWithTs.groupBy(f => f._1).map { f =>
                val id = f._1
                val firstEvent = f._2(0)._2._1
                val `type` = firstEvent.edata.`type`
                val env = firstEvent.context.env
                val timeSpent = CommonUtil.roundDouble(f._2.map(x => x._2._2).sum, 2)
                val visitCount = f._2.length.toLong
                PageSummary(id, `type`, env, timeSpent, visitCount)
            }
        } else Iterable[PageSummary]();

        val envSummaries = if (pageSummaries.size > 0) {
            pageSummaries.groupBy { x => x.env }.map { f =>
                val timeSpent = CommonUtil.roundDouble(f._2.map(x => x.time_spent).sum, 2)
                val count = f._2.map(x => x.visit_count).max;
                EnvSummary(f._1, timeSpent, count)
            }
        } else Iterable[EnvSummary]();
    }

    def rollUpSummary(currentSumm: Summary, idleTime: Int, itemMapping: Map[String, Item]) {
        if (null == currentSumm.CHILD) {
            computeMetrics(currentSumm, idleTime, itemMapping)
        } else {
            computeMetrics(currentSumm, idleTime, itemMapping)
            for (child <- currentSumm.CHILD) {
                if (child.isClosed) {
                    reduce(currentSumm, child)
                } else {
                    rollUpSummary(child, idleTime, itemMapping)
                }
            }
        }
    }

    private def reduce(current: Summary, child: Summary) {

    }

    def close(idleTime: Int, itemMapping: Map[String, Item]) {
        rollUpSummary(this, idleTime, itemMapping)
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