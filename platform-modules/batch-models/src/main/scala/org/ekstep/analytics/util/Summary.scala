package org.ekstep.analytics.util

import org.ekstep.analytics.framework._
import scala.collection.mutable.Buffer
import org.ekstep.analytics.model.ItemResponse
import org.ekstep.analytics.model.EnvSummary
import org.ekstep.analytics.model.EventSummary
import org.apache.commons.lang3.StringUtils

class Summary(val summaryKey: String, val event: V3Event) {

    val did: Option[String] = None
    val sid: String = ""
    val uid: String = ""
    val pdata: PData = PData("", "")
    val channel: String = "in.ekstep"
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

    def computeMetrics(currentSumm: Summary) {

    }

    def rollUpSummary(currentSumm: Summary) {

    }

    def close() {
        for (child <- CHILD) {
            if (child.isClosed == false) {
                child.isClosed = true
            }
        }
        rollUpSummary(this)
        this.isClosed = true
    }
}