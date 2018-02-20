package org.ekstep.analytics.util

import org.ekstep.analytics.framework._
import scala.collection.mutable.Buffer
import org.ekstep.analytics.model.ItemResponse
import org.ekstep.analytics.model.EnvSummary
import org.ekstep.analytics.model.EventSummary

class Summary {

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
    
    val events: Buffer[V3Event] = Buffer()
    
    val CHILD: Summary = null
    val PARENT: Summary = null
    
    def add(event: V3Event) {
        
    }
    
    def check() {
        
    }
    
    def computeMetrics(){
        
    }
    
    def close(){
        
    }
}