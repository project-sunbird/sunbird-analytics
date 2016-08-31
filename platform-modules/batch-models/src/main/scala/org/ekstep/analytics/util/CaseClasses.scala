package org.ekstep.analytics.util

import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.AlgoInput

class CaseClasses extends Serializable {}

/* Computed Event Without Optional Fields - Start */
@scala.beans.BeanInfo
class GData(val id: String, val ver: String) extends Serializable {}

@scala.beans.BeanInfo
class DerivedEvent(val eid: String, val ets: Long, val syncts: Long, val ver: String, val mid: String, val uid: String, val content_id: String,
                   val context: Context, val dimensions: Dimensions, val edata: MEEdata, val tags: AnyRef) extends Input with AlgoInput;

@scala.beans.BeanInfo
class Dimensions(val uid: String, val did: String, val gdata: GData, val domain: String, val loc: String, val group_user: Boolean, val anonymous_user: Boolean) extends Serializable;

@scala.beans.BeanInfo
class PData(val id: String, val model: String, val ver: String) extends Serializable;

@scala.beans.BeanInfo
class DtRange(val from: Long, val to: Long) extends Serializable;

@scala.beans.BeanInfo
class Context(val pdata: PData, val dspec: Map[String, String], val granularity: String, val date_range: DtRange) extends Serializable;

@scala.beans.BeanInfo
class Eks(val id: String, val ver: String, val levels: Array[Map[String, Any]], val noOfAttempts: Int, val timeSpent: Double,
                     val interruptTime: Double, val timeDiff: Double, val start_time: Long, val end_time: Long, val currentLevel: Map[String, String],
                     val noOfLevelTransitions: Int, val interactEventsPerMin: Double, val completionStatus: Boolean, val screenSummary: Array[AnyRef], 
                     val noOfInteractEvents: Int, val eventsSummary: Array[AnyRef], val syncDate: Long, val contentType: AnyRef, val mimeType: AnyRef, 
                     val did: String, val tags: AnyRef, val telemetryVer: String)

@scala.beans.BeanInfo
class MEEdata(val eks: Eks) extends Serializable;
/* Computed Event Without Optional Fields - End */