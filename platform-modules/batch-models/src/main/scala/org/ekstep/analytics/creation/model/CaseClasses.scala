package org.ekstep.analytics.creation.model

import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.Input


/**
 * @author Jitendra Singh Sankhwar
 */
class CaseClasses extends Serializable {}


/**
 * Models to support Creation event model. 
 */
@scala.beans.BeanInfo
class CreationEks(val defaultPlugins: List[String], val loadtimes: Map[String, Long], val client: Map[String, String], val path: String, val method: String, val request: String, val response: String, val responseTime: Long, val status: Int, val uip: String, val `type`: String, val pluginid: String, val pluginver: String, val objectid: String, val stage: String, val containerid: String, val containerplugin: String, val target: String, val action: String, val err: String, val data: Map[String, AnyRef], val severity: String, val duration: Long, val uaspec: Map[String, String], val env: String, val id: String, val name: String, val url: String, val context: String, val targetid: String, val subtype: String) extends Serializable {}
@scala.beans.BeanInfo
class CreationPData(val id: String, val pid: String, val ver: String) extends Serializable;
@scala.beans.BeanInfo
class CreationCData(val `type`: String, val id: String) extends Serializable;
@scala.beans.BeanInfo
class CreationContext(val sid: String, val content_id: String) extends Serializable;
@scala.beans.BeanInfo
class CreationEData(val eks: CreationEks) extends Serializable;

/**
 * Creation event model 
 */
@scala.beans.BeanInfo
case class CreationEvent(val eid: String, val ets: Long, val ver: String, val mid: String, val pdata: Option[CreationPData] = None, val cdata: Option[List[CreationCData]] = None, val uid: String, val context: Option[CreationContext] = None, val rid: String, val edata: CreationEData, val tags: List[AnyRef]) extends Input with AlgoInput;