package org.ekstep.analytics.creation.model

import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.Input

class CaseClasses extends Serializable {}


@scala.beans.BeanInfo
class PData(val id: String, val pid: String, val ver: String) extends Serializable;
@scala.beans.BeanInfo
class CData(val `type`: String, val id: String) extends Serializable;
@scala.beans.BeanInfo
class Context(val sid: String, val content_id: String) extends Serializable;
@scala.beans.BeanInfo
class EData(val eks: Map[String, AnyRef]) extends Serializable;

@scala.beans.BeanInfo
case class CreationEvent(val eid: String, val ets: Long, val ver: String, val mid: String, val pdata: Option[PData] = None, val cdata: Option[List[CData]] = None, val uid: String, val context: Option[Context] = None, val rid: String, val edata: EData, val tags: List[AnyRef]) extends Input with AlgoInput;