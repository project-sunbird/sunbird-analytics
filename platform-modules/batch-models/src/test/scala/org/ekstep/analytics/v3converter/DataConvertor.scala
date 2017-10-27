package org.ekstep.analytics.v3converter

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.CommonUtil

case class DerivedEventTest(eid: String, ets: Long, syncts: Long, ver: String, mid: String, uid: String, channel: Option[String], content_id: Option[String] = None, cdata: Option[CData], context: Context, dimensions: Dimensions, edata: MEEdata, tags: Option[AnyRef] = None) extends Input with AlgoInput;
case class UserProfile(uid: String, gender: String, age: Int);
case class EDataV3(val dspec: Map[String, AnyRef], val loc: String, val pass: String, val qid: String, val score: Int,
              val res: Array[String], val length: AnyRef, val atmpts: Int, val failedatmpts: Int, val category: String,
              val current: String, val max: String, val `type`: String, val extype: String, val id: String, val gid: String,
              val itype: String, val stageid: String, val stageto: String, val resvalues: Array[Map[String, AnyRef]],
              val params: Array[Map[String, AnyRef]], val uri: String, val state: String, val subtype: String,
              val pos: Array[Map[String, AnyRef]], val values: Array[AnyRef], val tid: String, val direction: String,
              val datatype: String, val count: AnyRef, val contents: Array[Map[String, AnyRef]], val comments: String,
              val rating: Double, val qtitle: String, val qdesc: String, val mmc: Array[String], val context: Map[String, AnyRef],
              val method: String, val request: AnyRef, val defaultPlugins: List[String], val loadtimes: Map[String, Number], val client: Map[String, String], val path: String,
              val response: String, val responseTime: Long, val status: String, val uip: String, val pluginid: String, val pluginver: String, val objectid: String,
              val stage: String, val containerid: String, val containerplugin: String, val target: String, val action: String, val err: AnyRef, val data: AnyRef,
              val severity: String, val duration: Long, val uaspec: Map[String, String], val env: String, val pageid: String, val name: String, val url: String,
              val targetid: String, val parentid: Option[String], val parenttype: Option[String], val code: Option[String], val prevstate: String,
              val email: Option[String], val access: Option[List[Map[String, String]]], val partners: Option[List[Map[String, String]]], val profile: Option[List[Map[String, String]]], val item: Question, val visits: List[Visit]) extends Serializable {}

case class EventV3(val eid: String, val ets: Long, val `@timestamp`: String, val ver: String, val mid: String, val actor: Actor, val context: V3Context, val `object`: Option[V3Object], val edata: EDataV3, val tags: List[AnyRef] = null) extends AlgoInput with Input {}

class DataConvertor extends SparkSpec(null) {

    val fileNameList = List("test-data1.log", "test-data2.log", "test-data3.log", "test-data4.log", "test-data5.log",
        "test-data6.log", "test-data7.log", "test-data8.log")
    val inputPath = "src/test/resources/genie-usage-summary/"
    val outputPath = "/tmp/data/v3/"

    val eventMapping = Map("GE_START" -> "START", "GE_GENIE_START" -> "START", "GE_END" -> "END", "GE_GENIE_END" -> "END", "GE_INTERACT" -> "IMPRESSION", "GE_CREATE_PROFILE" -> "AUDIT", "GE_CREATE_USER" -> "AUDIT", "GE_SESSION_START" -> "START", "GE_SESSION_END" -> "END", "GE_LAUNCH_GAME" -> "START", "GE_GAME_END" -> "END", "GE_ERROR" -> "ERROR", "GE_API_CALL" -> "LOG", "GE_INTERRUPT" -> "INTERRUPT", "GE_GENIE_RESUME" -> "INTERRUPT", "GE_GENIE_UPDATE" -> "LOG", "GE_TRANSFER" -> "SHARE", "GE_FEEDBACK" -> "FEEDBACK", "OE_START" -> "START")
    val filterList = List("GE_GENIE_START", "GE_START", "GE_GENIE_END", "GE_END", "GE_INTERACT", "GE_INTERACT", "GE_CREATE_PROFILE", "GE_CREATE_USER", "GE_SESSION_START", "GE_SESSION_END", "GE_LAUNCH_GAME", "GE_GAME_END", "GE_ERROR", "GE_API_CALL", "GE_INTERRUPT", "GE_GENIE_RESUME", "GE_GENIE_UPDATE", "GE_TRANSFER", "GE_FEEDBACK", "OE_START")

    def getEid(key: String): String = eventMapping.get(key).get

    def filterEvent(events: RDD[Event], list: List[String]): RDD[Event] = {
        DataFilter.filter(events, Filter("eid", "IN", Option(list)))
    }

    def saveToFile(data: Array[String], path: String) {
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> path)), data);
    }

    ignore should " filter v2 events successfully" in {
        fileNameList.map { fileName =>
            println("fileName : " + fileName)
            val rdd = loadFile[Event](inputPath + fileName);
            println("data loaded successfully.")
            println("count : " + rdd.count)

            val filteredEvents = filterEvent(rdd, filterList).collect()
            val events = filteredEvents.map(f => JSONUtils.serialize(f))
            println("size : " + filteredEvents.size)
            saveToFile(events, "/tmp/data/" + fileName)
        }
    }

    it should " convert data to telemetry v3 successfully" in {
        fileNameList.map { fileName =>
            val rdd = loadFile[Event]("/tmp/data/" + fileName);
            
            val data = rdd.collect().map { x =>
                val env = if ("OE_START".equals(x.eid)) "player" else "genie"
                val pData = V3PData(env, Option(x.ver))
                val rollUp = RollUp("l1", "l2", "l3", "l4")
                val context = V3Context(x.channel.getOrElse("in.ekstep"), Option(pData), env, Option(x.sid), Option(x.did), None, Option(rollUp))
                val `object` = V3Object(x.gdata.id, "user", Option(x.gdata.ver), Option(rollUp))
                
                val eks = JSONUtils.deserialize[V3EData](JSONUtils.serialize(x.edata.eks))
                val `type` = if("GE_SESSION_START".equals(x.eid) || "GE_SESSION_END".equals(x.eid)) "session" 
                    else if("GE_GAME_START".equals(x.eid) || "GE_GAME_END".equals(x.eid) || "GE_LAUNCH_GAME".equals(x.eid)) "contentplay" else "app"
                val eData = EDataV3(eks.dspec, eks.loc, eks.pass, eks.qid, eks.score, eks.res, eks.length, eks.atmpts, eks.failedatmpts, eks.category, eks.current, eks.max, `type`, eks.extype, eks.id, eks.gid, eks.itype, eks.stageid, eks.stageto, eks.resvalues, eks.params, eks.uri, eks.state, eks.subtype, eks.pos, eks.values, eks.tid, eks.direction, eks.datatype, eks.count, eks.contents, eks.comments, eks.rating, eks.qtitle, eks.qdesc, eks.mmc, eks.context, eks.method, eks.request, eks.defaultPlugins, eks.loadtimes, eks.client, eks.path, eks.response, eks.responseTime, eks.status, eks.uip, eks.pluginid, eks.pluginver, eks.objectid, eks.stage, eks.containerid, eks.containerplugin, eks.target, eks.action, eks.err, eks.data, eks.severity, eks.duration, eks.uaspec, eks.env, eks.stageid, eks.name, eks.url, eks.targetid, eks.parentid, eks.parenttype, eks.code, eks.prevstate, eks.email, eks.access, eks.partners, eks.profile, eks.item, eks.visits)
                val eid = getEid(x.eid)
                val ets = if (null != x.ets && x.ets != 0) x.ets else CommonUtil.getTimestamp(x.ts)
                EventV3(eid, ets, x.`@timestamp`, x.ver, "", Actor(x.uid, "User"), context, Option(`object`), eData, x.tags.asInstanceOf[List[Map[String, String]]])
            }
            val events = data.map(f => JSONUtils.serialize(f))
            saveToFile(events, outputPath + fileName)
        }
    }
}
