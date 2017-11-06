package org.ekstep.analytics.v3converter

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.CommonUtil

case class DerivedEventTest(eid: String, ets: Long, syncts: Long, ver: String, mid: String, uid: String, channel: Option[String], content_id: Option[String] = None, cdata: Option[CData], context: Context, dimensions: Dimensions, edata: MEEdata, tags: Option[AnyRef] = None) extends Input with AlgoInput;
case class UserProfile(uid: String, gender: String, age: Int);

case class EventV3(val eid: String, val ets: Long, val `@timestamp`: String, val ver: String, val mid: String, val actor: Actor, val context: V3Context, val `object`: Option[V3Object], val edata: EDataV3, val tags: List[AnyRef] = null) extends AlgoInput with Input {}

case class EDataV3(val `type`: Option[String], val dspec: Option[Map[String, AnyRef]], val uaspec: Option[Map[String, String]], val loc: Option[String], val mode: Option[String], val duration: Option[Double], val pageid: Option[String],
                   val summary: Option[Map[String, AnyRef]], val subtype: Option[String], val uri: Option[String], val visits: Option[List[Visit]], val id: Option[String], val target: Option[Map[String, AnyRef]],
                   val plugin: Option[Map[String, AnyRef]], val extra: Option[Map[String, AnyRef]], val item: Option[Question], val pass: Option[String], val score: Option[Int], val resvalues: Option[Array[Map[String, AnyRef]]],
                   val values: Option[Array[AnyRef]], val rating: Option[Double], val comments: Option[String], val dir: Option[String], val items: Option[List[Map[String, AnyRef]]], val props: Option[List[String]],
                   val state: Option[String], val prevstate: Option[String], val err: Option[AnyRef], val errtype: Option[String], val stacktrace: Option[String], val `object`: Option[Map[String, AnyRef]],
                   val level: Option[String], val message: Option[String], val params: Option[Array[Map[String, AnyRef]]]) extends Serializable {}

class DataConvertor extends SparkSpec(null) {

    val fileNameList = List("test_data_groupInfo.log", "test_data_partnerId.log", "test_data.log", "test_data1.log", "test_data2.log", "test_data3.log", "test_data4.log", "test_data5.log",
        "test_data6.log", "test_data7.log", "test_data8.log", "test_data9.log", "test_data1.log")

    val inputPath = "src/test/resources/session-summary/test/"
    val outputPath = "/Users/amitBehera/backup/data/v3/"

    val eventMapping = Map(
        "GE_START" -> "START",
        "GE_GENIE_START" -> "START",
        "GE_END" -> "END",
        "GE_GENIE_END" -> "END",
        "GE_INTERACT" -> "IMPRESSION",
        "GE_CREATE_PROFILE" -> "AUDIT",
        "GE_CREATE_USER" -> "AUDIT",
        "GE_SESSION_START" -> "START",
        "GE_SESSION_END" -> "END",
        "GE_LAUNCH_GAME" -> "START",
        "GE_GAME_END" -> "END",
        "GE_ERROR" -> "ERROR",
        "GE_API_CALL" -> "LOG",
        "GE_SERVICE_API_CALL" -> "LOG",
        "GE_INTERRUPT" -> "INTERRUPT",
        "GE_GENIE_RESUME" -> "INTERRUPT",
        "GE_GENIE_UPDATE" -> "LOG",
        "GE_TRANSFER" -> "SHARE",
        "GE_FEEDBACK" -> "FEEDBACK",
        "OE_START" -> "START",
        "OE_END" -> "END",
        "OE_ASSESS" -> "ASSESS",
        "OE_INTERACT" -> "INTERACT",
        "OE_INTERRUPT" -> "INTERRUPT",
        "OE_NAVIGATE" -> "IMPRESSION",
        "OE_ITEM_RESPONSE" -> "RESPONSE",
        "OE_LEVEL_SET" -> "RESPONSE")
    //val filterList = List("GE_GENIE_START", "GE_START", "GE_GENIE_END", "GE_END", "GE_INTERACT", "GE_INTERACT", "GE_CREATE_PROFILE", "GE_CREATE_USER", "GE_SESSION_START", "GE_SESSION_END", "GE_LAUNCH_GAME", "GE_GAME_END", "GE_ERROR", "GE_API_CALL", "GE_INTERRUPT", "GE_GENIE_RESUME", "GE_GENIE_UPDATE", "GE_TRANSFER", "GE_FEEDBACK", "OE_START")
    val filterList = List("OE_ASSESS", "OE_START", "OE_END", "OE_LEVEL_SET", "OE_INTERACT", "OE_INTERRUPT", "OE_NAVIGATE", "OE_ITEM_RESPONSE")

    def getEid(key: String): String = {
        try {
            eventMapping.get(key).get
        } catch {
            case t: Throwable =>
                t.printStackTrace() // TODO: handle error
                println("event id: " + key)
                null;
        }

    }

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

    def edataConvt(eid: String, event: Event): EDataV3 = {
        val `type` = if ("GE_SESSION_START".equals(event.eid) || "GE_SESSION_END".equals(event.eid)) "session"
        else if ("GE_GAME_START".equals(event.eid) || "GE_GAME_END".equals(event.eid) || "GE_LAUNCH_GAME".equals(event.eid)) "contentplay" else "app"
        val eks = event.edata.eks
        val exTime = 30
        val max = 10
        val item = Question(eks.qid, max, exTime, eks.params, eks.uri, eks.qtitle, eks.qdesc, eks.mmc, Array())
        val edata = eid match {
            case "ASSESS" =>
                EDataV3(None, None, None, None, None, CommonUtil.getTimeSpent(eks.length), None, None, None, None, None, None, None, None, None, Option(item), Option(eks.pass), Option(eks.score), Option(eks.resvalues), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
            case _ =>
                EDataV3(Option(eks.`type`), Option(eks.dspec), Option(Map()), Option(eks.loc), Option(""), Option(0.0d), Option(eks.stageid),
                    Option(Map()), Option(eks.subtype), Option(eks.uri), Option(List()), Option(eks.id), Option(Map()),
                    Option(Map()), Option(Map()), None, None, None, Option(eks.resvalues),
                    Option(eks.values), Option(eks.rating), Option(eks.comments), Option(""), Option(List(Map())), Option(List()),
                    Option(eks.state), Option(""), None, None, None, Option(Map()),
                    Option(""), Option(""), Option(eks.params))
        }
        edata;
    }

    it should " convert data to telemetry v3 successfully" in {
        fileNameList.map { fileName =>
            val rdd = loadFile[Event](inputPath + fileName);

            val data = rdd.collect().map { x =>
                val env = if (x.eid.startsWith("CP")) "portal" else if (x.eid.startsWith("OE")) "player" else if (x.eid.startsWith("GE")) "genie" else if (x.eid.startsWith("CE")) "attool" else ""
                val pData = V3PData(env, Option(x.ver))
                val rollUp = RollUp("l1", "l2", "l3", "l4")
                val context = V3Context(x.channel.getOrElse("in.ekstep"), Option(pData), env, Option(x.sid), Option(x.did), None, Option(rollUp))
                val `object` = V3Object(x.gdata.id, "user", Option(x.gdata.ver), Option(rollUp))
                //val eData = EDataV3(eks.dspec, eks.loc, eks.pass, eks.qid, eks.score, eks.res, eks.length, eks.atmpts, eks.failedatmpts, eks.category, eks.current, eks.max, `type`, eks.extype, eks.id, eks.gid, eks.itype, eks.stageid, eks.stageto, eks.resvalues, eks.params, eks.uri, eks.state, eks.subtype, eks.pos, eks.values, eks.tid, eks.direction, eks.datatype, eks.count, eks.contents, eks.comments, eks.rating, eks.qtitle, eks.qdesc, eks.mmc, eks.context, eks.method, eks.request, eks.defaultPlugins, eks.loadtimes, eks.client, eks.path, eks.response, eks.responseTime, eks.status, eks.uip, eks.pluginid, eks.pluginver, eks.objectid, eks.stage, eks.containerid, eks.containerplugin, eks.target, eks.action, eks.err, eks.data, eks.severity, eks.duration, eks.uaspec, eks.env, eks.stageid, eks.name, eks.url, eks.targetid, eks.parentid, eks.parenttype, eks.code, eks.prevstate, eks.email, eks.access, eks.partners, eks.profile, eks.item, eks.visits)
                val eid = getEid(x.eid)
                val eData = edataConvt(eid, x)
                val ets = if (null != x.ets && x.ets != 0) x.ets else CommonUtil.getTimestamp(x.ts)
                EventV3(eid, ets, x.`@timestamp`, x.ver, "", Actor(x.uid, "User"), context, Option(`object`), eData, x.tags.asInstanceOf[List[Map[String, String]]])
            }
            val events = data.map(f => JSONUtils.serialize(f))
            saveToFile(events, outputPath + fileName)
        }
    }
}
