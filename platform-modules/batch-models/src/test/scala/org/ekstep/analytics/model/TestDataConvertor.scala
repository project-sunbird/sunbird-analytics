package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import scala.util.parsing.json.JSON
import org.json4s.JsonUtil
import org.ekstep.analytics.framework.util.JSONUtils

case class DerivedEventTest(eid: String, ets: Long, syncts: Long, ver: String, mid: String, uid: String, channel: Option[String], content_id: Option[String] = None, cdata: Option[CData], context: Context, dimensions: Dimensions, edata: MEEdata, tags: Option[AnyRef] = None) extends Input with AlgoInput;
case class UserProfile(uid: String, gender: String, age: Int);

case class Actor(id: String, `type`: String);
case class PDataV3(id: String, ver: Option[String] = None, pid: Option[String] = None);
case class CDataV3(id: String, `type`: String);
case class RollUp(l1: String, l2: String, l3: String, l4: String)
case class Context(channel: String, pdata: Option[PDataV3], env: String, sid: Option[String], did: Option[String], cdata: Option[List[CDataV3]], rollup: Option[RollUp])
case class Object(id: String, `type`: String, ver: Option[String], rollup: Option[RollUp])

class EDataV3(val dspec: Map[String, AnyRef], val loc: String, val pass: String, val qid: String, val score: Int, val res: Array[String], val length: AnyRef, val atmpts: Int, val failedatmpts: Int, val category: String, val current: String, val max: String, val `type`: String, val extype: String, val id: String, val gid: String, val itype: String, val stageid: String, val stageto: String, val resvalues: Array[Map[String, AnyRef]], val params: Array[Map[String, AnyRef]], val uri: String, val state: String, val subtype: String, val pos: Array[Map[String, AnyRef]], val values: Array[AnyRef], val tid: String, val direction: String, val datatype: String, val count: AnyRef, val contents: Array[Map[String, AnyRef]], val comments: String, val rating: Double, val qtitle: String, val qdesc: String, val mmc: Array[String], val context: Map[String, AnyRef], val method: String, val request: AnyRef) extends Serializable {}

case class EventV3(val eid: String, val ets: Long, val ver: String, val mid: String, val actor: Actor, val context: Context, val `object`: Option[Object], val edata: EDataV3, val tags: AnyRef = null)

class TestDataConvertor extends SparkSpec(null) {
    "Convertor" should " convert data to telemetry v3 successfully" in {
        val fileName = "test_data1.log"
        val inputPath = "src/test/resources/pipeline-summary/" + fileName
        val rdd = loadFile[Event](inputPath);
        println("data loaded successfully.")
        println("count : " + rdd.count)

        val data = rdd.map { x =>
            println("pdata : " + x.gdata)
            val pData = PDataV3(x.gdata.id, Option(x.gdata.ver))
            val cData = if(null != x.cdata) x.cdata.map { f => CDataV3(f.id, f.`type`.getOrElse("")) } else List()
            val rollUp = RollUp("l1", "l2", "l3", "l4")

            val context = Context(x.channel.getOrElse("in.ekstep"), Option(pData), "env", Option(x.sid), Option(x.did), Option(cData), Option(rollUp))
            val `object` = Object(x.gdata.id, "user", Option(x.gdata.ver), Option(rollUp))
            val eData = JSONUtils.deserialize[EDataV3](JSONUtils.serialize(x.edata.eks))
            EventV3(x.eid, x.ets, x.ver, "", Actor(x.uid, "User"), context, Option(`object`), eData, x.etags)
        }

        val path = "/tmp/data/" + fileName
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> path)), data);
        println("File created successfully : " + path)
    }
}
