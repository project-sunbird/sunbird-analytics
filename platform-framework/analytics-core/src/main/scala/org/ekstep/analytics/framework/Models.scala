package org.ekstep.analytics.framework

import java.io.Serializable
import java.util.Date
import scala.beans.BeanProperty
import org.apache.spark.rdd.RDD

class Models extends Serializable {}

@scala.beans.BeanInfo
class GData(val id: String, val ver: String) extends Serializable {}

@scala.beans.BeanInfo
class Eks(val dspec: Map[String, AnyRef], val loc: String, val pass: String, val qid: String, val score: Int, val res: Array[String], val length: AnyRef,
          val atmpts: Int, val failedatmpts: Int, val category: String, val current: String, val max: String, val `type`: String, val extype: String,
          val id: String, val gid: String, val itype: String, val stageid: String, val stageto: String, val resvalues: Array[Map[String, AnyRef]],
          val params: Array[Map[String, AnyRef]], val uri: String, val state: String, val subtype: String, val pos: Array[Map[String, AnyRef]],
          val values: Array[AnyRef], val tid: String, val direction: String, val datatype: String, val count: AnyRef, val contents: Array[Map[String, AnyRef]], val comments: String, val rating: Double, val qtitle: String, val qdesc: String, val mmc: Array[String]) extends Serializable {}

@scala.beans.BeanInfo
class Ext(val stageId: String, val `type`: String) extends Serializable {}

@scala.beans.BeanInfo
class EData(val eks: Eks, val ext: Ext) extends Serializable {}

@scala.beans.BeanInfo
class Event(val eid: String, val ts: String, val ets: Long, val `@timestamp`: String, val ver: String, val gdata: GData, val sid: String,
            val uid: String, val did: String, val edata: EData, val tags: AnyRef = null, val cdata: List[CData] = List()) extends AlgoInput with Input {}

// Computed Event Model
@scala.beans.BeanInfo
case class CData(id: String, `type`: Option[String]);
@scala.beans.BeanInfo
case class DerivedEvent(eid: String, ets: Long, syncts: Long, ver: String, mid: String, uid: String, content_id: Option[String] = None, cdata: Option[CData], context: Context, dimensions: Dimensions, edata: MEEdata, tags: Option[AnyRef] = None) extends Input with AlgoInput;
@scala.beans.BeanInfo
case class MeasuredEvent(eid: String, ets: Long, syncts: Long, ver: String, mid: String, uid: String, content_id: Option[String] = None, cdata: Option[CData], context: Context, dimensions: Dimensions, edata: MEEdata, tags: Option[AnyRef] = None) extends Output;
@scala.beans.BeanInfo
case class Dimensions(uid: Option[String], val did: Option[String], gdata: Option[GData], cdata: Option[CData], domain: Option[String], user: Option[UserProfile], loc: Option[String] = None, group_user: Option[Boolean] = None, anonymous_user: Option[Boolean] = None, tag: Option[String] = None, period: Option[Int] = None, content_id: Option[String] = None, ss_mid: Option[String] = None, item_id: Option[String] = None, sid: Option[String] = None, stage_id: Option[String] = None, funnel: Option[String] = None, dspec: Option[Map[String, AnyRef]] = None, onboarding: Option[Boolean] = None, genieVer: Option[String] = None);
@scala.beans.BeanInfo
case class PData(id: String, model: String, ver: String, pid: Option[String] = None);
@scala.beans.BeanInfo
case class DtRange(from: Long, to: Long);
@scala.beans.BeanInfo
case class Context(pdata: PData, dspec: Option[Map[String, String]] = None, granularity: String, date_range: DtRange, status: Option[String] = None, client_id: Option[String] = None, attempt: Option[Int] = None);
@scala.beans.BeanInfo
case class MEEdata(eks: AnyRef);

// User profile event models

@scala.beans.BeanInfo
class ProfileEks(val ueksid: String, val utype: String, val loc: String, val err: String, val attrs: Array[AnyRef], val uid: String, val age: Int, val day: Int, val month: Int, val gender: String, val language: String, val standard: Int, val is_group_user: Boolean, val dspec: Map[String, AnyRef]) extends Serializable {}
@scala.beans.BeanInfo
class ProfileData(val eks: ProfileEks, val ext: Ext) extends Serializable {}
@scala.beans.BeanInfo
class ProfileEvent(val eid: String, val ts: String, val `@timestamp`: String, val ver: String, val gdata: GData, val sid: String, val uid: String, val did: String, val edata: ProfileData) extends Input with AlgoInput with Serializable {}

// User Model
case class User(name: String, encoded_id: String, ekstep_id: String, gender: String, dob: Date, language_id: Int);
case class UserProfile(uid: String, gender: String, age: Int);

// Analytics Framework Job Models
case class Query(bucket: Option[String] = None, prefix: Option[String] = None, startDate: Option[String] = None, endDate: Option[String] = None, delta: Option[Int] = None, brokerList: Option[String] = None, topic: Option[String] = None, windowType: Option[String] = None, windowDuration: Option[Int] = None, file: Option[String] = None)
@scala.beans.BeanInfo
case class Filter(name: String, operator: String, value: Option[AnyRef] = None);
@scala.beans.BeanInfo
case class Sort(name: String, order: Option[String]);
@scala.beans.BeanInfo
case class Dispatcher(to: String, params: Map[String, AnyRef]);
@scala.beans.BeanInfo
case class Fetcher(`type`: String, query: Option[Query], queries: Option[Array[Query]]);
@scala.beans.BeanInfo
case class JobConfig(search: Fetcher, filters: Option[Array[Filter]], sort: Option[Sort], model: String, modelParams: Option[Map[String, AnyRef]], output: Option[Array[Dispatcher]], parallelization: Option[Int], appName: Option[String], deviceMapping: Option[Boolean] = Option(false));

// LP API Response Model
case class Params(resmsgid: Option[String], msgid: Option[String], err: Option[String], status: Option[String], errmsg: Option[String])
case class Result(content: Option[Map[String, AnyRef]], contents: Option[Array[Map[String, AnyRef]]], questionnaire: Option[Map[String, AnyRef]],
                  assessment_item: Option[Map[String, AnyRef]], assessment_items: Option[Array[Map[String, AnyRef]]], assessment_item_set: Option[Map[String, AnyRef]],
                  games: Option[Array[Map[String, AnyRef]]], concepts: Option[Array[String]], maxScore: Double, items: Option[Array[Map[String, AnyRef]]]);
case class Response(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Result);

// Search Items
case class SearchFilter(property: String, operator: String, value: Option[AnyRef]);
case class Metadata(filters: Array[SearchFilter])
case class Request(metadata: Metadata, resultSize: Int)
case class Search(request: Request);

// Item Models
case class MicroConcept(id: String, metadata: Map[String, AnyRef]);
case class Item(id: String, metadata: Map[String, AnyRef], tags: Option[Array[String]], mc: Option[Array[String]], mmc: Option[Array[String]]);
case class ItemSet(id: String, metadata: Map[String, AnyRef], items: Array[Item], tags: Option[Array[String]], count: Int);
case class Questionnaire(id: String, metadata: Map[String, AnyRef], itemSets: Array[ItemSet], items: Array[Item], tags: Option[Array[String]]);
case class ItemConcept(concepts: Array[String], maxScore: Int);

// Content Models
case class Content(id: String, metadata: Map[String, AnyRef], tags: Option[Array[String]], concepts: Array[String]);
case class Game(identifier: String, code: String, subject: String, objectType: String);

// Domain Models
case class Concept(id: String, metadata: Map[String, AnyRef], tags: Option[Array[String]]);
case class Relation(startNodeId: String, endNodeId: String, startNodeType: String, endNodeType: String, relationType: String)
case class DomainMap(concepts: Array[Concept], relations: Array[Relation])
case class DomainResult(concepts: Array[Map[String, AnyRef]], relations: Array[Relation]);
case class DomainResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: DomainResult);

// Common models for all data products
case class LearnerId(learner_id: String)
case class ContentId(content_id: String)
case class ContentMetrics(id: String, top_k_timespent: Map[String, Double], top_k_sessions: Map[String, Long])

case class Empty() extends Input with AlgoOutput with Output
case class UpdaterOutput(msg: String) extends Output
case class ContentKey(period: Int, content_id: String, tag: String);
case class GenieKey(period: Int, tag: String);
case class ItemKey(period: Int, tag: String, content_id: String, item_id: String);
case class InCorrectRes(resp: String, mmc: List[String], count: Int) extends Input with AlgoInput;
case class Misconception(value: String, count: Int) extends Input with AlgoInput;
case class RegisteredTag(tag_id: String, last_updated: Long, active: Boolean);
trait CassandraTable extends AnyRef with Serializable;

object Period extends Enumeration {
    type Period = Value
    val DAY, WEEK, MONTH, CUMULATIVE, LAST7, LAST30, LAST90 = Value
}

object Level extends Enumeration {
    type Level = Value
    val INFO, DEBUG, WARN, ERROR = Value
}

trait Stage extends Enumeration {
    type Stage = Value
    val contentPlayed = Value
}

object OnboardStage extends Stage {
    override type Stage = Value
    val welcomeContent, addChild, firstLesson, gotoLibrary, searchLesson, loadOnboardPage = Value
}

object OtherStage extends Stage {
    override type Stage = Value
    val listContent, selectContent, downloadInitiated, downloadComplete = Value
}


