package org.ekstep.ilimi.analytics.framework

import java.io.Serializable
import java.util.Date

class Models extends Serializable {}

// Raw Event Model
case class Eks(subj: Option[String], mc: Option[Array[String]], mmc: Option[Array[String]], pass: Option[String], qid: Option[String], qtype: Option[String], qlevel: Option[String], score: Option[Int], maxscore: Option[Int], res: Option[Array[String]], exres: Option[Array[String]], length: Option[String], exlength: Option[Double], atmpts: Option[Int], failedatmpts: Option[Int], category: Option[String], current: Option[String], max: Option[String], `type`: Option[String], extype: Option[String], id: Option[String], gid: Option[String])
case class Edata(eks: Eks)
case class Gdata(id: Option[String], ver: Option[String])
case class Event(eid: Option[String], ts: Option[String], ver: Option[String], gdata: Option[Gdata], sid: Option[String], uid: Option[String], did: Option[String], edata: Edata)

// Computed Event Model


// User Model
case class User(name: String, encoded_id: String, ekstep_id: String, gender: String, dob: Date, language_id: Int);

// Analytics Framework Job Models
case class Query(bucket: Option[String], prefix: Option[String], startDate: Option[String], endDate: Option[String], gameId: Option[String], gameVersion: Option[String], telemetryVersion: Option[String], deviceInfo: Option[Map[String, AnyRef]], locationInfo: Option[Map[String, AnyRef]], brokerList: Option[String], topic: Option[String], windowType: Option[String], windowDuration: Option[Int])
case class Filter(name: Option[String], operator: Option[String], value: Option[AnyRef]);
case class Sort(name: Option[String], order: Option[String]);
case class Dispatcher(to: Option[String], params: Option[Map[String, AnyRef]]);
case class JobConfig(search: Option[Array[Query]], filters: Option[Array[Filter]], sort: Option[Sort], model: String, modelParams: Option[Map[String, AnyRef]], output: Option[Array[Dispatcher]], parallelization: Option[Int], appName: Option[String]);