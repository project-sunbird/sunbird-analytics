package org.ekstep.analytics.api

/**
 * @author Santhosh
 */
object Model {

}

case class Filter(partner_id: Option[String], group_user: Option[Boolean]);
case class Trend(day: Option[Int], week: Option[Int], month: Option[Int])
case class Request(filter: Option[Filter], summaries: Option[Array[String]], trend: Option[Trend]);
case class RequestBody(id: String, ver: String, ts: String, request: Request);

case class ContentSummary(total_ts: Double, total_sessions: Int, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double, avg_sessions_week: Option[Double], avg_ts_week: Option[Double])

case class Params(resmsgid: String, msgid: String, err: String, status: String, errmsg: String);
case class Response(id: String, ver: String, ts: String, params: Params, result: Map[String, AnyRef]);