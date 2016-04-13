package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import scala.collection.mutable.HashMap
import java.text.SimpleDateFormat
import java.util.Date
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.LocalDate
import org.joda.time.DateTime
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._

case class ContentSummary(content_id: String,start_date:Long,total_num_sessions:Long, total_ts:Double,
                           average_ts_session:Double,interactions_min_session:List[Double],average_interactions_min:Double,
                           num_sessions_week:Long,ts_week:Double)
case class ContentId(content_id: String)
                           
object ContentSummary extends IBatchModel[MeasuredEvent] with Serializable {
  
  def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
    println("### Running the model ContentSummary ###");
    val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
    val sortedEvents = filteredEvents.sortBy { x => x.ets };
    println("### Broadcasting data to all worker nodes ###");
    val config = jobParams.getOrElse(Map[String, AnyRef]());
    val configMapping = sc.broadcast(config);
    val deviceMapping = sc.broadcast(JobContext.deviceMapping);
     
    val contentMap = sortedEvents.groupBy { x => x.dimensions.gdata.get.id }
    val prevContentState = contentMap.map(f=>ContentId(f._1)).joinWithCassandraTable[ContentSummary]("content_db", "contentsummarizer").map(f => (f._1.content_id, f._2))
    val prevData = sc.broadcast(prevContentState)
    
    val contentSummary = contentMap.mapValues { events =>
      val firstEvent = events.head
      val lastEvent = events.last
      val gameId = firstEvent.dimensions.gdata.get.id
      val gameVersion = firstEvent.ver
      val eventStartTimestamp = firstEvent.context.date_range.from
      val eventEndTimestamp = lastEvent.context.date_range.to
      //val currentDate = eventStartTimestamp
      val startDate = 1450523803000l
      val numSessions = events.size
      val timeSpent = events.map{x => 
          (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double])
      }.sum
      val averageTsSession = timeSpent/numSessions  
      val interactionsMinSession = events.map{ x => 
          (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("interactEventsPerMin").get.asInstanceOf[Double])
      }.asInstanceOf[List[Double]]
      val averageInteractionsMin = ((interactionsMinSession.map(x => x).sum)/interactionsMinSession.size)
      val numWeeks = getWeeksBetween(startDate,eventStartTimestamp)
      val numSessionsWeek = numSessions/numWeeks//if(numWeeks>numSessions) numSessions else numSessions/numWeeks
      val tsWeek = timeSpent/numWeeks
     
      (gameId,startDate,numSessions,timeSpent,averageTsSession,interactionsMinSession,averageInteractionsMin,numSessionsWeek,tsWeek)
      
    }.map{f => {
      ContentSummary(f._2._1,f._2._2,f._2._3,f._2._4,f._2._5,f._2._6,f._2._7,f._2._8,f._2._9)
    } 
    }.cache();
    contentSummary.saveToCassandra("content_db", "contentsummarizer");
    contentSummary.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
  }

  private def getMeasuredEvent(userMap: (ContentSummary), config: Map[String, AnyRef]): MeasuredEvent = {
        val game = userMap;
        val mid = CommonUtil.getMessageId("ME_CONTENT_SUMMARY", null, "CONTENT", DtRange(0,0), game.content_id);
        val measures = Map(
            "timeSpent" -> game.total_ts,
            "numSessions" -> game.total_num_sessions,
            "averageTsSession" -> game.average_ts_session,
            "interactionsMinSession" -> game.interactions_min_session,
            "averageInteractionsMin" -> game.average_interactions_min,
            "numSessionsWeek" -> game.num_sessions_week,
            "tsWeek" -> game.ts_week);
        MeasuredEvent("ME_CONTENT_SUMMARY", System.currentTimeMillis(), 0, "1.0", mid, Option(userMap.content_id), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "CONTENT", DtRange(0,0)),
            Dimensions(None, None, None, None, None, None, None),
            MEEdata(measures));
    
  }
  private def getWeeksBetween(fromDate: Long, toDate: Long): Int = {
    val from = new LocalDate(fromDate)
    val to = new LocalDate(toDate)
    val dates = CommonUtil.datesBetween(from,to)
    val weeks = dates.size/7
    weeks
  }
}