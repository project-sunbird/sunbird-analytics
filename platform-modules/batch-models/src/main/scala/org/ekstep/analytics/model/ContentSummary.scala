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
import org.apache.spark.HashPartitioner

case class Content_Summary(content_id: String,start_date:Long,total_num_sessions:Long, total_ts:Double,
                   average_ts_session:Double,interactions_min_session:List[Double],average_interactions_min:Double,
                   num_sessions_week:Long,ts_week:Double)
case class ContentId(content_id: String)
                           
object ContentSummary extends IBatchModel[MeasuredEvent] with Serializable {
  
  def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
      
      println("### Running the model ContentSummary ###");
      val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
      val sortedEvents = filteredEvents.sortBy { x => x.ets };
      val config = jobParams.getOrElse(Map[String, AnyRef]());
      val configMapping = sc.broadcast(config);
     
      val contentMap = sortedEvents.groupBy { x => x.dimensions.gdata.get.id }
      val prevContentState = filteredEvents.map(f=>ContentId(f.dimensions.gdata.get.id)).joinWithCassandraTable[Content_Summary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SUMMARY_TABLE).map(f => (f._1.content_id, f._2))
      val contentData = prevContentState.leftOuterJoin(contentMap)
  
      val contentSummary = contentData.partitionBy(new HashPartitioner(JobContext.parallelization)).mapValues { events =>
          
          val MeasEvents = events._2.get
          val firstEvent = MeasEvents.head
          val lastEvent = MeasEvents.last
          val eventSyncts = lastEvent.syncts
          val gameId = firstEvent.dimensions.gdata.get.id
          val gameVersion = firstEvent.ver
          val eventStartTimestamp = firstEvent.context.date_range.from
          val eventEndTimestamp = lastEvent.context.date_range.to
          val date_range = DtRange(eventStartTimestamp,eventEndTimestamp)
          val prevStartDate = events._1.start_date
          var startDate = 0l
          var numSessions = 0l
          var timeSpent = 0d
          var interactionsMinSession:List[Double] = Nil
          var numWeeks =0
          if(prevStartDate > eventStartTimestamp)
          {
              startDate = prevStartDate
              numSessions = MeasEvents.size + events._1.total_num_sessions
              timeSpent = MeasEvents.map{x => 
                  (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double])
              }.sum + events._1.total_ts
              var prevInter = events._1.interactions_min_session
              interactionsMinSession = MeasEvents.map{ x => 
                  (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("interactEventsPerMin").get.asInstanceOf[Double])
              }.asInstanceOf[List[Double]].++(prevInter)
              numWeeks = getWeeksBetween(startDate,eventEndTimestamp)
          }
          else
          {
              startDate = eventStartTimestamp
              numSessions = MeasEvents.size 
              timeSpent = MeasEvents.map{x => 
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double])
              }.sum  
              interactionsMinSession = MeasEvents.map{ x => 
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("interactEventsPerMin").get.asInstanceOf[Double])
              }.asInstanceOf[List[Double]]
              numWeeks = getWeeksBetween(startDate,eventEndTimestamp)
          }
          val averageTsSession = CommonUtil.roundDouble((timeSpent/numSessions), 4)  
          val averageInteractionsMin = ((interactionsMinSession.map(x => x).sum)/interactionsMinSession.size)
          val numSessionsWeek = if(numWeeks==0) numSessions else numSessions/numWeeks
          val tsWeek = if(numWeeks==0) timeSpent else numSessions/numWeeks 
          
          (Content_Summary(gameId,startDate,numSessions,timeSpent,averageTsSession,interactionsMinSession,CommonUtil.roundDouble(averageInteractionsMin, 4),numSessionsWeek,tsWeek),date_range,eventSyncts,gameVersion)     
        
      }
      
      contentSummary.map(f => f._2._1).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SUMMARY_TABLE);
      
      contentSummary.map(f => {
            getMeasuredEvent(f._2._1, configMapping.value,f._2._2,f._2._3,f._2._4);
      }).map { x => JSONUtils.serialize(x) };
  }

  private def getMeasuredEvent(contentSumm: Content_Summary, config: Map[String, AnyRef],date_range:DtRange,eventSyncts: Long,game_version:String): MeasuredEvent = {
        
        val mid = CommonUtil.getMessageId("ME_CONTENT_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], date_range, contentSumm.content_id);
        val measures = Map(
            "timeSpent" -> contentSumm.total_ts,
            "numSessions" -> contentSumm.total_num_sessions,
            "averageTsSession" -> contentSumm.average_ts_session,
            "interactionsMinSession" -> contentSumm.interactions_min_session,
            "averageInteractionsMin" -> contentSumm.average_interactions_min,
            "numSessionsWeek" -> contentSumm.num_sessions_week,
            "tsWeek" -> contentSumm.ts_week);
        MeasuredEvent("ME_CONTENT_SUMMARY", System.currentTimeMillis(), eventSyncts, "1.0", mid, None, None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], date_range),
            Dimensions(None, None, Option(new GData(contentSumm.content_id, game_version)), None, None, None, None),
            MEEdata(measures));
  }
  
  private def getWeeksBetween(fromDate: Long, toDate: Long): Int = {
        val from = new LocalDate(fromDate)
        val to = new LocalDate(toDate)
        val dates = CommonUtil.datesBetween(from,to)
        dates.size/7
  }
}