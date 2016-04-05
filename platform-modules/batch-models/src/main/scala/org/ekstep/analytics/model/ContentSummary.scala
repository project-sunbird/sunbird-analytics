package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import scala.collection.mutable.HashMap

class Summary(val contentId: String,val ver: String, val dtRange: DtRange,val syncDate: Long, 
                 val loc: Option[String], val timeSpent: Double, val numSessions: Long, val averageTsSession: Double, 
                 val interactionsMinSession: List[Double], val averageInteractionsMin: Double, 
                 val numSessionsWeek: Map[String, Int], val tsWeek: Map[String, Double]) extends Serializable {};

object ContentSummary extends SessionBatchModel[Event] with Serializable {
  
  def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
    println("### Running the model ContentSummary ###");
    val filteredEvents = DataFilter.filter(data, Filter("eventId", "IN", Option(List("OE_ASSESS", "OE_START", "OE_END", "GE_GAME_END", "OE_INTERACT")))).sortBy { x => CommonUtil.getEventTS(x) };
    //val sortedEvents = filteredEvents.sortBy { x => CommonUtil.getEventTS(x) }
    println("### Broadcasting data to all worker nodes ###");
    val config = jobParams.getOrElse(Map[String, AnyRef]());
    val configMapping = sc.broadcast(config);
    val deviceMapping = sc.broadcast(JobContext.deviceMapping);
    
//    val gameSessions = getGameSessions(filteredEvents).map(x => x._2)
//    val sessions = gameSessions.map{x => x}.groupBy { x => x.head.gdata.id}.asInstanceOf[RDD[(String, Buffer[Event])]]
//    println("checking")
    
    val contentMap = filteredEvents.groupBy { x => x.gdata.id }//.asInstanceOf[RDD[(String,RDD[Event])]]
    val contentSummary = contentMap.mapValues { events =>
      val firstEvent = events.head//first();
      val lastEvent = events.last//take(events.count().asInstanceOf[Int]).last;
      val gameId = CommonUtil.getGameId(firstEvent);
      val gameVersion = CommonUtil.getGameVersion(lastEvent);
      val startTimestamp = Option(CommonUtil.getEventTS(firstEvent));
      val endTimestamp = Option(CommonUtil.getEventTS(lastEvent));
      val startDate = Option(CommonUtil.getEventDate(firstEvent));
      val lastDate = Option(CommonUtil.getEventDate(lastEvent));
      val loc = deviceMapping.value.getOrElse(firstEvent.did, "");
     
      val oeEnds = events.filter { x => "OE_END".equals(x.eid) };
      val numSessions = oeEnds.size//count()
      val timeSpent = oeEnds.map { x =>
        (x.edata.eks.length.asInstanceOf[Double])
      }.sum
      val averageTsSession = timeSpent/numSessions
//    println("checking outside sessions")     
//      val interactionsMinSession = sessions.mapValues { sess =>
//        println("checking inside sessions")
//        if(sess.head.gdata.id.equals(firstEvent.gdata.id))
//        {
//          val sessionsPerContent = sess.asInstanceOf[RDD[Event]]
//          val noOfInteractEvents = DataFilter.filter(sessionsPerContent, Filter("edata.eks.type", "IN", Option(List("TOUCH", "DRAG", "DROP", "PINCH", "ZOOM", "SHAKE", "ROTATE", "SPEAK", "LISTEN", "WRITE", "DRAW", "START", "END", "CHOOSE", "ACTIVATE")))).count();
//          var tmpLastEvent: Event = null;
//          val eventsWithTs = sessionsPerContent.map { x =>
//               if (tmpLastEvent == null) tmpLastEvent = x;
//               val ts = CommonUtil.getTimeDiff(tmpLastEvent, x).get;
//               tmpLastEvent = x;
//               (x, ts)
//           }
//          val tS = eventsWithTs.map(f => f._2).sum;
//          val interactEventsPerMin: Double = if (noOfInteractEvents == 0 || tS == 0) 0d else BigDecimal(noOfInteractEvents / (tS / 60)).toDouble;
//          interactEventsPerMin
//        }
//          
//      }.asInstanceOf[RDD[Double]]
//      val averageInteractionsMin = (interactionsMinSession.map{ x => x}.sum)/interactionsMinSession.count()
      val interactionsMinSession = null
      val averageInteractionsMin = 0d
      val numSessionsWeek = Map(1+":"+startDate.get+"-"+lastDate.get -> numSessions )
      //println(numSessionsWeek)
      val tsWeek = Map(1+":"+startDate.get+"-"+lastDate.get -> timeSpent )
      //println(tsWeek)
      new Summary(gameId,gameVersion,DtRange(startTimestamp.getOrElse(0l),endTimestamp.getOrElse(0l)), CommonUtil.getEventSyncTS(lastEvent), 
               Option(loc), timeSpent, numSessions, averageTsSession, interactionsMinSession.asInstanceOf[List[Double]], 
               averageInteractionsMin, numSessionsWeek, tsWeek )
    }
    contentSummary.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
  }
  
  private def getMeasuredEvent(userMap: (String, Summary), config: Map[String, AnyRef]): MeasuredEvent = {
        val game = userMap._2;
        val mid = CommonUtil.getMessageId("ME_CONTENT_SUMMARY", userMap._1, "CONTENT", game.dtRange, game.contentId);
        val measures = Map(
            "contentId" -> game.contentId,
            "timeSpent" -> game.timeSpent,
            "numSessions" -> game.numSessions,
            "averageTsSession" -> game.averageTsSession,
            "interactionsMinSession" -> game.interactionsMinSession,
            "averageInteractionsMin" -> game.averageInteractionsMin,
            "numSessionsWeek" -> game.numSessionsWeek,
            "tsWeek" -> game.tsWeek);
        MeasuredEvent("ME_CONTENT_SUMMARY", System.currentTimeMillis(), game.syncDate, "1.0", mid, Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "CONTENT", game.dtRange),
            Dimensions(None, Option(game.contentId), Option(new GData(game.contentId, game.ver)), None, None, None, game.loc),
            MEEdata(measures));
    
  }
}