package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util._
import java.io.FileWriter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

case class UserCount(uid: String, count: Int);

/**
 * @author Santhosh
 */
object ScreenTimeSummary extends SessionBatchModel[Event] with Serializable {

    def execute(sc: SparkContext, data: RDD[Event], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        
        val configMapping = sc.broadcast(jobParams.getOrElse(Map[String, AnyRef]()));
        val filteredData = data.filter { event =>  
            event.eid match {
                case "OE_INTERACT" =>
                    event.edata.ext != null && event.edata.ext.stageId != null;
                case _ =>
                    true
            }    
        }
        val gameSessions = getGameSessions(filteredData);
        gameSessions.mapValues { events =>
            val firstEvent = events(0);
            val lastEvent = events.last;
            val gdata = new GData(CommonUtil.getGameId(firstEvent), CommonUtil.getGameVersion(firstEvent))
            val dtRange = DtRange(CommonUtil.getEventTS(firstEvent), CommonUtil.getEventTS(lastEvent));
            var stageList = ListBuffer[(String, Double)]();
            var prevEvent = events(0);
            events.foreach { x =>  
                x.eid match {
                    case "OE_START" =>
                        stageList += Tuple2("splash", CommonUtil.getTimeDiff(prevEvent, x).get);
                    case "OE_INTERACT" =>
                        stageList += Tuple2(x.edata.ext.stageId, CommonUtil.getTimeDiff(prevEvent, x).get);
                    case "OE_INTERRUPT" =>
                        stageList += Tuple2(x.edata.eks.id, CommonUtil.getTimeDiff(prevEvent, x).get);
                    case "OE_END" =>
                        stageList += Tuple2("endStage", CommonUtil.getTimeDiff(prevEvent, x).get);
                }
                prevEvent = x;
            }
            
            var stageMap = HashMap[String, Double]();
            var currStage : String = null; 
            stageList.foreach { x =>
                if(currStage == null) {
                    currStage = x._1;
                }
                if(stageMap.getOrElse(currStage, null) == null) {
                    stageMap.put(currStage, x._2);
                } else {
                    stageMap.put(currStage, stageMap.get(currStage).get + x._2);
                }
                if(!currStage.equals(x._1)) {
                    currStage = x._1;
                }
            }
            (gdata, dtRange, stageMap);
        }.map(f => {
            val config = configMapping.value;
            MeasuredEvent("ME_SCREEN_SUMMARY", System.currentTimeMillis(), "1.0", Option(f._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ScreenTimeSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "SESSION", f._2._2),
            Dimensions(None, Option(f._2._1), None, None, None, None),
            MEEdata(f._2._3));
        }).map { x => JSONUtils.serialize(x) };
        
    }
}