package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
trait SessionBatchModel[T] extends IBatchModel[T] {

    def getGameSessions(data: RDD[Event]): RDD[(String, Buffer[Event])] = {
        data.filter { x => x.uid != null }
            .map(event => (event.uid, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                events.sortBy { x => CommonUtil.getEventTS(x) }.groupBy { e => (e.sid,e.ver) }.mapValues { x =>
                    var sessions = Buffer[Buffer[Event]]();
                    var tmpArr = Buffer[Event]();
                    var lastContentId:String = x(0).gdata.id;
                    x.foreach { y =>
                        y.eid match {
                            case "OE_START" =>
                                if (tmpArr.length > 0) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                }
                                tmpArr += y;  
                                lastContentId = y.gdata.id;
                            case "OE_END" =>
                                tmpArr += y;
                                sessions += tmpArr;
                                tmpArr = Buffer[Event]();
                            case _ =>
                                if(!lastContentId.equals(y.gdata.id)) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                }
                                tmpArr += y;
                                lastContentId = y.gdata.id;
                        }
                    }   
                    sessions += tmpArr;
                    sessions;
                }.map(f => f._2).reduce((a,b) => a ++ b);
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }
    
    def getGenieLaunchSessions(data: RDD[Event]): RDD[(String, Buffer[Event])] = {
        data.filter { x => x.did != null }
            .map(event => (event.did, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                events.sortBy { x => CommonUtil.getEventTS(x) }.groupBy { e => (e.sid,e.ver) }.mapValues { x =>
                    var sessions = Buffer[Buffer[Event]]();
                    var tmpArr = Buffer[Event]();
                    x.foreach { y =>
                        y.eid match {
                            case "GE_GENIE_START" =>
                                if (tmpArr.length > 0) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                }
                                tmpArr += y;

                            case "GE_GENIE_END" =>
                                tmpArr += y;
                                sessions += tmpArr;
                                tmpArr = Buffer[Event]();
                            case _ =>
                                if (!tmpArr.isEmpty)
                                    tmpArr += y;
                        }
                    }
                    sessions += tmpArr;
                    sessions;
                }.map(f => f._2).reduce((a, b) => a ++ b);
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }
    
    def getGenieSessions(data: RDD[Event]): RDD[(String, Buffer[Event])] = {
        data.filter { x => x.did != null }
            .map(event => (event.did, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                events.sortBy { x => CommonUtil.getEventTS(x) }.groupBy { e => (e.sid,e.ver) }.mapValues { x =>
                    var sessions = Buffer[Buffer[Event]]();
                    var tmpArr = Buffer[Event]();
                    x.foreach { y =>
                        y.eid match {
                            case "GE_SESSION_START" =>
                                if (tmpArr.length > 0) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                }
                                tmpArr += y;

                            case "GE_SESSION_END" =>
                                tmpArr += y;
                                sessions += tmpArr;
                                tmpArr = Buffer[Event]();
                            case _ =>
                                if (!tmpArr.isEmpty)
                                    tmpArr += y;
                        }
                    }
                    sessions += tmpArr;
                    sessions;
                }.map(f => f._2).reduce((a, b) => a ++ b);
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }
}