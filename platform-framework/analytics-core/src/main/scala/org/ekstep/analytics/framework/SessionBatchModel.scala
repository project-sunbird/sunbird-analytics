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
                events.sortBy { x => CommonUtil.getEventTS(x) }.groupBy { e => e.sid }.mapValues { x =>
                    var sessions = Buffer[Buffer[Event]]();
                    var tmpArr = Buffer[Event]();
                    x.foreach { y =>
                        y.eid match {
                            case "OE_START" =>
                                if (tmpArr.length > 0) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                }
                                tmpArr += y;   
                            case "OE_END" =>
                                tmpArr += y;
                                sessions += tmpArr;
                                tmpArr = Buffer[Event]();
                            case _ =>
                                tmpArr += y;
                        }
                    }   
                    sessions += tmpArr;
                    sessions;
                }.map(f => f._2).reduce((a,b) => a ++ b);
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }
    
    def getGameSessionsV2(data: RDD[TelemetryEventV2]): RDD[(String, Buffer[TelemetryEventV2])] = {
        data.filter { x => x.uid != null }
            .map(event => (event.uid, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                events.sortBy { x => x.ets }.groupBy { e => e.sid }.mapValues { x =>
                    var sessions = Buffer[Buffer[TelemetryEventV2]]();
                    var tmpArr = Buffer[TelemetryEventV2]();
                    x.foreach { y =>
                        y.eid match {
                            case "OE_START" =>
                                if (tmpArr.length > 0) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[TelemetryEventV2]();
                                }
                                tmpArr += y;   
                            case "OE_END" =>
                                tmpArr += y;
                                sessions += tmpArr;
                                tmpArr = Buffer[TelemetryEventV2]();
                            case _ =>
                                tmpArr += y;
                        }
                    }   
                    sessions += tmpArr;
                    sessions;
                }.map(f => f._2).reduce((a,b) => a ++ b);
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }

}