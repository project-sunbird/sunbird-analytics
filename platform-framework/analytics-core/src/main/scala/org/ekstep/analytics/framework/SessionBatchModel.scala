package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner

/**
 * @author Santhosh
 */
trait SessionBatchModel[T] extends IBatchModel[T] {

    def getGameSessions(events: RDD[Event]): RDD[(String, Buffer[Event])] = {
        events.filter { x => x.uid != null }
            .map(event => (event.uid, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
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
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }
    
    def getGameSessionsV2(events: RDD[TelemetryEventV2]): RDD[(String, Buffer[TelemetryEventV2])] = {
        events.filter { x => x.uid != null }
            .map(event => (event.uid, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
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
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }

}