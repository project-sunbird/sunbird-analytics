package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner

/**
 * @author Santhosh
 */
trait SessionBatchModel extends IBatchModel {

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

}