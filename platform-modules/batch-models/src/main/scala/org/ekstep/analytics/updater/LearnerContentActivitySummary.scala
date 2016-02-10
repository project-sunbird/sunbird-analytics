package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.JobContext
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JSONUtils

case class LearnerContentActivity(learner_id: String, content_id: String, time_spent: Double, interactions_per_min: Double, num_of_sessions_played: Int);

object LearnerContentActivitySummary extends IBatchModel[MeasuredEvent] with Serializable {

    private def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
        num.toDouble(ts.sum) / ts.size
    }

    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        val activity = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x =>
                val learner_id = x._1;
                val allEvents = x._2;
                val perContentAct = allEvents.sortBy { x => x.ets }.groupBy { x => x.dimensions.gdata.get.id }.map { x =>
                    val content = x._1;
                    val events = x._2;
                    val numOfSessionsPlayed = events.length;
                    val eksMap = events.map { x => x.edata.eks }.map { x => x.asInstanceOf[Map[String, AnyRef]] };
                    val timeSpent = eksMap.map { x => x.get("timeSpent").get.asInstanceOf[Double] }.sum;
                    val interactionsPerMin = average(eksMap.map(f => f.get("interactEventsPerMin").get.asInstanceOf[Double]));
                    LearnerContentActivity(learner_id, content, timeSpent, interactionsPerMin, numOfSessionsPlayed);
                }
                perContentAct;
            }.flatMap { x => x };
        activity.saveToCassandra("learner_db", "learnercontentsummary");
        activity.map { x => JSONUtils.serialize(x) };
    }
}