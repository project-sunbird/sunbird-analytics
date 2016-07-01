package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.Filter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.JobContext
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.JobLogger

case class LearnerContentActivity(learner_id: String, content_id: String, time_spent: Double, interactions_per_min: Double, num_of_sessions_played: Int) extends AlgoOutput with Output;

object LearnerContentActivitySummary extends IBatchModelTemplate[DerivedEvent, DerivedEvent, LearnerContentActivity, LearnerContentActivity] with Serializable {

    val className = "org.ekstep.analytics.updater.LearnerContentActivitySummary"

    private def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
        num.toDouble(ts.sum) / ts.size
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY"))), Filter("uid", "ISNOTEMPTY", None));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LearnerContentActivity] = {
        data.map(event => (event.uid, Buffer(event)))
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
    }

    override def postProcess(data: RDD[LearnerContentActivity], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LearnerContentActivity] = {
        JobLogger.log("Saving learner content summary data to DB", className, None, None, None)
        data.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_CONTENT_SUMMARY_TABLE);
        data
    }
}