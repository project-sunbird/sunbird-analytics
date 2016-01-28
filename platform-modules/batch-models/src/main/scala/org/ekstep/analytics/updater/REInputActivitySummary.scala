package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import scala.collection.mutable.Buffer
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.annotation.migration
import scala.reflect.runtime.universe

case class Activity(learner_id: String, content: String, time_spent: Double, interactions_per_min: Double, num_of_sessions_played: Int);
object REInputActivitySummary {
    def writeIntoDB(sc: SparkContext, events: RDD[MeasuredEvent]) {
        val activity = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x =>
                val learner_id = x._1;
                val allEvents = x._2;
                val perContentEvents = allEvents.groupBy { x => x.gdata.get.id }
                val perContentAct = perContentEvents.map { x =>
                    val content = x._1;
                    val events = x._2;
                    val numOfSessionsPlayed = events.length;
                    val eksMap = events.map { x => x.edata.eks }.map { x => x.asInstanceOf[Map[String, AnyRef]] };
                    val timeSpent = eksMap.map { x => x.getOrElse("timeSpent", 0d).asInstanceOf[Double] }.sum;
                    val interactionsPerMin = eksMap.map(f => f.getOrElse("interactEventsPerMin", 0d).asInstanceOf[Double]).sum;
                    Activity(learner_id, content, timeSpent, interactionsPerMin, numOfSessionsPlayed);
                }
                perContentAct;
            }.flatMap { x => x };
            activity.saveToCassandra("learner_db", "learnerproficiency");
    }
}