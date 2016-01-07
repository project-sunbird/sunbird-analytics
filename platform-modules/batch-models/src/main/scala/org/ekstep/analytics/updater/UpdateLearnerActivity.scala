package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.adapter.model.LearnerSnapshot
import java.util.UUID
import org.joda.time.DateTime
import org.ekstep.analytics.adapter.learner.LearnerAdapter
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Future

/**
 * @author Santhosh
 */
object UpdateLearnerActivity extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val la = events.map { event =>
            val eks = event.edata.eks.asInstanceOf[Map[String, AnyRef]];
            val leaner_id = UUID.fromString(event.uid.get);
            val m_time_spent = eks.getOrElse("meanTimeSpent", 0d).asInstanceOf[Double];
            val m_time_btw_gp = eks.getOrElse("meanTimeBtwnGamePlays", 0d).asInstanceOf[Double];
            val m_active_time_on_pf = eks.getOrElse("meanActiveTimeOnPlatform", 0d).asInstanceOf[Double];
            val m_interrupt_time = eks.getOrElse("meanInterruptTime", 0d).asInstanceOf[Double];
            val t_ts_on_pf = eks.getOrElse("totalTimeSpentOnPlatform", 0d).asInstanceOf[Double];
            val n_of_sess_on_pf = eks.getOrElse("numOfSessionsOnPlatform", 0d).asInstanceOf[Int];
            val m_ts_on_an_act = eks.getOrElse("meanTimeSpentOnAnAct", Map()).asInstanceOf[Map[String, Double]];
            val m_count_on_an_act = eks.getOrElse("meanCountOfAct", Map()).asInstanceOf[Map[String, Double]];
            val l_visit_ts = new DateTime(eks.getOrElse("lastVisitTimeStamp", 0L).asInstanceOf[Long]);

            val most_active_hr_of_the_day = eks.getOrElse("mostActiveHrOfTheDay", 0).asInstanceOf[Int];
            val top_k_content = eks.getOrElse("topKcontent", List[String]()).asInstanceOf[List[String]];
            val sess_start_time = new DateTime(eks.getOrElse("startTimestamp", 0L).asInstanceOf[Long]);
            val sess_end_time = new DateTime(eks.getOrElse("endTimestamp", 0L).asInstanceOf[Long])
            val dp_start_time = new DateTime(event.context.dt_range.from)
            val dp_end_time = new DateTime(event.context.dt_range.to)

            LearnerSnapshot(leaner_id, m_time_spent, m_time_btw_gp, m_active_time_on_pf, m_interrupt_time, t_ts_on_pf, m_ts_on_an_act, m_count_on_an_act,
                n_of_sess_on_pf, l_visit_ts, most_active_hr_of_the_day, top_k_content, sess_start_time, sess_end_time, dp_start_time, dp_end_time)
        }.collect();
        val futures = la.map(LearnerAdapter.LearnerSnapshot.store(_)).toList;
        val f = Future.sequence(futures);
        val res = Await.result(Future.sequence(futures), 10.seconds);
        val updatedRecords = res.map { x => if(x.wasApplied) 1 else 0 }.sum;
        sc.parallelize(Array(la.length,updatedRecords).map(x => x.toString()), 1);
    }
}