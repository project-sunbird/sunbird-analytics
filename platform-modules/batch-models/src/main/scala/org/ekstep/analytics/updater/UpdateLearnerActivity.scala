package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Filter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.util.UUID
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.JSONUtils

case class LearnerSnapshot(learner_id: String, m_time_spent: Double, m_time_btw_gp: Double, m_active_time_on_pf: Double, m_interrupt_time: Double, t_ts_on_pf: Double,
                           m_ts_on_an_act: Map[String, Double], m_count_on_an_act: Map[String, Double], n_of_sess_on_pf: Int, l_visit_ts: DateTime,
                           most_active_hr_of_the_day: Int, top_k_content: List[String], sess_start_time: DateTime, sess_end_time: DateTime,
                           dp_start_time: DateTime, dp_end_time: DateTime)

/**
 * @author Santhosh
 */
object UpdateLearnerActivity extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val filteredData = DataFilter.filter(DataFilter.filter(events, Filter("eid", "EQ", Option("ME_LEARNER_ACTIVITY_SUMMARY"))), Filter("uid", "ISNOTEMPTY", None));
        
        val la = filteredData.map { event =>

            val eks = event.edata.eks.asInstanceOf[Map[String, AnyRef]];
            val leaner_id = event.uid.get;
            val m_time_spent = eks.getOrElse("meanTimeSpent", 0d).asInstanceOf[Double];
            val m_time_btw_gp = eks.getOrElse("meanTimeBtwnGamePlays", 0d).asInstanceOf[Double];
            val m_active_time_on_pf = eks.getOrElse("meanActiveTimeOnPlatform", 0d).asInstanceOf[Double];
            val m_interrupt_time = eks.getOrElse("meanInterruptTime", 0d).asInstanceOf[Double];
            val t_ts_on_pf = eks.getOrElse("totalTimeSpentOnPlatform", 0d).asInstanceOf[Double];
            val n_of_sess_on_pf = eks.getOrElse("numOfSessionsOnPlatform", 0).asInstanceOf[Int];
            val m_ts_on_an_act = eks.getOrElse("meanTimeSpentOnAnAct", Map()).asInstanceOf[Map[String, Double]];
            val m_count_on_an_act = eks.getOrElse("meanCountOfAct", Map()).asInstanceOf[Map[String, Double]];
            val l_visit_ts = new DateTime(eks.getOrElse("last_visit_ts", 0L).asInstanceOf[Long]);
            val sess_start_time = new DateTime(eks.getOrElse("start_ts", 0L).asInstanceOf[Long]);
            val sess_end_time = new DateTime(eks.getOrElse("end_ts", 0L).asInstanceOf[Long]);
            val most_active_hr_of_the_day = eks.getOrElse("mostActiveHrOfTheDay", 0).asInstanceOf[Int];
            val top_k_content = eks.getOrElse("topKcontent", List[String]()).asInstanceOf[List[String]];
            val dp_start_time = new DateTime(event.context.date_range.from);
            val dp_end_time = new DateTime(event.context.date_range.to);
            LearnerSnapshot(leaner_id, m_time_spent, m_time_btw_gp, m_active_time_on_pf, m_interrupt_time, t_ts_on_pf, m_ts_on_an_act, m_count_on_an_act,
                n_of_sess_on_pf, l_visit_ts, most_active_hr_of_the_day, top_k_content, sess_start_time, sess_end_time, dp_start_time, dp_end_time)
        }.filter(_ != null);
        la.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_SNAPSHOT_TABLE);
        println("Count:", la.count());
        la.map { x => JSONUtils.serialize(x) };
    }
}