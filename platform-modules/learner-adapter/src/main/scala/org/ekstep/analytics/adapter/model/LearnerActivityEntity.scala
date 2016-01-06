package org.ekstep.analytics.adapter.model

import scala.concurrent.Future
import com.websudos.phantom.dsl._
import com.websudos.phantom.builder.query.InsertQuery
import com.websudos.phantom.builder.Unspecified

/**
 * @author Santhosh
 */

case class LearnerActivitySummary(leaner_id: UUID, m_time_spent: Double, m_active_time_on_pf: Double, m_interrupt_time: Double, t_ts_on_pf: Double,
                                  m_ts_on_an_act: Map[String,Double], m_count_on_an_act: Map[String,Double], n_of_sess_on_pf: Int, l_visit_ts: DateTime, 
                                  most_active_hr_of_the_day: Int, top_k_content: List[String], sess_start_time: DateTime, sess_end_time: DateTime,
                                  dp_start_time: DateTime, dp_end_time: DateTime)

class LearnerActivityEntity extends CassandraTable[LearnerActivityDAO, LearnerActivitySummary] {

    object leaner_id extends UUIDColumn(this) with PartitionKey[UUID]
    object m_time_spent extends DoubleColumn(this)
    object m_active_time_on_pf extends DoubleColumn(this)
    object m_interrupt_time extends DoubleColumn(this)
    object t_ts_on_pf extends DoubleColumn(this)
    object m_ts_on_an_act extends MapColumn[LearnerActivityDAO, LearnerActivitySummary, String, Double](this)
    object m_count_on_an_act extends MapColumn[LearnerActivityDAO, LearnerActivitySummary, String, Double](this)
    object n_of_sess_on_pf extends IntColumn(this)
    object l_visit_ts extends DateTimeColumn(this)
    object most_active_hr_of_the_day extends IntColumn(this)
    object top_k_content extends ListColumn[LearnerActivityDAO, LearnerActivitySummary, String](this)
    object sess_start_time extends DateTimeColumn(this)
    object sess_end_time extends DateTimeColumn(this)
    object dp_start_time extends DateTimeColumn(this)
    object dp_end_time extends DateTimeColumn(this)

    def fromRow(row: Row): LearnerActivitySummary = {
        LearnerActivitySummary(leaner_id(row), m_time_spent(row), m_active_time_on_pf(row), m_interrupt_time(row),
            t_ts_on_pf(row), m_ts_on_an_act(row), m_count_on_an_act(row), n_of_sess_on_pf(row), l_visit_ts(row),
            most_active_hr_of_the_day(row), top_k_content(row), sess_start_time(row), sess_end_time(row),
            dp_start_time(row), dp_end_time(row))
    }
}

abstract class LearnerActivityDAO extends LearnerActivityEntity with RootConnector {

    def store(learnerActSumm: LearnerActivitySummary): Future[ResultSet] = {
        getInsertQuery(learnerActSumm).future();
    }

    private def getInsertQuery(learnerActSumm: LearnerActivitySummary): InsertQuery[LearnerActivityDAO, LearnerActivitySummary, Unspecified] = {
        insert.value(_.leaner_id, learnerActSumm.leaner_id)
            .value(_.m_time_spent, learnerActSumm.m_time_spent)
            .value(_.m_active_time_on_pf, learnerActSumm.m_active_time_on_pf)
            .value(_.m_interrupt_time, learnerActSumm.m_interrupt_time)
            .value(_.t_ts_on_pf, learnerActSumm.t_ts_on_pf)
            .value(_.m_ts_on_an_act, learnerActSumm.m_ts_on_an_act)
            .value(_.m_count_on_an_act, learnerActSumm.m_count_on_an_act)
            .value(_.n_of_sess_on_pf, learnerActSumm.n_of_sess_on_pf)
            .value(_.l_visit_ts, learnerActSumm.l_visit_ts)
            .value(_.most_active_hr_of_the_day, learnerActSumm.most_active_hr_of_the_day)
            .value(_.top_k_content, learnerActSumm.top_k_content)
            .value(_.sess_start_time, learnerActSumm.sess_start_time)
            .value(_.sess_end_time, learnerActSumm.sess_end_time)
            .value(_.dp_start_time, learnerActSumm.dp_start_time)
            .value(_.dp_end_time, learnerActSumm.dp_end_time);
    }

    def getById(id: UUID): Future[Option[LearnerActivitySummary]] = {
        select.where(_.leaner_id eqs id).one()
    }
    
    def count(): Future[Option[Long]] = {
        select.count().one();
    }
}