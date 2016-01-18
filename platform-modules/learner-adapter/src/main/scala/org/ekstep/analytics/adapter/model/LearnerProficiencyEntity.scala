package org.ekstep.analytics.adapter.model

import scala.concurrent.Future
import com.websudos.phantom.dsl._
import com.websudos.phantom.builder.query.InsertQuery
import com.websudos.phantom.builder.Unspecified

case class LearnerProficiency(learner_id: String, proficiency: Map[String, Double], startTime: Long, endTime: Long)

class LearnerProficiencyEntity extends CassandraTable[LearnerProficiencyDAO, LearnerProficiency] {
    object learner_id extends StringColumn(this) with PartitionKey[String]
    object proficiency extends MapColumn[LearnerProficiencyDAO, LearnerProficiency, String, Double](this)
    object startTime extends LongColumn(this)
    object endTime extends LongColumn(this)

    def fromRow(row: Row): LearnerProficiency = {
        LearnerProficiency(learner_id(row), proficiency(row), startTime(row), endTime(row))
    }

}
abstract class LearnerProficiencyDAO extends LearnerProficiencyEntity with RootConnector {

    def store(learnerActSumm: LearnerProficiency): Future[ResultSet] = {
        getInsertQuery(learnerActSumm).future();
    }

    private def getInsertQuery(learnerActSumm: LearnerProficiency): InsertQuery[LearnerProficiencyDAO, LearnerProficiency, Unspecified] = {
        insert.value(_.learner_id, learnerActSumm.learner_id)
            .value(_.proficiency, learnerActSumm.proficiency)
            .value(_.startTime, learnerActSumm.startTime)
            .value(_.endTime, learnerActSumm.endTime)
    }

    def getById(id: String): Future[Option[LearnerProficiency]] = {
        select.where(_.learner_id eqs id).one()
    }
    
    def count(): Future[Option[Long]] = {
        select.count().one();
    }
}