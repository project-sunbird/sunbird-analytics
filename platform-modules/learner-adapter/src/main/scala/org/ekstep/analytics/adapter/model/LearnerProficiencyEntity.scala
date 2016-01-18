package org.ekstep.analytics.adapter.model

import scala.concurrent.Future
import com.websudos.phantom.dsl._
import com.websudos.phantom.builder.query.InsertQuery
import com.websudos.phantom.builder.Unspecified

//case class LearnerProficiency(conecpt_id:String,prof:Float,startTime:Long,endTime:Long)
case class LearnerProficiencySnapshot(learner_id: UUID,conecpt_id:String,prof:Float,startTime:Long,endTime:Long)

class LearnerProficiencyEntity extends CassandraTable[LearnerProficiencyDAO, LearnerProficiencySnapshot]{
     object learner_id extends UUIDColumn(this) with PartitionKey[UUID]
     object conecpt_id extends StringColumn(this)
     object prof extends FloatColumn(this)
     object startTime extends LongColumn(this)
     object endTime extends LongColumn(this)
    
     def fromRow(row: Row): LearnerProficiencySnapshot = {
        LearnerProficiencySnapshot(learner_id(row), conecpt_id(row),prof(row),startTime(row),endTime(row))
    }
  
}
abstract class LearnerProficiencyDAO extends LearnerProficiencyEntity with RootConnector {

    def store(learnerActSumm: LearnerProficiencySnapshot): Future[ResultSet] = {
        getInsertQuery(learnerActSumm).future();
    }

    private def getInsertQuery(learnerActSumm: LearnerProficiencySnapshot): InsertQuery[LearnerProficiencyDAO, LearnerProficiencySnapshot, Unspecified] = {
        insert.value(_.learner_id, learnerActSumm.learner_id)
    }

    def getById(id: UUID): Future[Option[LearnerProficiencySnapshot]] = {
        select.where(_.learner_id eqs id).one()
    }
    
    def count(): Future[Option[Long]] = {
        select.count().one();
    }
}