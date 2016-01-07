package org.ekstep.analytics.adapter.learner

import org.scalatest._
import java.util.UUID
import org.joda.time.DateTime
import org.ekstep.analytics.adapter.model.LearnerSnapshot
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * @author Santhosh
 */
class TestLearnerAdapter extends FlatSpec with Matchers with Defaults.connector.Connector {

    "LearnerAdapter" should "insert a record into leaner activity summary table" in {
        
        val count1 = Await.result(LearnerAdapter.LearnerSnapshot.count(), 5.seconds).get;
        val learnerActSumm = LearnerSnapshot(UUID.randomUUID(), 12.53, 10.56, 450.34, 10.23, 430.00, Map("TOUCH" -> 12.12), Map("TOUCH" -> 50.12), 45,
            new DateTime(1451915786364L), 16, List("numeracy_369", "numeracy_377"), new DateTime(1451915786364L), new DateTime(1451915786405L),
            new DateTime(1449914267000L), new DateTime(1449914532000L));
        val res = Await.result(LearnerAdapter.LearnerSnapshot.store(learnerActSumm), 5.seconds);
        val count2 = Await.result(LearnerAdapter.LearnerSnapshot.count(), 5.seconds).get;
        count2 should be > count1;
        count2-count1 should be (1);
    }
    
    it should "fetch data from learner activity summary table" in {
        
        val uuid = UUID.randomUUID();
        val learnerActSumm = LearnerSnapshot(uuid, 12.53, 54.00, 450.34, 10.23, 430.00, Map("TOUCH" -> 13.12), Map("TOUCH" -> 60.12), 45,
            new DateTime(1451915786364L), 16, List("numeracy_369", "numeracy_377"), new DateTime(1451915786364L), new DateTime(1451915786405L),
            new DateTime(1449914267000L), new DateTime(1449914532000L));
        Await.result(LearnerAdapter.LearnerSnapshot.store(learnerActSumm), 5.seconds);
        val res = Await.result(LearnerAdapter.LearnerSnapshot.getById(uuid), 5.seconds).get;
        res.dp_end_time.getMillis should be (1449914532000L)
        res.dp_start_time.getMillis should be (1449914267000L)
        res.sess_end_time.getMillis should be (1451915786405L)
        res.sess_start_time.getMillis should be (1451915786364L)
        res.l_visit_ts.getMillis should be (1451915786364L)
        res.leaner_id should be (uuid)
        res.m_active_time_on_pf should be (450.34)
        res.m_time_spent should be (12.53)
        res.m_interrupt_time should be (10.23)
        res.t_ts_on_pf should be (430.00)
        res.n_of_sess_on_pf should be (45)
        res.most_active_hr_of_the_day should be (16)
        res.top_k_content should be (List("numeracy_369", "numeracy_377"))
        res.m_ts_on_an_act.get("TOUCH").get should be (13.12)
        res.m_count_on_an_act.get("TOUCH").get should be (60.12)
    }
    
}