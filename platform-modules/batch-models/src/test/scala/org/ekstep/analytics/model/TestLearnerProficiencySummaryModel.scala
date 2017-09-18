package org.ekstep.analytics.model

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import com.datastax.spark.connector._
import org.joda.time.DateTime
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import scala.collection.mutable.Buffer

class TestLearnerProficiencySummaryModel extends SparkSpec(null) {

    "LearnerProficiencySummaryModel" should "check Proficiency and Model param should be updated in db" in {

        val modelParams = Map("alpha" -> 1, "beta" -> 1);
        // Override user data in learnerproficiency table
        val learnerProf = LearnerProficiency("8b4f3775-6f65-4abf-9afa-b15b8f82a24b", Map("Num:C3:SC1:MC12" -> 0.5), DateTime.now(), DateTime.now(), Map("Num:C3:SC1:MC12" -> JSONUtils.serialize(modelParams)));
        val rdd = sc.parallelize(Array(learnerProf));
        rdd.saveToCassandra("local_learner_db", "learnerproficiency");

        val rdd0 = loadFile[DerivedEvent]("src/test/resources/learner-proficiency/proficiency_update_db_test1.log");
        val rdd01 = LearnerProficiencySummaryModel.execute(rdd0, Option(Map("apiVersion" -> "v2")));
        val proficiency1 = sc.cassandraTable[LearnerProficiency]("local_learner_db", "learnerproficiency").where("learner_id = ?", "8b4f3775-6f65-4abf-9afa-b15b8f82a24b").first();

        // Check Proficiency and Model parameter values - Iteration 1
        proficiency1.model_params.contains("Num:C3:SC1:MC12") should be(true);
        val modelParams1 = JSONUtils.deserialize[Map[String, Double]](proficiency1.model_params.get("Num:C3:SC1:MC12").get);
        modelParams1.get("alpha").get should be(4.0);
        modelParams1.get("beta").get should be(2.0);

        proficiency1.proficiency.contains("Num:C3:SC1:MC12") should be(true);
        proficiency1.proficiency.get("Num:C3:SC1:MC12").get should be(0.67);

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/learner-proficiency/proficiency_update_db_test2.log");
        val rdd11 = LearnerProficiencySummaryModel.execute(rdd1, Option(Map("apiVersion" -> "v2")));

        // Check Proficiency and Model parameter values - Iteration 2
        val proficiency2 = sc.cassandraTable[LearnerProficiency]("local_learner_db", "learnerproficiency").where("learner_id = ?", "8b4f3775-6f65-4abf-9afa-b15b8f82a24b").first();

        proficiency2.model_params.contains("Num:C3:SC1:MC12") should be(true)
        proficiency2.model_params.contains("Num:C3:SC1:MC13") should be(true)
        val modelParams2 = JSONUtils.deserialize[Map[String, Double]](proficiency2.model_params.get("Num:C3:SC1:MC12").get);
        modelParams2.get("alpha").get should be(6);
        modelParams2.get("beta").get should be(2);

        val modelParams3 = JSONUtils.deserialize[Map[String, Double]](proficiency2.model_params.get("Num:C3:SC1:MC13").get);
        modelParams3.get("alpha").get should be(2.5);
        modelParams3.get("beta").get should be(1);

        proficiency2.proficiency.contains("Num:C3:SC1:MC12") should be(true)
        proficiency2.proficiency.get("Num:C3:SC1:MC12").get should be(0.75);

        proficiency2.proficiency.contains("Num:C3:SC1:MC13") should be(true)
        proficiency2.proficiency.get("Num:C3:SC1:MC13").get should be(0.71);

        val out = rdd01.collect();
        out.length should be(1)
        val event1 = out(0);
//        event1.mid should be("408D620EDDFE92D28BB87F88F90F1894");
        event1.syncts should be(1453207670750L);

        val out1 = rdd11.collect();
        out1.length should be(1)
        val event2 = out1(0);
//        event2.mid should be("408D620EDDFE92D28BB87F88F90F1894");
        event2.syncts should be(1453207670750L);
    }

    it should "print the item data for testing" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/learner-proficiency/test.log");
        val rdd2 = LearnerProficiencySummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2")));
        var out = rdd2.collect();
        out.length should be(44)
    }

    it should "check the zero Proficiency Updater event is coming" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/learner-proficiency/emptyMC_test.log");
        val rdd2 = LearnerProficiencySummaryModel.execute(rdd, Option(Map("apiVersion" -> "v2")));
        var out = rdd2.collect();
        out.length should be(2)
    }

    it should "compute proficiency fetch item data from v2 domain model" in {

        val modelParams = Map("alpha" -> 1, "beta" -> 1);
        val learnerProf = LearnerProficiency("53ef3f1f-40e7-4f18-82aa-db2ad920a4c0", Map(), DateTime.now(), DateTime.now(), Map());
        val rdd = sc.parallelize(Array(learnerProf));
        rdd.saveToCassandra("local_learner_db", "learnerproficiency");

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/learner-proficiency/test_datav2.log");
        val rdd2 = LearnerProficiencySummaryModel.execute(rdd1, Option(Map("apiVersion" -> "v2")));
        var out = rdd2.collect();
        out.length should be(1)

        val event = out(0);
        val profsList = event.edata.eks.asInstanceOf[Map[String, AnyRef]].get("proficiencySummary").get.asInstanceOf[List[ProficiencySummary]];
        profsList.size should be(22);
        val profs = profsList.map { x => (x.conceptId, x.proficiency) }.toMap

        profs.get("Num:C3:SC3:MC4").get should be(0.6);
        profs.get("Num:C4:SC1:MC6").get should be(0.6);
        profs.get("Num:C3:SC7:MC8").get should be(0.6);
        profs.get("Num:C3:SC9:MC1").get should be(0.6);
        profs.get("Num:C1:SC2:MC23").get should be(0.6);
        profs.get("Num:C2:SC3:M7").get should be(0.6);
        profs.get("Num:C4:SC5:MC12").get should be(0.6);
        profs.get("Num:C1:SC2:MC22").get should be(0.6);
        profs.get("Num:C3:SC1:MC4").get should be(0.71);
        profs.get("Num:C4:SC5:MC9").get should be(0.6);
        profs.get("Num:C4:SC2:MC5").get should be(0.6);
        profs.get("Num:C4:SC6:MC1").get should be(0.2);
        profs.get("Num:C1:SC2:MC15").get should be(0.6);
        profs.get("Num:C3:SC1:MC3").get should be(0.71);
        profs.get("Num:C2:SC2:MC5").get should be(0.87);
        profs.get("Num:C4:SC7:MC1").get should be(0.2);
        profs.get("Num:C1:SC2:MC16").get should be(0.6);
        profs.get("Num:C3:SC2:MC5").get should be(0.6);

        val lp = sc.cassandraTable[LearnerProficiency]("local_learner_db", "learnerproficiency").where("learner_id = ?", "53ef3f1f-40e7-4f18-82aa-db2ad920a4c0").first();
        lp.proficiency.size should be(22);
        lp.model_params.size should be(22);
        lp.proficiency should be(profs)
    }

    it should " test the algo where concept is not empty " in {
        val learner_id = "test_learner_id123";
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM local_learner_db.learnerproficiency where learner_id = '" + learner_id + "'");
        }
        val rdd = loadFile[DerivedEvent]("src/test/resources/learner-proficiency/test1.log");
        val rdd2 = LearnerProficiencySummaryModel.execute(rdd, Option(Map("modelVersion" -> "1.0", "modelId" -> "ProficiencyUpdater")));
        var out = rdd2.collect();
        out.length should be(1)
    }
}