package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.JobContext
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.adapter.learner.LearnerAdapter
import scala.concurrent.Future

case class Assessment(learner_id: String, itemId: String, itemMC: Array[String], itemMMC: Array[String],
                      normScore: Int, maxScore: Int, itemMisconception: Array[String], timeSpent: Double);

class ProficiencyInputMapper extends IBatchModel[MeasuredEvent] {
    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val assessments = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
                var assessmentBuff = Buffer[Assessment]();
                x.foreach { x =>
                    val learner_id = x.uid.get
                    val itemResponses = x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("itemResponses").get.asInstanceOf[Buffer[Map[String, AnyRef]]]
                    itemResponses.foreach { f =>
                        val itemId = f.get("itemId").get.asInstanceOf[String];
                        val itemMC = f.get("mc").get.asInstanceOf[Array[String]]
                        
                        if(itemMC.isEmpty&&itemMC.length==0){
                            //fetch from content
                        }
                        val itemMCC = f.get("mcc").get.asInstanceOf[Array[String]]
                        val score = f.get("score").get.asInstanceOf[Int]
                        val maxScore = f.get("maxScore").get.asInstanceOf[Int]
                        if(maxScore==0){
                            //fetch from content 
                        }
                        val timeSpent = f.get("timeSpent").get.asInstanceOf[Double]
                        val itemMisconception = Array[String]();
                        val normScore = (score/maxScore)
                        val assess = Assessment(learner_id,itemId,itemMC,itemMCC,normScore,maxScore,itemMisconception,timeSpent)
                        val knwState = LearnerAdapter.LearnerProficiency.getById(learner_id);
                        
                        assessmentBuff += assess
                    }
                }
                assessmentBuff;
            }.map(f => f._2).flatMap { x => x.map { x => x } }
            
        return null;
    }
}