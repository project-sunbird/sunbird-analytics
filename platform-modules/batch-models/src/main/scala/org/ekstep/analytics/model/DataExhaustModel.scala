package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.joda.time.DateTime

case class JobSummary(client_id: String, request_id: String, job_id: String, status: String, request_data: String, config: String, 
                      locations: List[String], dt_file_created: DateTime, dt_first_event: DateTime, dt_last_event: DateTime, 
                      dt_expiration: DateTime, iteration: Int, dt_job_submitted: DateTime, dt_job_processing: DateTime, 
                      dt_job_completed: DateTime, input_events: Int, output_events: Int, file_size: Long, latency: Int, 
                      executionTime: Long, err_message: String) extends AlgoOutput

object DataExhaustModel extends IBatchModelTemplate[Event, Event, JobSummary, Empty] with Serializable {
  
    val className = "org.ekstep.analytics.model.DataExhaustModel"
    override def name: String = "DataExhaustModel"
    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Event] = {

        null
    }
    
    override def algorithm(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[JobSummary] = {
    
        null
    }
    
    override def postProcess(data: RDD[JobSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
    
        null
    }
}