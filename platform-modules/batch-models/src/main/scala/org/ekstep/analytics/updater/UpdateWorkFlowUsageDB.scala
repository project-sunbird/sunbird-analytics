package org.ekstep.analytics.updater

import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.WorkFlowUsageSummaryFact
import org.ekstep.analytics.util.WorkFlowSummaryIndex
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._

object UpdateWorkFlowUsageDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, WorkFlowUsageSummaryFact, WorkFlowSummaryIndex] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateWorkFlowUsageDB"
    override def name: String = "UpdateWorkFlowUsageDB"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_WORKFLOW_USAGE_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[WorkFlowUsageSummaryFact] = {
        null
    }

    override def postProcess(data: RDD[WorkFlowUsageSummaryFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[WorkFlowSummaryIndex] = {
        // Update the database
        data.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT)
        data.map { x => WorkFlowSummaryIndex(x.d_period, x.d_channel, x.d_app_id, x.d_tag, x.d_type, x.d_device_id, x.d_content_id, x.d_user_id) };
    }
}