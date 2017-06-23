package org.ekstep.analytics.job.consolidated

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.metrics.job._

object MetricsEventCreationJob extends optional.Application with IJob {
  
    val className = "org.ekstep.analytics.job.MetricsEventCreationJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        val models = List(AppUsageMetricCreationModel, AssetSnapshotMetricCreationModel, CEUsageMetricCreationModel, ConceptSnapshotMetricCreationModel, ContentSnapshotMetricCreationModel, ContentUsageMetricCreationModel, GenieUsageMetricCreationModel, ItemUsageMetricCreationModel, TextbookSnapshotMetricCreationModel, TextbookUsageMetricCreationModel, AuthorUsageMetricCreationModel)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, models, className);
    }
}