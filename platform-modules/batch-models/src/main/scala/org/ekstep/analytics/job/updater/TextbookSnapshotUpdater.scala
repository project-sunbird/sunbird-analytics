package org.ekstep.analytics.job.updater

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.UpdateTextbookSnapshotDB

/**
 * @author mahesh
 */

object TextbookSnapshotUpdater {

	def main(config: String)(implicit sc: Option[SparkContext] = None) {
		implicit val sparkContext: SparkContext = sc.getOrElse(null);
		JobDriver.run("batch", config, UpdateTextbookSnapshotDB);
	}

}