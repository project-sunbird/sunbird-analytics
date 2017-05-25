/**
 * @author Jitendra Singh Sankhwar
 */
package org.ekstep.analytics.job.updater

import org.apache.spark.SparkContext
import optional.Application
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.updater.UpdateContentEditorUsageDB

object ContentEditorUsageUpdater extends Application with IJob {
    
    implicit val className = "org.ekstep.analytics.job.updater.ContentEditorUsageUpdater"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, UpdateContentEditorUsageDB);
     }
  
}