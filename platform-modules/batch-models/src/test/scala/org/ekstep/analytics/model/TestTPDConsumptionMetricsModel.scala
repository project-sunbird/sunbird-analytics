package org.ekstep.analytics.model

import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.job.Metrics.TPDConsumptionMetricsJob
import org.scalamock.scalatest.MockFactory

class TestTPDConsumptionMetricsModel extends SparkSpec(null) with MockFactory{

  implicit val fc = new FrameworkContext()

  "TPDConsumptionMetricsModel" should "execute the method and won't throw any exeception" in  {
      val jobConfig = """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.TPDConsumptionMetricsModel","modelParams":{},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"ETB Creation Metrics Model","deviceMapping":false}"""
    TPDConsumptionMetricsJob.main(jobConfig)(Option(sc))
  }

  it should "execute the method for druid Config and won't throw any error" in {
    val config = """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.TPDConsumptionMetricsModel","modelParams":{"druidQuery":{"queryType":"groupBy","dataSource":"summary-events","intervals":"2019-09-08/2019-09-09","aggregations":[{"name":"sum__edata_time_spent","type":"doubleSum","fieldName":"edata_time_spent"}],"dimensions":[{"fieldName":"object_rollup_l1","aliasName":"courseId"},{"fieldName":"uid","aliasName":"userId"},{"fieldName":"context_cdata_id","aliasName":"batchId"}],"filters":[{"type":"equals","dimension":"eid","value":"ME_WORKFLOW_SUMMARY"},{"type":"in","dimension":"dimensions_pdata_id","values":["dev.sunbird.app","dev.sunbird.portal"]},{"type":"equals","dimension":"dimensions_type","value":"content"},{"type":"equals","dimension":"dimensions_mode","value":"play"},{"type":"equals","dimension":"context_cdata_type","value":"batch"}],"postAggregation":[{"type":"arithmetic","name":"timespent","fields":{"leftField":"sum__edata_time_spent","rightField":60,"rightFieldType":"constant"},"fn":"/"}],"descending":"false"}},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"TPD Course Consumption Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    TPDConsumptionMetricsModel.execute(sc.emptyRDD, jobConfig.modelParams)
  }
}
