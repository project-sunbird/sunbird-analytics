package org.ekstep.analytics.framework.driver

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JobLogger}

/**
 * @author Santhosh
 */
object BatchJobDriver {

    implicit val className = "org.ekstep.analytics.framework.driver.BatchJobDriver"
    def process[T, R](config: JobConfig, model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        process(config, List(model));
    }

    def process[T, R](config: JobConfig, models: List[IBatchModel[T, R]])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {
        JobContext.parallelization = CommonUtil.getParallelization(config);
        if (null == sc) {
            val sparkCassandraConnectionHost = config.modelParams.getOrElse(Map()).get("sparkCassandraConnectionHost")
            implicit val sc = CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost);
            try {
                _process(config, models);
            } finally {
                CommonUtil.closeSparkContext();
                /*
                 * Clearing previous job persisting rdd, in case of the job got failed
                 * */
                if (JobContext.rddList.nonEmpty)
                    JobContext.rddList.clear()
            }
        } else {
            _process(config, models);
        }
    }

    private def _process[T, R](config: JobConfig, models: List[IBatchModel[T, R]])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext) {

        val rdd = DataFetcher.fetchBatchData[T](config.search).cache();
        val count = rdd.count;
        _setDeviceMapping(config, rdd);
        val data = DataFilter.filterAndSort[T](rdd, config.filters, config.sort);
        models.foreach { model =>
            JobContext.jobName = model.name
            // TODO: It is not necessary that the end date always exists. The below log statement might throw exceptions
            val endDate = config.search.queries.getOrElse(Array(Query())).last.endDate
            JobLogger.start("Started processing of " + model.name, Option(Map("config" -> config, "model" -> model.name, "date" -> endDate)));
            try {
                val result = _processModel(config, data, model);
                JobLogger.end(model.name + " processing complete", "SUCCESS", Option(Map("model" -> model.name, "date" -> endDate, "inputEvents" -> count, "outputEvents" -> result._2, "timeTaken" -> Double.box(result._1 / 1000))));
            } catch {
                case ex: Exception =>
                    JobLogger.log(ex.getMessage, None, ERROR);
                    JobLogger.end(model.name + " processing failed", "FAILED", Option(Map("model" -> model.name, "date" -> endDate, "inputEvents" -> count, "statusMsg" -> ex.getMessage)));
                    ex.printStackTrace();
            } finally {
                rdd.unpersist()
            }
        }
    }

    private def _setDeviceMapping[T](config: JobConfig, data: RDD[T])(implicit mf: Manifest[T]) {
        if (config.deviceMapping.nonEmpty && config.deviceMapping.get) {
            JobContext.deviceMapping = mf.toString() match {
                case "org.ekstep.analytics.framework.Event" =>
                    data.map(x => x.asInstanceOf[Event]).filter { x => ("GE_GENIE_START".equals(x.eid) || "GE_START".equals(x.eid)) }.map { x => (x.did, if (x.edata != null) x.edata.eks.loc else "") }.collect().toMap;
                case _ => Map()
            }
        }
    }

    private def _processModel[T, R](config: JobConfig, data: RDD[T], model: IBatchModel[T, R])(implicit mf: Manifest[T], mfr: Manifest[R], sc: SparkContext): (Long, Long) = {

        CommonUtil.time({
            val output = model.execute(data, config.modelParams);
            // JobContext.recordRDD(output);
            val count = OutputDispatcher.dispatch(config.output, output);
            // JobContext.cleanUpRDDs();
            count;
        })

    }
}