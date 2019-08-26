package org.ekstep.analytics.model

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.exception.{DispatcherException, DruidConfigException}
import org.sunbird.cloud.storage.conf.AppConf


case class DruidOutput(date: Option[String], state: Option[String], producer_id: Option[String], total_scans: Option[Integer] = Option(0), total_sessions: Option[Integer] = Option(0),
                       total_ts: Option[Double] = Option(0.0)) extends Input with AlgoInput with AlgoOutput with Output

case class ReportConfig(id: String, queryType: String, dateRange: QueryDateRange, metrics: List[Metrics], labels: Map[String, String], output: List[OutputConfig])
case class QueryDateRange(interval: Option[QueryInterval], staticInterval: Option[String], granularity: Option[String])
case class QueryInterval(startDate: String, endDate: String)
case class Metrics(metric: String, label: String, druidQuery: DruidQueryModel)
case class OutputConfig(`type`: String, label: String, metrics: List[String], dims: List[String], fileParameters: List[String] = List("id", "dims"))

object DruidQueryProcessingModel  extends IBatchModelTemplate[DruidOutput, DruidOutput, DruidOutput, DruidOutput] with Serializable {

    implicit val className = "org.ekstep.analytics.model.DruidQueryProcessingModel"
    override def name: String = "DruidQueryProcessingModel"

    override def preProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {
        data
    }

    override def algorithm(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {
        val strConfig = config.get("reportConfig").get.asInstanceOf[Map[String, AnyRef]]
        val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(strConfig))

        val queryDims = reportConfig.metrics.map{f =>
            f.druidQuery.dimensions.getOrElse(List()).map(f => f._2)
        }.distinct

        if(queryDims.length > 1) throw new DruidConfigException("Query dimensions are not matching")

        val interval = strConfig.get("dateRange").get.asInstanceOf[Map[String, AnyRef]]
        val granularity = interval.get("granularity")
        val queryInterval = if(interval.get("staticInterval").nonEmpty) {
            interval.get("staticInterval").get.asInstanceOf[String]
        }
        else if(interval.get("interval").nonEmpty) {
            val dateRange = interval.get("interval").get.asInstanceOf[Map[String, String]]
            dateRange.get("startDate").get + "/" + dateRange.get("endDate").get
        }
        else throw new DruidConfigException("Both staticInterval and interval cannot be missing. Either of them should be specified")

        val metrics = reportConfig.metrics.map{f =>
            val queryConfig = if(granularity.nonEmpty)
                JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervals" -> queryInterval, "granularity" -> granularity.get)
            else
                JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervals" -> queryInterval)

            val data = DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig)))
            data.map{ x =>
                val dataMap = JSONUtils.deserialize[Map[String, AnyRef]](x)
                val key = dataMap.filter(m => (queryDims.flatMap(f => f) ++ List("date")).contains(m._1)).values.map(f => f.toString).toList.sorted(Ordering.String.reverse).mkString(",")
                (key, dataMap)
            }
        }.flatMap(f => f)
        val finalResult = sc.parallelize(metrics).foldByKey(Map())(_ ++ _)
        finalResult.map{f => JSONUtils.deserialize[DruidOutput](JSONUtils.serialize(f._2))}
    }

    override def postProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {

        val configMap = config.get("reportConfig").get.asInstanceOf[Map[String, AnyRef]]
        val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
        val dimFields = reportConfig.metrics.map(m => m.druidQuery.dimensions.get.map(f => f._2)).flatMap(f => f)
        val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
        implicit val sqlContext = new SQLContext(sc);
        import sqlContext.implicits._

        // Change map to foreach as parallel execution might conflict with local file path
        val key = config.getOrElse("key", null).asInstanceOf[String];
        reportConfig.output.foreach{ f =>
            if("csv".equalsIgnoreCase(f.`type`)) {
                val df = data.toDF().na.fill(0L)
                val metricFields = f.metrics
                val fieldsList = (dimFields ++ metricFields ++ List("date")).distinct
                val dimsLabels = labelsLookup.filter(x => f.dims.contains(x._1)).values.toList
                val filteredDf = df.select(fieldsList.head, fieldsList.tail:_*)
                val renamedDf =  filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*)
                val reportFinalId = if(f.label.nonEmpty) reportConfig.id + "/" + f.label else reportConfig.id
                //renamedDf.show(150)
                val dirPath = writeToCSVAndRename(renamedDf, config ++ Map("dims" -> dimsLabels, "reportId" -> reportFinalId, "fileParameters" -> f.fileParameters))
                AzureDispatcher.dispatchDirectory(config ++ Map("dirPath" -> dirPath, "key" -> (key + reportFinalId + "/")))
            }
            else {
                val strData = data.map(f => JSONUtils.serialize(f))
                AzureDispatcher.dispatch(strData.collect(), config)
            }
        }
        data
    }

    def writeToCSVAndRename(data: DataFrame, config: Map[String, AnyRef])(implicit sc: SparkContext): String = {
        val filePath = config.getOrElse("filePath", AppConf.getConfig("spark_output_temp_dir")).asInstanceOf[String];
        val key = config.getOrElse("key", null).asInstanceOf[String];
        val reportId = config.getOrElse("reportId", "").asInstanceOf[String];
        val fileParameters = config.getOrElse("fileParameters", List("")).asInstanceOf[List[String]];
        var dims = config.getOrElse("dims", List()).asInstanceOf[List[String]];

        dims = if (fileParameters.nonEmpty && fileParameters.contains("date")) dims ++ List("Date") else dims
        val finalPath = filePath + key.split("/").last
        if(dims.nonEmpty) {
            val duplicateDims = dims.map(f => f.concat("Duplicate"))
            var duplicateDimsDf = data
            dims.foreach{f =>
                duplicateDimsDf = duplicateDimsDf.withColumn(f.concat("Duplicate"), col(f))
            }
            duplicateDimsDf.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").partitionBy(duplicateDims: _*).mode("overwrite").save(finalPath)
        }
        else
            data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(finalPath)
        val renameDir = finalPath+"/renamed"
        renameHadoopFiles(finalPath, renameDir, reportId, dims)
    }

    def renameHadoopFiles(tempDir: String, outDir: String, id: String, dims: List[String])(implicit sc: SparkContext): String = {

        val fs = FileSystem.get(sc.hadoopConfiguration)
        val fileList = fs.listFiles(new Path(s"$tempDir/"), true)
        while(fileList.hasNext){
            val filePath = fileList.next().getPath.toString
            if(!(filePath.contains("_SUCCESS"))) {
                val breakUps = filePath.split("/").filter(f => f.contains("="))
                val dimsKeys = breakUps.filter { f =>
                    val bool = dims.map(x => f.contains(x))
                    if (bool.contains(true)) true
                    else false
                }
                val finalKeys = dimsKeys.map { f =>
                    f.split("=").last
                }
                val key = if (finalKeys.length > 1) finalKeys.mkString("/") else finalKeys.head
                val crcKey = if (finalKeys.length > 1) {
                    val builder = new StringBuilder
                    val keyStr = finalKeys.mkString("/")
                    val replaceStr = "/."
                    builder.append(keyStr.substring(0, keyStr.lastIndexOf("/")))
                    builder.append(replaceStr)
                    builder.append(keyStr.substring(keyStr.lastIndexOf("/") + replaceStr.length - 1))
                    builder
                } else
                    "."+finalKeys.head
                fs.rename(new Path(filePath), new Path(s"$outDir/$id/$key.csv"))
                fs.delete(new Path(s"$outDir/$id/$crcKey.csv.crc"), false)
            }
        }
        outDir + "/" + id
    }
}
