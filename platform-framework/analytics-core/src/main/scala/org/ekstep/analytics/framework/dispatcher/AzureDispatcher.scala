package org.ekstep.analytics.framework.dispatcher

import java.io.FileWriter
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.{CommonUtil, JobLogger}
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.{FileSystem, Path}
import org.ekstep.analytics.framework.Level
import scala.concurrent.Await
import scala.util.{Failure, Success}

object AzureDispatcher extends IDispatcher {

    implicit val className = "org.ekstep.analytics.framework.dispatcher.AzureDispatcher"

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]): Array[String] = {
        var filePath = config.getOrElse("filePath", null).asInstanceOf[String];
        val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
        val key = config.getOrElse("key", null).asInstanceOf[String];
        val zip = config.getOrElse("zip", false).asInstanceOf[Boolean];
        val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];

        if (null == bucket || null == key) {
            throw new DispatcherException("'bucket' & 'key' parameters are required to send output to azure")
        }
        var deleteFile = false;
        if (null == filePath) {
            filePath = AppConf.getConfig("spark_output_temp_dir") + "output-" + System.currentTimeMillis() + ".log";
            val fw = new FileWriter(filePath, true);
            events.foreach { x => { fw.write(x + "\n"); } };
            fw.close();
            deleteFile = true;
        }
        val finalPath = if (zip) CommonUtil.gzip(filePath) else filePath;
        StorageServiceFactory.getStorageService(StorageConfig("azure", AppConf.getStorageKey("azure"), AppConf.getStorageSecret("azure"))).upload(bucket, finalPath, key, Option(isPublic));
        if (deleteFile) CommonUtil.deleteFile(filePath);
        if (zip) CommonUtil.deleteFile(finalPath);
        events;
    }

    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext) = {

//        dispatch(events.collect(), config);
        val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
        val key = config.getOrElse("key", null).asInstanceOf[String];
        val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];

        if (null == bucket || null == key) {
            throw new DispatcherException("'bucket' & 'key' parameters are required to send output to azure")
        }
        events.saveAsTextFile("wasb://" + bucket + "@" + AppConf.getStorageKey(AppConf.getStorageType()) + ".blob.core.windows.net/" + key);
    }

    def dispatch(config: Map[String, AnyRef], data: DataFrame)(implicit sc: SparkContext) = {
        val filePath = config.getOrElse("filePath", AppConf.getConfig("spark_output_temp_dir")).asInstanceOf[String];
        val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
        val key = config.getOrElse("key", null).asInstanceOf[String];
        val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];
        val reportId = config.getOrElse("reportId", "").asInstanceOf[String];
        val filePattern = config.getOrElse("filePattern", "").asInstanceOf[String];
        var dims = config.getOrElse("dims", List()).asInstanceOf[List[String]];

        if (null == bucket || null == key) {
            throw new DispatcherException("'bucket' & 'key' parameters are required to send output to azure")
        }
        dims = if (filePattern.nonEmpty && filePattern.contains("date")) dims ++ List("Date") else dims
        val finalPath = filePath + key.split("/").last
        if(dims.nonEmpty)
            data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").partitionBy(dims:_*).mode("overwrite").save(finalPath)
        else
            data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(finalPath)
        val renameDir = finalPath+"/renamed"
        val renamedPath = renameHadoopFiles(finalPath, renameDir, filePattern, reportId, dims)
        val finalKey = key + reportId + "/"
        val uploadMsg = StorageServiceFactory.getStorageService(StorageConfig("azure", AppConf.getStorageKey("azure"), AppConf.getStorageSecret("azure")))
          .uploadFolder(bucket, renamedPath, finalKey, Option(isPublic));
        val result = Await.result(uploadMsg, scala.concurrent.duration.Duration.apply(1, "minute"))
        JobLogger.log("Successfully Uploaded files", Option(Map("filesUploaded" -> result)), Level.INFO)
//        uploadMsg.onComplete {
//            case Success(files) => println("Successfully Uploaded files: " , files);JobLogger.log("Successfully Uploaded files", None, Level.INFO)
//            case Failure(ex) => throw ex
//        }
        CommonUtil.deleteDirectory(finalPath);
    }

    def renameHadoopFiles(tempDir: String, outDir: String, filePattern: String, id: String, dims: List[String])(implicit sc: SparkContext): String = {

        // TO-DO
        // Use filePattern for renaming files
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
