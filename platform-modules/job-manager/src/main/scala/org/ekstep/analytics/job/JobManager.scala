package org.ekstep.analytics.job

import org.ekstep.analytics.framework.util.JSONUtils
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ArrayBlockingQueue
import org.ekstep.analytics.kafka.consumer.JobConsumer
import org.ekstep.analytics.kafka.consumer.JobConsumerConfig
import java.util.concurrent.Executors
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import java.util.concurrent.CountDownLatch
import org.ekstep.analytics.framework.util.EventBusUtil
import com.google.common.eventbus.Subscribe
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.S3Util
import org.apache.commons.lang3.StringUtils

case class JobManagerConfig(jobsCount: Int, topic: String, bootStrapServer: String, consumerGroup: String, slackChannel: String, slackUserName: String, tempBucket: String, tempFolder: String, runMode: String = "shutdown");

object JobManager extends optional.Application {

    implicit val className = "org.ekstep.analytics.job.JobManager";
    
    var jobsCompletedCount = 0;

    def main(config: String) {
        JobLogger.init("JobManager");
        val jobManagerConfig = JSONUtils.deserialize[JobManagerConfig](config);
        JobLogger.log("Starting job manager", Option(Map("config" -> jobManagerConfig)), INFO);
        init(jobManagerConfig);
    }

    def init(config: JobManagerConfig) = {
        S3Util.deleteFolder(config.tempBucket, config.tempFolder);
        val jobQueue: BlockingQueue[String] = new ArrayBlockingQueue[String](config.jobsCount);
        val consumer = initializeConsumer(config, jobQueue);
        JobLogger.log("Initialized the job consumer", None, INFO);
        val executor = Executors.newFixedThreadPool(1);
        JobLogger.log("Total job count: " + config.jobsCount, None, INFO);
        val doneSignal = new CountDownLatch(config.jobsCount);
        JobMonitor.init(config);
        JobLogger.log("Initialized the job event listener. Starting the job executor", None, INFO);
        executor.submit(new JobRunner(config, jobQueue, doneSignal));

        doneSignal.await();
        JobLogger.log("Job manager execution completed. Shutting down the executor and consumer", None, INFO);
        executor.shutdown();
        JobLogger.log("Job manager executor shutdown completed", None, INFO);
        consumer.shutdown();
        JobLogger.log("Job manager consumer shutdown completed", None, INFO);
    }

    private def initializeConsumer(config: JobManagerConfig, jobQueue: BlockingQueue[String]): JobConsumer = {
        val props = JobConsumerConfig.makeProps(config.bootStrapServer, config.consumerGroup)
        val consumer = new JobConsumer(config.topic, props, jobQueue);
        consumer.start();
        consumer;
    }
}

class JobRunner(config: JobManagerConfig, jobQueue: BlockingQueue[String], doneSignal: CountDownLatch) extends Runnable {

    implicit val className: String = "JobRunner";

    override def run {
        while (doneSignal.getCount() !=0 ) {
            val record = jobQueue.take();
            executeJob(record);
            doneSignal.countDown();
        }
    }

    private def executeJob(record: String) {
        val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](record);
        val modelName = jobConfig.get("model").get.toString()
        try {
            if(StringUtils.equals("data-exhaust",modelName)){
                Thread.sleep(1000*60*30) // Sleep for 30 mins
            }
            JobLogger.log("Executing " + modelName, None, INFO);
            JobExecutor.main(modelName, JSONUtils.serialize(jobConfig.get("config").get))
            JobLogger.log("Finished executing " + modelName, None, INFO);
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
        }
    }
}

object TestJobManager {
    def main(args: Array[String]): Unit = {
        val config = """{"jobsCount":2,"topic":"local.analytics.job_queue","bootStrapServer":"localhost:9092","consumerGroup":"jobmanager","slackChannel":"#test_channel","slackUserName":"JobManager","tempBucket":"ekstep-dev-data-store","tempFolder":"transient-data"}""";
        JobManager.main(config);
    }
}