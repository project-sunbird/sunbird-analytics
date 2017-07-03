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

case class JobManagerConfig(jobsCount: Int, topic: String, bootStrapServer: String, consumerGroup: String);

object JobManager extends optional.Application {

    var jobsCompletedCount = 0;

    def main(config: String) {
        val jobManagerConfig = JSONUtils.deserialize[JobManagerConfig](config);
        init(jobManagerConfig);
    }

    def init(config: JobManagerConfig) = {
        val jobQueue: BlockingQueue[String] = new ArrayBlockingQueue[String](config.jobsCount);
        val consumer = initializeConsumer(config, jobQueue);
        val executor = Executors.newFixedThreadPool(1);
        val doneSignal = new CountDownLatch(config.jobsCount);
        executor.submit(new JobRunner(config, jobQueue, doneSignal));

        doneSignal.await();
        executor.shutdown();
        consumer.shutdown();
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
        while (doneSignal.getCount() < config.jobsCount) {
            val record = jobQueue.take();
            executeJob(record);
            doneSignal.countDown();
        }
    }

    private def executeJob(record: String) {
        val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](record);
        val modelName = jobConfig.get("model").get.toString()
        JobLogger.log(modelName.toUpperCase() + " job started.", None, INFO)
        JobExecutor.main(modelName, JSONUtils.serialize(jobConfig.get("config").get))
        JobLogger.log(modelName.toUpperCase() + " job finished.", None, INFO)
        println(modelName.toUpperCase() + " job finished.")
    }
}

object TestJobManager {
    def main(args: Array[String]): Unit = {
        val config = """{"jobsCount":2,"topic":"dev.jobs.queue","bootStrapServer":"localhost:9092","consumerGroup":"jobmanager"}""";
        JobManager.main(config);
    }
}