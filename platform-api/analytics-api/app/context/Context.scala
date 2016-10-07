package context

import play.api._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.ekstep.analytics.api.service.RecommendationAPIService
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.api.util.ContentCacheUtil

object Context {

    var sc: SparkContext = null;

    def setSparkContext() = {
        Logger.info("Starting spark context")
        val conf = new SparkConf().setAppName("AnalyticsAPIEngine");
        val master = conf.getOption("spark.master");
        // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered as they depend on environment variables
        if (master.isEmpty) {
            Logger.info("Master not found. Setting it to local[*]")
            conf.setMaster("local[*]");
        }
        if (!conf.contains("spark.cassandra.connection.host")) {
            conf.set("spark.cassandra.connection.host", play.Play.application.configuration.getString("spark.cassandra.connection.host"))
        }
        // $COVERAGE-ON$
        sc = new SparkContext(conf);
        setS3Conf(sc);
        Logger.info("Spark context started")
    }
    
    def setS3Conf(sc: SparkContext) = {
        Logger.info("Configuring S3 AccessKey& SecrateKey to SparkContext")
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getAwsKey());
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getAwsSecret());
    }

    def closeSparkContext() = {
        Logger.info("Closing Spark Context")
        sc.stop();
    }

    def checkSparkContext() {
        try {
            val nums = Array(10, 5, 18, 4, 8, 56)
            val rdd = sc.parallelize(nums)
            rdd.sortBy(f => f).collect
        } catch {
        	// $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered
            case ex: SparkException =>
                Context.resetSparkContext();
            case ex: Exception =>
                ex.printStackTrace();
                Logger.info("Spark context is down...");
        	// $COVERAGE-ON$            
        }
    }
    
    def resetSparkContext() {
    	// $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered
        closeSparkContext();
        setSparkContext();
        ContentCacheUtil.initCache()(Context.sc, play.Play.application.configuration.underlying());
        // $COVERAGE-ON$
    }

}