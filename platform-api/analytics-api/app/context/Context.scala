package context

import play.api._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Context {
    
    var sc: SparkContext = null;
    
    def setSparkContext() = {
        Logger.info("Starting spark context")
        val conf = new SparkConf().setAppName("AnalyticsAPIEngine");
        val master = conf.getOption("spark.master");
        if (master.isEmpty) {
            Logger.info("Master not found. Setting it to local[*]")
            conf.setMaster("local[*]");
        }
        if (!conf.contains("spark.cassandra.connection.host")) {
            conf.set("spark.cassandra.connection.host", play.Play.application.configuration.getString("spark.cassandra.connection.host"))
        }
        sc = new SparkContext(conf);
        Logger.info("Spark context started")
    }
    
    def closeSparkContext() = {
        Logger.info("Closing Spark Context")
        sc.stop();
    }
}