import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructField, _}
import com.datastax.spark.connector._
import redis.clients.jedis.{JedisPool, JedisPoolConfig}
import org.sunbird.cloud.storage.conf.AppConf
import org.apache.spark.sql.functions.{lower,col}

case class districts(subdivision_1_custom_name : String , subdivision_2_custom_name : String)
case class DeviceProfileDistrict(device_id : String, user_declared_state : String ,user_declared_district : String)

object DistrictMappingMisMatch extends optional.Application {

  def main(cassandraHost : String , redisHost : String, env : String) : Unit = {
    implicit val fc = new FrameworkContext();
    implicit val spark = getSparkSession(cassandraHost,redisHost)

try {
  val mappingDf = spark.read.format("csv").option("header", "true")
    .load("wasbs://public@" + AppConf.getStorageKey("azure") + ".blob.core.windows.net/maxmind_custom_data_mapping.csv")
  val masterData = mappingDf.select("subdivision_1_custom_name", "subdivision_2_custom_name")
    .filter(col("subdivision_1_custom_name").isNotNull && col("subdivision_2_custom_name").isNotNull).distinct()

  val deviceDF = getMisMatchDiDs(masterData)
  updateRedis(deviceDF, redisHost)
  updateCassandra(deviceDF,env)
}finally {
  spark.stop()
}
  }

  def getMisMatchDiDs(masterDf : DataFrame)(implicit spark: SparkSession): DataFrame =
  {
   val schema = StructType(Array(
     StructField("_id", StringType),
     StructField("user_declared_state", StringType),
     StructField("user_declared_district", StringType)))

    val df = spark.read
      .format("org.apache.spark.sql.redis")
      .option("keys.pattern", "*")
      .schema(schema)
      .load()

    val filterDf = df.join(masterDf, lower(df("user_declared_state")) <=> lower(masterDf("subdivision_1_custom_name"))
      && lower(df("user_declared_district")) <=> lower(masterDf("subdivision_2_custom_name")),"left").filter(masterDf("subdivision_1_custom_name").isNull)

    Console.println("mismatch count"  + filterDf.count)

    filterDf
  }

  def updateRedis(deviceDf:DataFrame, redisHost : String): Unit =
  {
    def buildPoolConfig = {
      val poolConfig = new JedisPoolConfig
      poolConfig.setMaxTotal(2)
      poolConfig.setMaxIdle(2)
      poolConfig.setMinIdle(1)
      poolConfig
    }

    deviceDf.foreachPartition(partition => {
      val jedisPool = new JedisPool(buildPoolConfig, "localhost")
      val conn = jedisPool.getResource
      conn.select(2)
      partition.foreach(f => {
        conn.hdel(f.getString(0), "user_declared_state","user_declared_district")
      })
      conn.close()
      jedisPool.close()
    })

  }

  def updateCassandra(deviceDf:DataFrame, env: String)(implicit spark: SparkSession) : Unit =
  {
   import spark.implicits._
    val filteredRows = spark.sparkContext.cassandraTable(env+"_device_db", "device_profile")
      .select("device_id")
      .where("device_id IN ?", deviceDf.select(deviceDf("_id")).as[String].collectAsList())
      .as((device_id: String)
      =>(device_id))
   Console.println("cassandra rows ",filteredRows.count)
    filteredRows.map(f => (f,null,null)).saveToCassandra(env+"_device_db","device_profile",SomeColumns("device_id", "user_declared_state", "user_declared_district"))
  }

  def getSparkSession(cassandraHost: String, redisHost: String): SparkSession =
  {
    val conf = new SparkConf()
      .setAppName("DistrictMappingMisMatch")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.redis.host", redisHost)
      .set("spark.redis.port", "6379")
      .set("spark.redis.db", "2")

    val spark = SparkSession.builder().appName("DistrictMappingMisMatch").config(conf).getOrCreate()
    implicit val sc = spark.sparkContext

    CommonUtil.setAzureConf(sc)
  spark
  }
}


object TestDistrictMappingMisMatch {

  def main(args: Array[String]): Unit = {
    DistrictMappingMisMatch.main("localhost","localhost","local");
  }

}
