import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructField, _}
import com.datastax.spark.connector._
import org.sunbird.cloud.storage.conf.AppConf
import org.apache.spark.sql.functions.{col, lower}
import org.ekstep.analytics.framework.util.HTTPClient
import redis.clients.jedis.JedisPool


case class LocationList(count: Int, response: List[Map[String, String]])

case class LocationResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: LocationList)

object DistrictMappingMisMatch extends optional.Application {

    def main(cassandraHost: String, redisHost: String, env: String, locationIp : String): Unit = {
        implicit val fc = new FrameworkContext();
        implicit val spark = getSparkSession(cassandraHost, redisHost)
        val execTime = CommonUtil.time({
            try {

                val masterData = getLocationData(locationIp)
                val deviceDF = getMisMatchDiDs(masterData)
                updateRedis(deviceDF, redisHost)
                updateCassandra(deviceDF,env)
                saveDiDsToFile(deviceDF)
            } finally {
                spark.stop()
            }
        })
        Console.println("time take to execute " + execTime)
    }

    def getMisMatchDiDs(masterDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
        val schema = StructType(Array(
            StructField("_id", StringType),
            StructField("user_declared_state", StringType),
            StructField("user_declared_district", StringType)))

        val df = spark.read
          .format("org.apache.spark.sql.redis")
          .option("keys.pattern", "*")
          .schema(schema)
          .load()

        val filterDf = df.join(masterDf, lower(df("user_declared_state")) <=> lower(masterDf("_1"))
          && lower(df("user_declared_district")) <=> lower(masterDf("_2")), "left").filter(masterDf("_1").isNull && df("user_declared_state").isNotNull)

        Console.println("mismatch count" + filterDf.count)

        filterDf
    }

    def updateRedis(deviceDf: DataFrame, redisHost: String): Unit = {
        val redisConn = new JedisPool(redisHost).getResource
        redisConn.select(2)
        deviceDf.foreach(f => {
            redisConn.hdel(f.getString(0), "user_declared_state", "user_declared_district")
        })
        redisConn.close()
    }

    def updateCassandra(deviceDf: DataFrame, env: String)(implicit spark: SparkSession): Unit = {
        import spark.implicits._
        val filteredRows = spark.sparkContext.cassandraTable(env + "_device_db", "device_profile")
          .select("device_id")
          .where("device  _id IN ?", deviceDf.select(deviceDf("_id")).as[String].collectAsList())
          .as((device_id: String)
          => (device_id))
        Console.println("cassandra rows ", filteredRows.count)
        filteredRows.map(f => (f, null, null)).saveToCassandra(env + "_device_db", "device_profile", SomeColumns("device_id", "user_declared_state", "user_declared_district"))
    }

    def getLocationData(locationIp : String)(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        val locationurl = "http://"+ locationIp +":9000/v1/location/search"
        val request =
            s"""
               |{
               |  "request": {
               |    "filters": {},
               |    "limit": 10000
               |  }
               |}
               """.stripMargin
        val response = RestUtil.post[LocationResponse](locationurl, request)

        val states = response.result.response.map(f => {
            if (f.getOrElse("type", "").equalsIgnoreCase("state"))
                (f.get("id").get, f.get("name").get)
            else
                ("", "")
        }).filter(f => !f._1.isEmpty)
        val districts = response.result.response.map(f => {
            if (f.getOrElse("type", "").equalsIgnoreCase("district"))
                (f.get("parentId").get, f.get("name").get)
            else
                ("", "")
        }).filter(f => !f._1.isEmpty)
        val masterData = states.map(tup1 => districts.filter(tup2 => tup2._1 == tup1._1).map(tup2 => (tup1._2, tup2._2))).flatten.distinct
        println("master size" + masterData.size)
        spark.sparkContext.parallelize(masterData).toDF()
    }

    def getSparkSession(cassandraHost: String, redisHost: String): SparkSession = {
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

    def saveDiDsToFile(deviceDF: DataFrame): Unit = {
        deviceDF.drop("_1").drop("_2").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("user_locations.csv")
    }

}


object TestDistrictMappingMisMatch {

    def main(args: Array[String]): Unit = {
        DistrictMappingMisMatch.main("localhost", "localhost", "local", "localhost");
    }

}
