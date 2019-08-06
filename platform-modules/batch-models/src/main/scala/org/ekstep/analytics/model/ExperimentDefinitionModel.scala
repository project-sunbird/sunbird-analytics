package org.ekstep.analytics.model

import java.text.SimpleDateFormat
import java.util.Date

import com.datastax.spark.connector._
import com.datastax.spark.connector.types.TimestampFormatter
import org.apache.http.HttpHeaders
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Level.ERROR
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{IBatchModelTemplate, _}

import scala.collection.mutable.Buffer


case class ExperimentMappingOutput(userId: Option[String] = None, deviceId: Option[String] = None, key: String, url: Option[String] = None, id: String
                                   , name: String, platform: String, userIdMod: Int = 0, deviceIdMod: Int = 0, expType: String, startDate: String,
                                   endDate: String, lastUpdatedOn: String) extends AlgoOutput with Output

case class UserList(count: Int, content: List[Map[String, AnyRef]])

case class UserResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Map[String, UserList]);

case class CriteriaModel(`type`: String, filters: AnyRef, modulus: Option[Int])

case class ExperimentDefinition(expid: String, expname: String, criteria: String, expdata: String, status: String) extends AlgoInput with Input

case class ExperimentData(startDate: String, endDate: String, key: String, client: String, modulus: Option[Int])


case class ExperiementDefinitionOutput(expid: String, status: String, status_message: String, updatedon: String, stats: Map[String, Long], updatedby: String)


object ExperimentDefinitionModel extends IBatchModelTemplate[Empty, ExperimentDefinition, ExperimentMappingOutput, ExperimentMappingOutput] with Serializable {

    implicit val className = "org.ekstep.analytics.model.ExperimentDefinitionModel"

    override def name: String = "ExperimentDefinitionModel"

    override def preProcess(data: RDD[Empty], config: Predef.Map[String, AnyRef])(implicit sc: SparkContext): RDD[ExperimentDefinition] = {

        val experiments = sc.cassandraTable[ExperimentDefinition](AppConf.getConfig("experiment.definition.cassandra.keyspace")
            , AppConf.getConfig("experiment.definition.cassandra.tableName"))
          .filter(metadata => metadata.status.equalsIgnoreCase("SUBMITTED"))

        experiments
    }

    override def algorithm(experiments: RDD[ExperimentDefinition], config: Predef.Map[String, AnyRef])(implicit sc: SparkContext): RDD[ExperimentMappingOutput] = {

        var metadata: Buffer[ExperiementDefinitionOutput] = Buffer()
        implicit val utils: ExperimentDataUtils = new ExperimentDataUtils
        val result = algorithmProcess(experiments, metadata)
        sc.makeRDD(metadata).saveToCassandra(AppConf.getConfig("experiment.definition.cassandra.keyspace"),
            AppConf.getConfig("experiment.definition.cassandra.tableName"), SomeColumns("expid", "status", "status_message", "updatedon", "updatedby", "stats"));
        result.fold(sc.emptyRDD[ExperimentMappingOutput])(_ ++ _)

    }


    override def postProcess(data: RDD[ExperimentMappingOutput], config: Predef.Map[String, AnyRef])(implicit sc: SparkContext): RDD[ExperimentMappingOutput] = {

        data.distinct()
    }

    def algorithmProcess(experiments: RDD[ExperimentDefinition], metadata: Buffer[ExperiementDefinitionOutput])(implicit sc: SparkContext, util: ExperimentDataUtils): Array[RDD[ExperimentMappingOutput]] = {
        val apiTokenURL = AppConf.getConfig("api.openidconnect.token.url")

        val access_token = util.getUserAPIToken()
        val experiment_list = experiments.collect()

        val device_profile = util.getDeviceProfile(AppConf.getConfig("device.profile.cassandra.keyspace"),
            AppConf.getConfig("device.profile.cassandra.tableName"))

        val result = experiment_list.map(exp => {

            val criteria = JSONUtils.deserialize[CriteriaModel](exp.criteria)
            val filter_type = JSONUtils.deserialize[CriteriaModel](exp.criteria).`type`

            try {
                (filter_type) match {
                    case ("user") | ("user_mod") =>

                        val userResponse = util.getUserDetails(access_token, JSONUtils.serialize(criteria.filters))
                        if (null != userResponse && userResponse.responseCode.equalsIgnoreCase("OK")) {
                            val userResult = userResponse.result.get("response").get
                            metadata ++= Seq(populateExperimentMetadata(exp, userResult.content.size, filter_type, "ACTIVE", "Experiment Mapped Sucessfully"))
                            sc.parallelize(userResult.content.map(z =>
                                populateExperimentMapping(Some(z.get("id").get.asInstanceOf[String]), exp, filter_type)))

                        }
                        else {
                            sc.emptyRDD[ExperimentMappingOutput]
                        }

                    case ("device") | ("device_mod") =>

                        val filters = criteria.filters.asInstanceOf[List[Map[String, AnyRef]]].
                          map(f => Filter(f.get("name").get.asInstanceOf[String], f.get("operator").get.asInstanceOf[String], f.get("value")))

                        val filteredProfile = DataFilter.filter(device_profile, filters.toArray)
                        val deviceRDD = filteredProfile.map(z => populateExperimentMapping(z.device_id, exp, filter_type))
                        metadata ++= Seq(populateExperimentMetadata(exp, deviceRDD.count(), filter_type, "ACTIVE", "Experiment Mapped Sucessfully"))
                        deviceRDD

                }
            } catch {
                case ex: Exception =>
                    JobLogger.log(ex.getMessage, None, ERROR);
                    metadata ++= Seq(populateExperimentMetadata(exp, 0, filter_type, "FAILED", "Experiment Failed : " + ex.getMessage))
                    ex.printStackTrace();
                    sc.emptyRDD[ExperimentMappingOutput]
            }


        })

        result
    }

    def populateExperimentMapping(mapping_id: Option[String], exp: ExperimentDefinition, expType: String): ExperimentMappingOutput = {
        val expData = JSONUtils.deserialize[ExperimentData](exp.expdata)
        val modulus = expData.modulus.getOrElse(0)
        if (expType.contains("user"))
            ExperimentMappingOutput(userId = mapping_id,
                id = exp.expid, name = exp.expname, platform = expData.client, key = expData.key, expType = expType,
                startDate = getDateHourFormat(expData.startDate), endDate = getDateHourFormat(expData.endDate),
                lastUpdatedOn = getDateHourFormat(TimestampFormatter.format(new Date)), userIdMod = modulus)
        else
            ExperimentMappingOutput(userId = mapping_id,
                id = exp.expid, name = exp.expname, platform = expData.client, key = expData.key, expType = expType,
                startDate = getDateHourFormat(expData.startDate), endDate = getDateHourFormat(expData.endDate),
                lastUpdatedOn = getDateHourFormat(TimestampFormatter.format(new Date)), deviceIdMod = modulus)
    }

    def populateExperimentMetadata(exp: ExperimentDefinition, mappedCount: Long, expType: String, status: String, status_msg: String): ExperiementDefinitionOutput = {

        val stats = Map(expType + "Matched" -> mappedCount)
        ExperiementDefinitionOutput(exp.expid, status, status_msg, TimestampFormatter.format(new Date), stats, "ExperimentDataproduct")

    }

    def getDateHourFormat(date: String): String = {
        val outputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        val inputFormat = new SimpleDateFormat("yyyy-MM-dd")

        outputFormat.format(inputFormat.parse(date))
    }

}

class ExperimentDataUtils {

    def getUserAPIToken(): String = {
        val apiTokenURL = AppConf.getConfig("api.openidconnect.token.url")
        val bodyMap = Predef.Map("client_id" -> "admin-cli", "password" -> AppConf.getConfig("api.openidconnect.token.password"),
            "grant_type" -> "password", "username" -> AppConf.getConfig("api.openidconnect.token.username"))
        val response = RestUtil.postURLEncoded[String](apiTokenURL, bodyMap)
        val accessToken = JSONUtils.deserialize[Map[String, String]](response).get("access_token").get
        accessToken
    }

    def getUserDetails[T](accessToken: String, request_filter: String)(implicit mf: Manifest[T]): UserResponse = {
        val userapiURL = AppConf.getConfig("user.search.api.url")
        var filter = request_filter
        val user_search_limit = AppConf.getConfig("user.search.limit")

        val request =
            s"""
               |{
               |"request":{
               |"filters" :
               |${request_filter}
               |,
               |"limit" : ${user_search_limit}
               |}
               |
               |}
            """.stripMargin
        val requestHeaders = Predef.Map(HttpHeaders.AUTHORIZATION -> AppConf.getConfig("user.search.api.key"), "x-authenticated-user-token" -> accessToken)
        val userResponse = RestUtil.post[UserResponse](userapiURL, request, Some(requestHeaders))

        userResponse
    }

    def getDeviceProfile(keySpace: String, table: String)(implicit sc: SparkContext): RDD[DeviceProfileModel] = {

        sc.cassandraTable[DeviceProfileModel](keySpace, table)
    }


}