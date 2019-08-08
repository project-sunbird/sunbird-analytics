package org.ekstep.analytics.model

import java.text.SimpleDateFormat
import java.util.Date

import com.datastax.spark.connector._
import com.datastax.spark.connector.types.TimestampFormatter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Level.ERROR
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{IBatchModelTemplate, _}
import org.ekstep.analytics.util.Constants

import scala.collection.mutable.{Buffer, ListBuffer}


case class ExperimentDefinitionOutput(userId: Option[String] = None, deviceId: Option[String] = None, key: String, url: Option[String] = None, id: String
                                   , name: String, platform: String, userIdMod: Int = 0, deviceIdMod: Int = 0, expType: String, startDate: String,
                                   endDate: String, lastUpdatedOn: String) extends AlgoOutput with Output

case class UserList(count: Int, content: List[Map[String, AnyRef]])

case class UserResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Map[String, UserList]);

case class CriteriaModel(`type`: String, filters: AnyRef, modulus: Option[Int])

case class ExperimentDefinition(exp_id: String, exp_name: String, criteria: String, exp_data: String, status: String) extends AlgoInput with Input

case class ExperimentData(startDate: String, endDate: String, key: String, client: String, modulus: Option[Int])


case class ExperimentDefinitionMetadata(exp_id: String, status: String, status_message: String, updated_on: String, stats: Map[String, Long], updated_by: String)


object ExperimentDefinitionModel extends IBatchModelTemplate[Empty, ExperimentDefinition, ExperimentDefinitionOutput, ExperimentDefinitionOutput] with Serializable {

    implicit val className = "org.ekstep.analytics.model.ExperimentDefinitionModel"

    override def name: String = "ExperimentDefinitionModel"

    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ExperimentDefinition] = {

        val experiments = sc.cassandraTable[ExperimentDefinition](Constants.PLATFORM_KEY_SPACE_NAME
            , Constants.EXPERIMENT_DEFINITION_TABLE)
          .filter(metadata => metadata.status.equalsIgnoreCase("SUBMITTED"))

        experiments
    }

    override def algorithm(experiments: RDD[ExperimentDefinition], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ExperimentDefinitionOutput] = {

        var metadata: ListBuffer[ExperimentDefinitionMetadata] = ListBuffer()
        implicit val utils: ExperimentDataUtils = new ExperimentDataUtils
        val result = algorithmProcess(experiments, metadata)
        sc.makeRDD(metadata).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME,
            Constants.EXPERIMENT_DEFINITION_TABLE, SomeColumns("exp_id", "status", "status_message", "updated_on", "updated_by", "stats"));
        result.fold(sc.emptyRDD[ExperimentDefinitionOutput])(_ ++ _)

    }


    override def postProcess(data: RDD[ExperimentDefinitionOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ExperimentDefinitionOutput] = {

        data
    }

    def algorithmProcess(experiments: RDD[ExperimentDefinition], metadata: Buffer[ExperimentDefinitionMetadata])(implicit sc: SparkContext, util: ExperimentDataUtils): Array[RDD[ExperimentDefinitionOutput]] = {
        val experiment_list = experiments.collect()
        val device_profile = util.getDeviceProfile(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_PROFILE_TABLE)

        val result = experiment_list.map(exp => {
            val criteria = JSONUtils.deserialize[CriteriaModel](exp.criteria)
            val filter_type = JSONUtils.deserialize[CriteriaModel](exp.criteria).`type`
            try {
                filter_type match {
                    case "user" | "user_mod" =>
                        val userResponse = util.getUserDetails(JSONUtils.serialize(criteria.filters))
                        if (null != userResponse && !userResponse.responseCode.isEmpty && userResponse.responseCode.equalsIgnoreCase("OK")) {
                            val userResult = userResponse.result.get("response").get
                            metadata ++= Seq(populateExperimentMetadata(exp, userResult.content.size, filter_type,
                                "ACTIVE", "Experiment Mapped Sucessfully"))
                            sc.parallelize(userResult.content.map(z =>
                                populateExperimentMapping(Some(z.get("id").get.asInstanceOf[String]), exp, filter_type)))
                        } else {
                            metadata ++= Seq(populateExperimentMetadata(exp, 0, filter_type, "FAILED",
                                "Experiment Failed, Please Check the criteria"))
                            sc.emptyRDD[ExperimentDefinitionOutput]
                        }

                    case "device" | "device_mod" =>
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
                    sc.emptyRDD[ExperimentDefinitionOutput]
            }
        })
        result
    }

    def populateExperimentMapping(mapping_id: Option[String], exp: ExperimentDefinition, expType: String): ExperimentDefinitionOutput = {
        val expData = JSONUtils.deserialize[ExperimentData](exp.exp_data)
        val modulus = expData.modulus.getOrElse(0)
        val outputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        val inputFormat = new SimpleDateFormat("yyyy-MM-dd")
        if (expType.contains("user"))
            ExperimentDefinitionOutput(userId = mapping_id,
                id = exp.exp_id, name = exp.exp_name, platform = expData.client, key = expData.key, expType = expType,
                startDate = outputFormat.format(inputFormat.parse(expData.startDate)),
                endDate = outputFormat.format(inputFormat.parse(expData.endDate)),
                lastUpdatedOn = outputFormat.format(inputFormat.parse(TimestampFormatter.format(new Date))),
                userIdMod = modulus)
        else
            ExperimentDefinitionOutput(userId = mapping_id,
                id = exp.exp_id, name = exp.exp_name, platform = expData.client, key = expData.key, expType = expType,
                startDate = outputFormat.format(inputFormat.parse(expData.startDate)),
                endDate = outputFormat.format(inputFormat.parse(expData.endDate)),
                lastUpdatedOn = outputFormat.format(inputFormat.parse(TimestampFormatter.format(new Date))),
                deviceIdMod = modulus)
    }

    def populateExperimentMetadata(exp: ExperimentDefinition, mappedCount: Long, expType: String, status: String, status_msg: String): ExperimentDefinitionMetadata = {
        val stats = Map(expType + "Matched" -> mappedCount)
        ExperimentDefinitionMetadata(exp.exp_id, status, status_msg, TimestampFormatter.format(new Date), stats, "ExperimentDataProduct")

    }
}

class ExperimentDataUtils {

    def getUserDetails[T](request_filter: String)(implicit mf: Manifest[T]): UserResponse = {
        val user_search_limit = AppConf.getConfig("user.search.limit")
        val request =
            s"""
               |{
               |  "request": {
               |    "filters" : ${request_filter} ,
               |    "limit" : ${user_search_limit}
               |    }
               |  }
               """.stripMargin

        val userResponse = RestUtil.post[UserResponse](Constants.USER_SEARCH_URL, request)
        userResponse
    }

    def getDeviceProfile(keySpace: String, table: String)(implicit sc: SparkContext): RDD[DeviceProfileModel] = {
        sc.cassandraTable[DeviceProfileModel](keySpace, table)
    }


}