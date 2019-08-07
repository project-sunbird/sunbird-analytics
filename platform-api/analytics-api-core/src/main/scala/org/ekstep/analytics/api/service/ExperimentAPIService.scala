package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.Config
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.util.{CommonUtil, DBUtil}
import org.ekstep.analytics.framework.ExperimentStatus
import org.ekstep.analytics.framework.util.JSONUtils
import org.joda.time.DateTime

 object ExperimentAPIService {

   implicit val className = "org.ekstep.analytics.api.service.ExperimentAPIService"

   case class CreateExperimentRequest(request: String, config: Config)

   case class GetExperimentRequest(requestId: String, config: Config)

   def createRequest(request: String)(implicit config: Config): Response = {
     val body = JSONUtils.deserialize[ExperimentRequestBody](request)
     val isValid = _validateExpReq(body)
     if ("true".equals(isValid.get("status").get)) {
       val job = upsertRequest(body)
       val response = CommonUtil.caseClassToMap(_createJobResponse(job))
       CommonUtil.OK(APIIds.EXPERIEMNT_CREATE_REQUEST, response)
     } else {
       CommonUtil.errorResponse(APIIds.EXPERIEMNT_CREATE_REQUEST, isValid.get("message").get, ResponseCode.CLIENT_ERROR.toString)
     }
   }

   private def upsertRequest(body: ExperimentRequestBody)(implicit config: Config): ExperimentCreateRequest = {
     val expReq = body.request
     val experiment = DBUtil.getExperiementRequest(expReq.expId.get)

     if (null == experiment) {
       _saveExpRequest(expReq)
     } else {
       if (StringUtils.equalsIgnoreCase(ExperimentStatus.FAILED.toString(), experiment.status.getOrElse(""))) {
         _saveExpRequest(expReq)
       } else experiment
     }
   }

   def getExperimentRequest(requestId: String)(implicit config: Config): Response = {
     val job = DBUtil.getExperiementRequest(requestId)
     if (null == job) {
       CommonUtil.errorResponse(APIIds.EXPERIEMNT_GET_REQUEST, "no experiemnt available with the given experimentid", ResponseCode.OK.toString)
     } else {
       val jobStatusRes = _createJobResponse(job)
       CommonUtil.OK(APIIds.EXPERIEMNT_GET_REQUEST, CommonUtil.caseClassToMap(jobStatusRes))
     }
   }

   private def _saveExpRequest(request: ExperimentRequest): ExperimentCreateRequest = {
     val status = ExperimentStatus.SUBMITTED.toString()
     val submittedDate = Option(DateTime.now())
     val status_msg = "Experiment sucessfully submitted by " + request.createdBy.get
     val expRequest = ExperimentCreateRequest(request.expId, request.name, request.description,
       request.createdBy, Some("Experiment_CREATE_API"),
       submittedDate, submittedDate, Some(JSONUtils.serialize(request.criteria)), Some(JSONUtils.serialize(request.data)), Some(status), Some(status_msg), None)

     DBUtil.saveExpRequest(Array(expRequest))
     expRequest
   }

   private def getDateInMillis(date: DateTime): Option[Long] = {
     if (null != date) Option(date.getMillis) else None
   }

   private def _createJobResponse(expRequest: ExperimentCreateRequest): ExperimentResponse = {
     val stats = expRequest.stats.getOrElse(null)
     val processed = List(ExperimentStatus.ACTIVE.toString(), ExperimentStatus.FAILED.toString).contains(expRequest.status.get)
     val statsOutput = if (processed && null != stats) {
       stats
     } else Map.empty[String,Long]

     val experimentRequest = ExperimentRequest(expRequest.expId, expRequest.expName, expRequest.createdBy, expRequest.expDescription,
       Option(JSONUtils.deserialize[Map[String, AnyRef]](expRequest.criteria.get)), Option(JSONUtils.deserialize[Map[String, String]](expRequest.data.get)))
     ExperimentResponse(experimentRequest, statsOutput, expRequest.udpatedOn.get.getMillis, expRequest.createdOn.get.getMillis,
       expRequest.status.get, expRequest.status_msg.get)
   }


   private def _validateExpReq(body: ExperimentRequestBody)(implicit config: Config): Map[String, String] = {

     val request = body.request
     if (null == request) {
       val message = "Request is empty"
       Map("status" -> "false", "message" -> message)
     } else {
       if (request.expId.isEmpty) {
         Map("status" -> "false", "message" -> "Experiemnt Id is empty")
       } else if (request.name.isEmpty) {
         Map("status" -> "false", "message" -> "Experiemnt Name is empty")
       } else if (request.createdBy.isEmpty) {
         Map("status" -> "false", "message" -> "Created By is empty")
       } else if (request.criteria.isEmpty) {
         Map("status" -> "false", "message" -> "Criteria are empty")
       } else if (request.criteria.get.get("filters").isEmpty) {
         Map("status" -> "false", "message" -> "Criteria Filter is Empty")
       }
       else if (request.criteria.get.get("type").isEmpty) {
         Map("status" -> "false", "message" -> "Criteria Type is empty")
       }
       else if (request.data.isEmpty) {
         Map("status" -> "false", "message" -> "Experiment Data is empty")
       }
       else {
         val endDate = request.data.get.get("endDate").get
         val startDate = request.data.get.get("startDate").get
         val days = CommonUtil.getDaysBetween(startDate, endDate)
         if (CommonUtil.getPeriod(endDate) < CommonUtil.getPeriod(CommonUtil.getToday))
           Map("status" -> "false", "message" -> "end_date should be greater than today's date..")
         else if (CommonUtil.getPeriod(startDate) < CommonUtil.getPeriod(CommonUtil.getToday))
           Map("status" -> "false", "message" -> "start_date should be greater than today's date..")
         else if (0 > days)
           Map("status" -> "false", "message" -> "Date range should not be -ve. Please check your start_date & end_date")
         else Map("status" -> "true")

       }

     }
   }
 }


class ExperimentAPIService extends Actor {

  import ExperimentAPIService._

  def receive = {
    case CreateExperimentRequest(request: String, config: Config) => sender() ! createRequest(request)(config)
    case GetExperimentRequest(requestId: String, config: Config) => sender() ! getExperimentRequest(requestId)(config)

  }
}

