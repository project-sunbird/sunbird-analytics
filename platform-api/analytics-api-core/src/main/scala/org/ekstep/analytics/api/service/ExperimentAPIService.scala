package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.Config
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.util.{CommonUtil, DBUtil}
import org.ekstep.analytics.framework.ExperimentStatus
import org.ekstep.analytics.framework.util.JSONUtils
import org.joda.time.DateTime

object ExperimentAPIService {

   implicit val className: String = "org.ekstep.analytics.api.service.ExperimentAPIService"

   case class CreateExperimentRequest(request: String, config: Config)

   case class GetExperimentRequest(requestId: String, config: Config)

   def createRequest(request: String)(implicit config: Config): ExperimentBodyResponse = {
     val body = JSONUtils.deserialize[ExperimentRequestBody](request)
     val isValid = validateExpReq(body)
     if ("success".equals(isValid.getOrElse("status", ""))) {
       val response = upsertRequest(body)
       CommonUtil.experimentOkResponse(APIIds.EXPERIEMNT_CREATE_REQUEST, response)
     } else {
       CommonUtil.experimentErrorResponse(APIIds.EXPERIEMNT_CREATE_REQUEST, isValid, ResponseCode.CLIENT_ERROR.toString)
     }
   }

   private def upsertRequest(body: ExperimentRequestBody)(implicit config: Config): Map[String, AnyRef] = {
     val expReq = body.request
     val experiment = DBUtil.getExperiementDefinition(expReq.expId)
     val result = experiment.map { exp => {
       if (ExperimentStatus.FAILED.toString.equalsIgnoreCase(exp.status.get)) {
         val experimentRequest = saveExperimentDefinition(expReq)
         CommonUtil.caseClassToMap(createExperimentResponse(experimentRequest))
       } else {
         CommonUtil.caseClassToMap(ExperimentErrorResponse(createExperimentResponse(exp), "failed", Map("msg" -> "ExperimentId already exists.")))
       }
     }
     }.getOrElse({
       val experimentRequest = saveExperimentDefinition(expReq)
       CommonUtil.caseClassToMap(createExperimentResponse(experimentRequest))
     })
     result
   }

   def getExperimentDefinition(requestId: String)(implicit config: Config): Response = {
     val experiment = DBUtil.getExperiementDefinition(requestId)

     val expStatus = experiment.map {
       exp => {
         createExperimentResponse(exp)
         CommonUtil.OK(APIIds.EXPERIEMNT_GET_REQUEST, CommonUtil.caseClassToMap(createExperimentResponse(exp)))
       }
     }.getOrElse(CommonUtil.errorResponse(APIIds.EXPERIEMNT_GET_REQUEST,
       "no experiment available with the given experimentid", ResponseCode.OK.toString))

     expStatus
   }

   private def saveExperimentDefinition(request: ExperimentCreateRequest): ExperimentDefinition = {
     val status = ExperimentStatus.SUBMITTED.toString
     val submittedDate = Option(DateTime.now())
     val statusMsg = "Experiment successfully submitted"
     val expRequest = ExperimentDefinition(request.expId, request.name, request.description,
       request.createdBy, "Experiment_CREATE_API", submittedDate, submittedDate, JSONUtils.serialize(request.criteria),
       JSONUtils.serialize(request.data), Some(status), Some(statusMsg), None)

     DBUtil.saveExperimentDefinition(Array(expRequest))
     expRequest
   }

   private def createExperimentResponse(expRequest: ExperimentDefinition): ExperimentResponse = {
     val stats = expRequest.stats.orNull
     val processed = List(ExperimentStatus.ACTIVE.toString, ExperimentStatus.FAILED.toString).contains(expRequest.status.get)
     val statsOutput = if (processed && null != stats) {
       stats
     } else Map[String, Long]()

     val experimentRequest = ExperimentCreateRequest(expRequest.expId, expRequest.expName, expRequest.createdBy, expRequest.expDescription,
       JSONUtils.deserialize[Map[String, AnyRef]](expRequest.criteria), JSONUtils.deserialize[Map[String, AnyRef]](expRequest.data))
     ExperimentResponse(experimentRequest, statsOutput, expRequest.udpatedOn.get.getMillis, expRequest.createdOn.get.getMillis,
       expRequest.status.get, expRequest.status_msg.get)
   }


   private def validateExpReq(body: ExperimentRequestBody)(implicit config: Config): Map[String, String] = {
     val request = body.request
     val errMap = scala.collection.mutable.Map[String, String]()
     if (null == request) {
       errMap("request") = "Request should not be empty"
     } else {
       if (Option(request.expId).isEmpty) {
         errMap("request.expid") = "Experiment Id should not be  empty"
       }
       if (Option(request.name).isEmpty) {
         errMap("request.name") = "Experiment Name should not be  empty"
       }
       if (Option(request.createdBy).isEmpty) {
         errMap("request.createdBy") = "Created By should not be empty"
       }

       if (Option(request.criteria).isEmpty) {
         errMap("request.createdBy") = "Criteria should not be empty"
       } else if (request.criteria.get("filters").isEmpty) {
         errMap("request.filters") = "Criteria Filters should not be empty"
       } else if (request.criteria.get("type").isEmpty) {
         errMap("request.type") = "Criteria Type should not be empty"
       }

       if (Option(request.data).isEmpty) {
         errMap("request.data") = "Experiment Data should not be empty"
       } else {
         val endDate = request.data.getOrElse("endDate", "").asInstanceOf[String]
         val startDate = request.data.getOrElse("startDate", "").asInstanceOf[String]
         if (endDate.isEmpty) {
           errMap("data.endDate") = "Experiment End_Date should not be empty"
         }
         else if (CommonUtil.getPeriod(endDate) < CommonUtil.getPeriod(CommonUtil.getToday()))
           errMap("data.startDate") = "End_Date should be greater than today's date."

         if (startDate.isEmpty) {
           errMap("data.endDate") = "Experiment Start_Date should not be empty"
         }
         else if (CommonUtil.getPeriod(startDate) < CommonUtil.getPeriod(CommonUtil.getToday()))
           errMap("data.endDate") = "Start_Date should be greater than or equal to today's date.."
         val days = CommonUtil.getDaysBetween(startDate, endDate)
         if (!startDate.isEmpty && !endDate.isEmpty && 0 > days)
           errMap("data.StartDate") = "Date range should not be -ve. Please check your start_date & end_date"
       }
     }
     if (errMap.nonEmpty) errMap += ("status" -> "failed") else errMap += ("status" -> "success")
     errMap.toMap
   }
 }


class ExperimentAPIService extends Actor {

  import ExperimentAPIService._

  def receive = {
    case CreateExperimentRequest(request: String, config: Config) => sender() ! createRequest(request)(config)
    case GetExperimentRequest(requestId: String, config: Config) => sender() ! getExperimentDefinition(requestId)(config)

  }
}

