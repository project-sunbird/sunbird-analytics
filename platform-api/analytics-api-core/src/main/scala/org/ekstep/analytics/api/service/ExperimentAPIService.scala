package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.Config
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.service.ExperimentAPIService.saveExpRequest
import org.ekstep.analytics.api.util.{CommonUtil, DBUtil}
import org.ekstep.analytics.framework.ExperimentStatus
import org.ekstep.analytics.framework.util.JSONUtils
import org.joda.time.DateTime

 object ExperimentAPIService {

   implicit val className = "org.ekstep.analytics.api.service.ExperimentAPIService"

   case class CreateExperimentRequest(request: String, config: Config)

   case class GetExperimentRequest(requestId: String, config: Config)

   def createRequest(request: String)(implicit config: Config): ExperimentBodyResponse = {
     val body = JSONUtils.deserialize[ExperimentRequestBody](request)
     val isValid = validateExpReq(body)
     if ("success".equals(isValid.get("status").get)) {
       val  response = upsertRequest(body)
       CommonUtil.experiemntOkResponse(APIIds.EXPERIEMNT_CREATE_REQUEST, response)
     } else {
       CommonUtil.experimentErrorResponse(APIIds.EXPERIEMNT_CREATE_REQUEST, isValid, ResponseCode.CLIENT_ERROR.toString)
     }
   }

   private def upsertRequest(body: ExperimentRequestBody)(implicit config: Config): Map[String,AnyRef] = {
     val expReq = body.request
     val experiment = DBUtil.getExperiementRequest(expReq.expId.get)
     val result = experiment.map{exp=> {
       if(ExperimentStatus.FAILED.toString.equalsIgnoreCase(exp.status.get)) {
         val experimentRequest = saveExpRequest(expReq)
         CommonUtil.caseClassToMap(createExperimentResponse(experimentRequest))
       }else {
         CommonUtil.caseClassToMap(ExperimentErrorResponse(createExperimentResponse(exp),"failed",Map("error" -> "Experiment already Submitted")))
       }
     }}.getOrElse({
       val experimentRequest = saveExpRequest(expReq)
       CommonUtil.caseClassToMap(createExperimentResponse(experimentRequest))
     })
     result
   }

   def getExperimentRequest(requestId: String)(implicit config: Config): Response = {
     val experiment = DBUtil.getExperiementRequest(requestId)

     val expStatus = experiment.map{
         exp => {
           createExperimentResponse(exp)
           CommonUtil.OK(APIIds.EXPERIEMNT_GET_REQUEST, CommonUtil.caseClassToMap(createExperimentResponse(exp)))
         }
       }.getOrElse(CommonUtil.errorResponse(APIIds.EXPERIEMNT_GET_REQUEST, "no experiemnt available with the given experimentid", ResponseCode.OK.toString))

     expStatus
   }

   private def saveExpRequest(request: ExperimentCreateRequest): ExperimentRequest = {
     val status = ExperimentStatus.SUBMITTED.toString()
     val submittedDate = Option(DateTime.now())
     val status_msg = "Experiment successfully submitted"
     val expRequest = ExperimentRequest(request.expId.get, request.name.get, request.description.get,
       request.createdBy.get, "Experiment_CREATE_API",
       submittedDate, submittedDate,JSONUtils.serialize(request.criteria), JSONUtils.serialize(request.data), Some(status), Some(status_msg), None)

     DBUtil.saveExpRequest(Array(expRequest))
     expRequest
   }

   private def createExperimentResponse(expRequest: ExperimentRequest): ExperimentResponse = {
     val stats = expRequest.stats.getOrElse(null)
     val processed = List(ExperimentStatus.ACTIVE.toString(), ExperimentStatus.FAILED.toString).contains(expRequest.status.get)
     val statsOutput = if (processed && null != stats) {
       stats
     } else Map.empty[String,Long]

     val experimentRequest = ExperimentCreateRequest(Some(expRequest.expId), Some(expRequest.expName), Some(expRequest.createdBy), Some(expRequest.expDescription),
       Option(JSONUtils.deserialize[Map[String, AnyRef]](expRequest.criteria)), Option(JSONUtils.deserialize[Map[String, String]](expRequest.data)))
     ExperimentResponse(experimentRequest, statsOutput, expRequest.udpatedOn.get.getMillis, expRequest.createdOn.get.getMillis,
       expRequest.status.get, expRequest.status_msg.get)
   }


   private def validateExpReq(body: ExperimentRequestBody)(implicit config: Config): Map[String, String] = {
     val request = body.request
     val errMap = scala.collection.mutable.Map[String, String]()
     if (null == request) {
       errMap("request") = "Request should not be empty"
     } else {
       if (request.expId.isEmpty) {
         errMap("request.expid") = "Experiment Id should not be  empty"
       }
       if (request.name.isEmpty) {
         errMap("request.name") = "Experiment Name should not be  empty"
       }
       if (request.createdBy.isEmpty)
         errMap("request.createdBy") = "Created By should not be empty"
       if (request.criteria.isEmpty) {
         errMap("request.createdBy") = "Criteria should not be empty"
       }
       else if (request.criteria.get.get("filters").isEmpty) {
         errMap("request.filters") = "Criteria Filters should not be empty"
       }
       else if (request.criteria.get.get("type").isEmpty) {
         errMap("request.type") = "Criteria Type should not be empty"
       }
       if (request.data.isEmpty) {
         errMap("request.data") = "Experiment Data should not be empty"
       }
       else {
         val endDate = request.data.get.getOrElse("endDate","")
         val startDate = request.data.get.getOrElse("startDate","")
         if (endDate.isEmpty) {
           errMap("data.endDate") = "Experiment End_Date should not be empty"
         }
         else if (CommonUtil.getPeriod(endDate) < CommonUtil.getPeriod(CommonUtil.getToday))
           errMap("data.startDate") = "End_Date should be greater than today's date."

         if (startDate.isEmpty) {
           errMap("data.endDate") = "Experiment Start_Date should not be empty"
         }
         else if (CommonUtil.getPeriod(startDate) < CommonUtil.getPeriod(CommonUtil.getToday))
           errMap("data.endDate") = "Start_Date should be greater than or equal to today's date.."
         val days = CommonUtil.getDaysBetween(startDate, endDate)
         if (!startDate.isEmpty && !endDate.isEmpty && 0 > days)
         errMap("data.StartDate") = "Date range should not be -ve. Please check your start_date & end_date"
       }
     }
     if(errMap.size >0) errMap += ("status" -> "failed")  else errMap += ("status" -> "success")
     errMap.toMap
   }
 }


class ExperimentAPIService extends Actor {

  import ExperimentAPIService._

  def receive = {
    case CreateExperimentRequest(request: String, config: Config) => sender() ! createRequest(request)(config)
    case GetExperimentRequest(requestId: String, config: Config) => sender() ! getExperimentRequest(requestId)(config)

  }
}

