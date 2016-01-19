package org.ekstep.analytics.updater

object UpdateProficiencyModelParam {
  def getParameterAlpha(learner_id: String, mc: String): Option[Double] = {
        return Option(0.5d);
    }
    def getParameterBeta(learner_id: String, mc: String): Option[Double] = {
        return Option(1d)
    }
    def saveModelParamToDB(learner_id: String, mc: String, alpha : Option[Double],beta:Option[Double]){
        
    }
}