package org.ekstep.ilimi.analytics.framework.filter

/**
 * @author Santhosh
 */
object NotEmptyMatcher extends IMatcher {
  
    def matchValue(value1: AnyRef, value2: Option[AnyRef]) : Boolean = {
        null != value1 && value1.toString().nonEmpty;
    }
}