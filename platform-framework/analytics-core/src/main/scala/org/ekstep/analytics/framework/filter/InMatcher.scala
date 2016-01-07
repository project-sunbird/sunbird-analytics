package org.ekstep.analytics.framework.filter

/**
 * @author Santhosh
 */
object InMatcher extends IMatcher {

    def matchValue(value1: AnyRef, value2: Option[AnyRef]): Boolean = {
        if (value2.isEmpty || !(value2.get.isInstanceOf[List[AnyRef]])) {
            false;
        } else {
            val bool = value2.get.asInstanceOf[List[AnyRef]].contains(value1);
            bool
        }
    }
}