package org.ekstep.ilimi.analytics.framework.filter

/**
 * @author Santhosh
 */
object InMatcher extends IMatcher {

    def matchValue(value1: AnyRef, value2: Option[AnyRef]): Boolean = {
        if (value2.isEmpty || !(value2.get.isInstanceOf[Array[AnyRef]])) {
            false;
        } else {
            val bool = value2.get.asInstanceOf[Array[AnyRef]].contains(value1);
            bool
        }
    }
}