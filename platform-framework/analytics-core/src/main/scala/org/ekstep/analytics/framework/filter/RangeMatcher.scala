package org.ekstep.analytics.framework.filter

/**
 * @author Santhosh
 */
object RangeMatcher extends IMatcher {

    def matchValue(value1: AnyRef, value2: Option[AnyRef]): Boolean = {
        if (value2.isEmpty || !(value2.get.isInstanceOf[Map[String, AnyRef]])) {
            return false;
        }
        val range = value2.get.asInstanceOf[Map[String, AnyRef]];
        val start = range.getOrElse("start", null);
        val end = range.getOrElse("end", null);
        if (start == null || end == null) {
            return false;
        }
        if (value1.isInstanceOf[Long]) {
            if (value1.asInstanceOf[Long] >= start.asInstanceOf[Long] && value1.asInstanceOf[Long] <= end.asInstanceOf[Long]) {
                return true;
            }
        }

        if (value1.isInstanceOf[Double]) {
            if (value1.asInstanceOf[Double] >= start.asInstanceOf[Double] && value1.asInstanceOf[Double] <= end.asInstanceOf[Double]) {
                return true;
            }
        }

        if (value1.isInstanceOf[Int]) {
            if (value1.asInstanceOf[Int] >= start.asInstanceOf[Int] && value1.asInstanceOf[Int] <= end.asInstanceOf[Int]) {
                return true;
            }
        }

        return false;
    }
}