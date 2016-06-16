package org.ekstep.analytics.framework.filter

import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.exception.DataFilterException
import org.ekstep.analytics.framework.util.JobLogger

/**
 * @author Santhosh
 */
object Matcher {

    val className = "org.ekstep.analytics.framework.filter.Matcher"
    @throws(classOf[DataFilterException])
    def getMatcher(op: String): IMatcher = {
        op match {
            case "NE"         => NotEqualsMatcher;
            case "IN"         => InMatcher;
            case "NIN"        => NotInMatcher;
            case "ISNULL"     => NullMatcher;
            case "ISEMPTY"    => EmptyMatcher;
            case "ISNOTNULL"  => NotNullMatcher;
            case "ISNOTEMPTY" => NotEmptyMatcher;
            case "EQ"         => EqualsMatcher;
            case _ =>
                val msg = "Unknown filter operation found"
                val exp = new DataFilterException(msg);
                JobLogger.error(msg, className, exp)
                throw exp;
        }
    }

    @throws(classOf[DataFilterException])
    def compare(value: AnyRef, filter: Filter): Boolean = {
        getMatcher(filter.operator).matchValue(value, filter.value);
    }
}