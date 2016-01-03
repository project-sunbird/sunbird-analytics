package org.ekstep.analytics.framework.filter

import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.exception.DataFilterException

/**
 * @author Santhosh
 */
object Matcher {

    def getMatcher(op: String): IMatcher = {
        op match {
            case "NE"         => NotEqualsMatcher;
            case "IN"         => InMatcher;
            case "ISNULL"     => NullMatcher;
            case "ISEMPTY"    => EmptyMatcher;
            case "ISNOTNULL"  => NotNullMatcher;
            case "ISNOTEMPTY" => NotEmptyMatcher;
            case "EQ"         => EqualsMatcher;
            case _ =>
                throw new DataFilterException("Unknown filter operation found");
        }
    }

    def compare(value: AnyRef, filter: Filter): Boolean = {
        getMatcher(filter.operator).matchValue(value, filter.value);
    }
}