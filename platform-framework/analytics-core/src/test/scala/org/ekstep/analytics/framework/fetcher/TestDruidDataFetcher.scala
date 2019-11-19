package org.ekstep.analytics.framework.fetcher

import ing.wbaa.druid.definitions.{AggregationType, PostAggregationType}
import org.ekstep.analytics.framework.{PostAggregationFields, SparkSpec}

class TestDruidDataFetcher extends SparkSpec {

    it should "check for getAggregationTypes methods" in {

        val uniqueExpr = DruidDataFetcher.getAggregationByType(AggregationType.HyperUnique, Option("Unique"), "field", None, None, None)
        val uniqueExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.HyperUnique, None, "field", None, None, None)
        uniqueExpr.toString should be ("HyperUniqueAgg(field,Some(Unique),false,false)")

        val thetaSketchExpr = DruidDataFetcher.getAggregationByType(AggregationType.ThetaSketch, Option("Unique"), "field", None, None, None)
        val thetaSketchExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.ThetaSketch, None, "field", None, None, None)
        thetaSketchExpr.toString should be ("ThetaSketchAgg(field,Some(Unique),false,16384)")

        val cardinalityExpr = DruidDataFetcher.getAggregationByType(AggregationType.Cardinality, Option("Unique"), "field", None, None, None)
        val cardinalityExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.Cardinality, None, "field", None, None, None)
        cardinalityExpr.toString should be ("CardinalityAgg(WrappedArray(Dim(field,None,None,None)),Some(Unique),false,false)")

        val longSumExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongSum, Option("Total"), "field", None, None, None)
        val longSumExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongSum, None, "field", None, None, None)

        val doubleSumExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleSum, Option("Total"), "field", None, None, None)
        val doubleSumExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleSum, None, "field", None, None, None)

        val doubleMaxExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleMax, Option("Max"), "field", None, None, None)
        val doubleMaxExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleMax, None, "field", None, None, None)

        val doubleMinExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleMin, Option("Min"), "field", None, None, None)
        val doubleMinExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleMin, None, "field", None, None, None)

        val longMaxExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongMax, Option("Max"), "field", None, None, None)
        val longMaxExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongMax, None, "field", None, None, None)

        val longMinExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongMin, Option("Min"), "field", None, None, None)
        val longMinExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongMin, None, "field", None, None, None)

        val doubleFirstExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleFirst, Option("First"), "field", None, None, None)
        val doubleFirstExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleFirst, None, "field", None, None, None)

        val doubleLastExpr = DruidDataFetcher.getAggregationByType(AggregationType.DoubleLast, Option("Last"), "field", None, None, None)
        val doubleLastExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.DoubleLast, None, "field", None, None, None)

        val longFirstExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongFirst, Option("First"), "field", None, None, None)
        val longFirstExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongFirst, None, "field", None, None, None)

        val longLastExpr = DruidDataFetcher.getAggregationByType(AggregationType.LongLast, Option("Last"), "field", None, None, None)
        val longLastExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.LongLast, None, "field", None, None, None)

        val javascriptExpr = DruidDataFetcher.getAggregationByType(AggregationType.Javascript, Option("OutPut"), "field",
            Option("function(current, edata_size) { return current + (edata_size == 0 ? 1 : 0); }"),
            Option("function(partialA, partialB) { return partialA + partialB; }"), Option("function () { return 0; }"))
        javascriptExpr.toString should be ("JavascriptAgg(List(field),function(current, edata_size) { return current + (edata_size == 0 ? 1 : 0); },function(partialA, partialB) { return partialA + partialB; },function () { return 0; },Some(OutPut))")

        val javascriptExprWithoutName = DruidDataFetcher.getAggregationByType(AggregationType.Javascript, None, "field",
            Option("function(current, edata_size) { return current + (edata_size == 0 ? 1 : 0); }"),
            Option("function(partialA, partialB) { return partialA + partialB; }"), Option("function () { return 0; }"))
    }

    it should "check for getFilterTypes methods" in {

        val isNullExpr = DruidDataFetcher.getFilterByType("isnull", "field", List())
        isNullExpr.asFilter.`type`.toString should be ("Selector")

        val isNotNullExpr = DruidDataFetcher.getFilterByType("isnotnull", "field", List())

        val equalsExpr = DruidDataFetcher.getFilterByType("equals", "field", List("abc"))

        val notEqualsExpr = DruidDataFetcher.getFilterByType("notequals", "field", List("xyz"))

        val containsIgnorecaseExpr = DruidDataFetcher.getFilterByType("containsignorecase", "field", List("abc"))

        val containsExpr = DruidDataFetcher.getFilterByType("contains", "field", List("abc"))

        val inExpr = DruidDataFetcher.getFilterByType("in", "field", List("abc", "xyz"))

        val notInExpr = DruidDataFetcher.getFilterByType("notin", "field", List("abc", "xyz"))

        val regexExpr = DruidDataFetcher.getFilterByType("regex", "field", List("%abc%"))

        val likeExpr = DruidDataFetcher.getFilterByType("like", "field", List("%abc%"))

        val greaterThanExpr = DruidDataFetcher.getFilterByType("greaterthan", "field", List(0.asInstanceOf[AnyRef]))

        val lessThanExpr = DruidDataFetcher.getFilterByType("lessthan", "field", List(1000.asInstanceOf[AnyRef]))
    }

    it should "check for getPostAggregation methods" in {

        val additionExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Addition", PostAggregationFields("field", ""), "+")
        additionExpr.getName.toString should be ("Addition")

        val subtractionExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Subtraction", PostAggregationFields("field", ""), "-")
        subtractionExpr.getName.toString should be ("Subtraction")

        val multiplicationExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Product", PostAggregationFields("field", ""), "*")
        multiplicationExpr.getName.toString should be ("Product")

        val divisionExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Arithmetic, "Division", PostAggregationFields("field", ""), "/")
        divisionExpr.getName.toString should be ("Division")

        val javaScriptExpr = DruidDataFetcher.getPostAggregationByType(PostAggregationType.Javascript, "Percentage", PostAggregationFields("fieldA", "fieldB"), "function(a, b) { return (a / b) * 100; }")
    }
}
