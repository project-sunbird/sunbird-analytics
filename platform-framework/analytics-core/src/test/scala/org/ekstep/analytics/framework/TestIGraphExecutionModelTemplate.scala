package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class TestIGraphExecutionModelTemplate extends SparkGraphSpec {

  "IGraphExecutionModelTemplate" should "execute the query" in {

        val query = "MATCH (n:domain) RETURN n LIMIT 1";
        val rdd = SampleGraphModelTemplate.execute(sc.parallelize(Seq(query)), None);
        rdd.count should be(1);
    }
}

object SampleGraphModelTemplate extends IGraphExecutionModelTemplate {

  override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
    
    input;
  }

  override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {

    ppQueries;
  }
}