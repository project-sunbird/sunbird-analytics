package org.ekstep.analytics.api

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.JSONUtils
import org.scalatest.BeforeAndAfterAll

import com.typesafe.config.ConfigFactory



/**
 * @author Santhosh
 */
class SparkSpec extends BaseSpec with BeforeAndAfterAll {

  implicit var sc: SparkContext = null;
  implicit val config = ConfigFactory.load();		
  
  override def beforeAll() {
    super.beforeAll();
    sc = CommonUtil.getSparkContext(1, "TestAnalyticsCore");
  }

  override def afterAll() {
    CommonUtil.closeSparkContext();
  }

  def loadFile[T](file: String)(implicit mf: Manifest[T]): RDD[T] = {
    if (file == null) {
      return null;
    }
    sc.textFile(file, 1).map { line => JSONUtils.deserialize[T](line) }.filter { x => x != null }.cache();
  }

}